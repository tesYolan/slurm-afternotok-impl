#!/bin/bash
#SBATCH --job-name=escalate-handler
#SBATCH --output=/data/tracker/handler-%j.log
#SBATCH --time=00:10:00
#SBATCH --mem=100M

# Escalation Handler Script
# ===================================
# This script runs as a Slurm job with --dependency=afternotok:<parent_job>
# It detects OOM and TIMEOUT failures and escalates resources accordingly.
#
# OOM failures     -> escalate memory (independent)
# TIMEOUT failures -> escalate time (independent)
#
# Environment variables received:
#   PARENT_JOB        - Job ID to check for failures
#   CURRENT_LEVEL     - Current memory level (0-based index)
#   CURRENT_TIME_LEVEL- Current time level (0-based index)
#   CURRENT_TIME      - Current time limit
#   CHAIN_ID          - Unique chain identifier for tracking
#   SCRIPT            - Original script to run
#   SCRIPT_ARGS       - Arguments for the script (space-separated)
#   MAX_LEVEL         - Maximum memory escalation level
#   TIME_MAX_LEVEL    - Maximum time escalation level
#   MEMORY_LADDER     - Space-separated list of memory levels (e.g., "1G 2G 4G 8G 16G")
#   TIME_LADDER       - Space-separated list of time levels (e.g., "00:05:00 00:15:00 ...")
#   ARRAY_THROTTLE    - Optional: max concurrent array tasks
#   CHECKPOINT_DIR    - Directory for checkpoint files
#   HANDLER_SCRIPT    - Path to this handler script
#   PYLIB             - Path to Python library (optional, will auto-detect)
#   LOGGING_ENABLED   - Whether to log to external DB
#   LOGGING_DB_PATH   - Path to SQLite database

set -o pipefail

# Find Python library
if [[ -z "$PYLIB" ]]; then
    SCRIPT_DIR="$(dirname "$(readlink -f "$0")")"
    PYLIB="$SCRIPT_DIR/mem-escalate-lib.py"
fi

# Disable Python bytecode caching (scripts are mounted from host)
export PYTHONDONTWRITEBYTECODE=1

echo "========================================"
echo "Escalation Handler"
echo "========================================"
echo "Handler Job ID:  $SLURM_JOB_ID"
echo "Parent Job:      $PARENT_JOB"
echo "Memory Level:    $CURRENT_LEVEL (max: $MAX_LEVEL)"
echo "Time Level:      ${CURRENT_TIME_LEVEL:-0} (max: ${TIME_MAX_LEVEL:-4})"
echo "Current Time:    ${CURRENT_TIME:-00:05:00}"
echo "Chain ID:        $CHAIN_ID"
echo "Script:          $SCRIPT $SCRIPT_ARGS"
echo "Memory Ladder:   $MEMORY_LADDER"
echo "Time Ladder:     $TIME_LADDER"
echo "Timestamp:       $(date -Iseconds)"
echo ""

# Convert memory ladder string to array
IFS=' ' read -ra LADDER <<< "$MEMORY_LADDER"

# Convert time ladder string to array
IFS=' ' read -ra TIME_LADDER_ARR <<< "$TIME_LADDER"

# Set defaults if not provided
CURRENT_TIME_LEVEL=${CURRENT_TIME_LEVEL:-0}
TIME_MAX_LEVEL=${TIME_MAX_LEVEL:-4}
CURRENT_TIME=${CURRENT_TIME:-${TIME_LADDER_ARR[0]:-00:05:00}}

# Get memory for a given level
get_memory_for_level() {
    local level=$1
    echo "${LADDER[$level]}"
}

# Get time for a given level
get_time_for_level() {
    local level=$1
    echo "${TIME_LADDER_ARR[$level]}"
}

# Get task indices by state from sacct
# Usage: get_indices_by_state <job_id> <include_pattern> [exclude_pattern]
get_indices_by_state() {
    local job_id="$1"
    local include="$2"
    local exclude="$3"

    sacct -nX -j "$job_id" -o JobID,State --parsable2 2>/dev/null | \
        grep -E "$include" | \
        { [[ -n "$exclude" ]] && grep -vE "$exclude" || cat; } | \
        awk -F'[_|]' '{print $2}' | \
        sort -un | paste -sd, -
}

# Count items in comma-separated list
count_indices() {
    local indices="$1"
    [[ -z "$indices" ]] && echo "0" && return
    echo "$indices" | tr ',' '\n' | wc -l
}

# Compress comma-separated indices into range notation with stride detection
# Input:  "0,1,2,3,4,100,101,102,200"
# Output: "0-4,100-102,200"
# Also detects stride patterns like "8,18,28,38" -> "8-38:10"
# And interleaved patterns like "5,6,15,16,25,26" -> "5-25:10,6-26:10"
#
# Slurm array syntax: start-end:stride
# Examples:
#   0-10      -> tasks 0,1,2,3,4,5,6,7,8,9,10
#   0-10:2    -> tasks 0,2,4,6,8,10
#   8-498:10  -> tasks 8,18,28,...,498
compress_indices_to_ranges() {
    local indices="$1"
    [[ -z "$indices" ]] && return

    # Convert to sorted array
    local -a nums
    IFS=',' read -ra nums <<< "$indices"

    # Sort numerically
    IFS=$'\n' nums=($(sort -n <<< "${nums[*]}")); unset IFS

    local count=${#nums[@]}

    # Handle trivial cases
    if (( count == 0 )); then
        return
    elif (( count == 1 )); then
        echo "${nums[0]}"
        return
    elif (( count == 2 )); then
        if (( nums[1] == nums[0] + 1 )); then
            echo "${nums[0]}-${nums[1]}"
        else
            echo "${nums[0]},${nums[1]}"
        fi
        return
    fi

    # FIX DELETE this till the end of the file. this came from making the errors 18, 19; and then each subsequent error was 10 apart. this created more items in the array than necessary.
    # Try to detect interleaved stride patterns first
    # Look for patterns like 5,6,15,16,25,26 which are two strides: 5,15,25 and 6,16,26
    # We detect this by looking at gaps between consecutive elements
    local -a gaps
    for ((i=1; i<count; i++)); do
        gaps+=($((nums[i] - nums[i-1])))
    done

    # Check for repeating gap pattern (e.g., [1,9,1,9,1,9] for 5,6,15,16,25,26)
    # First check if all gaps are the same (simple stride) - skip interleaved detection
    local all_same=1
    local first_gap="${gaps[0]}"
    for ((i=1; i<${#gaps[@]}; i++)); do
        if (( gaps[i] != first_gap )); then
            all_same=0
            break
        fi
    done

    # Find the period of the gap pattern (only if gaps vary)
    local detected_period=0
    if (( all_same == 0 )); then
        for period in 2 3 4 5; do
            if (( ${#gaps[@]} >= period * 3 )); then  # Need at least 3 repetitions
                local is_periodic=1
                for ((i=period; i<${#gaps[@]}; i++)); do
                    if (( gaps[i] != gaps[i % period] )); then
                        is_periodic=0
                        break
                    fi
                done
                if (( is_periodic )); then
                    detected_period=$period
                    break
                fi
            fi
        done
    fi

    if (( detected_period > 1 )); then
        # We have an interleaved pattern - extract each stride separately
        # Calculate the total stride (sum of one period of gaps)
        local total_stride=0
        for ((i=0; i<detected_period; i++)); do
            total_stride=$((total_stride + gaps[i]))
        done

        # Extract each interleaved sequence
        local result=""
        for ((offset=0; offset<detected_period; offset++)); do
            local seq_start="${nums[$offset]}"
            local seq_end="$seq_start"
            local seq_count=1

            # Follow this stride
            for ((j=offset+detected_period; j<count; j+=detected_period)); do
                seq_end="${nums[$j]}"
                ((seq_count++))
            done

            if (( seq_count >= 3 )); then
                if (( total_stride == 1 )); then
                    result+="${seq_start}-${seq_end},"
                else
                    result+="${seq_start}-${seq_end}:${total_stride},"
                fi
            elif (( seq_count == 2 )); then
                if (( seq_end == seq_start + 1 )); then
                    result+="${seq_start}-${seq_end},"
                else
                    result+="${seq_start},${seq_end},"
                fi
            else
                result+="${seq_start},"
            fi
        done
        echo "${result%,}"
        return
    fi

    # Fall back to simple stride detection (original algorithm)
    local result=""
    local i=0

    while (( i < count )); do
        local start="${nums[$i]}"

        # Try to find a strided or consecutive range starting here
        if (( i + 1 < count )); then
            local stride=$((nums[i+1] - nums[i]))
            local range_end="$start"
            local range_count=1

            # Count how many elements follow this stride
            local j=$((i + 1))
            while (( j < count )); do
                local expected=$((range_end + stride))
                if (( nums[j] == expected )); then
                    range_end="${nums[$j]}"
                    ((range_count++))
                    ((j++))
                else
                    break
                fi
            done

            # Decide how to output this range
            if (( range_count >= 3 )); then
                # Worth making a range (at least 3 elements)
                if (( stride == 1 )); then
                    # Consecutive range: start-end
                    result+="${start}-${range_end},"
                else
                    # Strided range: start-end:stride
                    result+="${start}-${range_end}:${stride},"
                fi
                i=$j
            elif (( range_count == 2 && stride == 1 )); then
                # Two consecutive: start-end is shorter than start,end for small numbers
                result+="${start}-${range_end},"
                i=$j
            else
                # Single element or not worth a range
                result+="${start},"
                ((i++))
            fi
        else
            # Last element
            result+="${start},"
            ((i++))
        fi
    done

    # Remove trailing comma
    echo "${result%,}"
}

# Split indices into chunks that fit within command-line limits
MAX_ARRAY_SPEC_LEN=3000

# Submit array job in batches if needed
# Usage: submit_array_batched <indices> <mem> <time> <throttle> <script> [args...]
submit_array_batched() {
    local indices="$1"
    local mem="$2"
    local time="$3"
    local throttle="$4"
    local script="$5"
    shift 5
    local script_args="$*"

    local compressed=$(compress_indices_to_ranges "$indices")
    local len=${#compressed}

    if (( len <= MAX_ARRAY_SPEC_LEN )); then
        # Fits in one submission
        local array_opt="--array=$compressed"
        [[ -n "$throttle" ]] && array_opt="--array=${compressed}%${throttle}"

        sbatch --parsable \
            "$array_opt" \
            --mem="$mem" \
            --time="$time" \
            --spread-job \
            --export=ALL \
            "$script" $script_args 2>&1
        return $?
    fi

    # Need to split into batches
    echo "Array spec too long ($len chars), splitting into batches..." >&2

    local -a nums
    IFS=',' read -ra nums <<< "$indices"
    local total=${#nums[@]}
    local batch_size=500  # Submit 500 indices at a time
    local first_job=""

    for ((start=0; start<total; start+=batch_size)); do
        local end=$((start + batch_size))
        (( end > total )) && end=$total

        # Extract batch
        local batch_indices=""
        for ((j=start; j<end; j++)); do
            batch_indices+="${nums[$j]},"
        done
        batch_indices="${batch_indices%,}"

        # Compress this batch
        local batch_compressed=$(compress_indices_to_ranges "$batch_indices")
        local array_opt="--array=$batch_compressed"
        [[ -n "$throttle" ]] && array_opt="--array=${batch_compressed}%${throttle}"

        local job_id
        job_id=$(sbatch --parsable \
            "$array_opt" \
            --mem="$mem" \
            --time="$time" \
            --spread-job \
            --export=ALL \
            "$script" $script_args 2>&1)

        if [[ ! "$job_id" =~ ^[0-9]+$ ]]; then
            echo "ERROR: Batch submission failed: $job_id" >&2
            return 1
        fi

        echo "  Submitted batch $((start/batch_size + 1)): job $job_id (indices $start-$((end-1)))" >&2

        # Track first job for dependency chain
        [[ -z "$first_job" ]] && first_job="$job_id"
    done

    # Return first job ID (handler will depend on this)
    echo "$first_job"
}

# ============================================================
# 1. Query failure indices from parent job
# ============================================================
echo "Checking parent job $PARENT_JOB for failures..."

# Wait a moment for sacct to update
sleep 2

# Get failure indices by state
oom_indices=$(get_indices_by_state "$PARENT_JOB" "OUT_OF_MEMORY")
timeout_indices=$(get_indices_by_state "$PARENT_JOB" "TIMEOUT")
other_failed_indices=$(get_indices_by_state "$PARENT_JOB" "FAILED|CANCELLED" "OUT_OF_MEMORY|TIMEOUT")

# Get counts
completed_count=$(sacct -nX -j "$PARENT_JOB" -o State --parsable2 2>/dev/null | grep -c "COMPLETED" || echo "0")
total_count=$(sacct -nX -j "$PARENT_JOB" -o State --parsable2 2>/dev/null | wc -l)
oom_count=$(count_indices "$oom_indices")
timeout_count=$(count_indices "$timeout_indices")
other_failed_count=$(count_indices "$other_failed_indices")

echo "Results for job $PARENT_JOB:"
echo "  Total tasks:     $total_count"
echo "  Completed:       $completed_count"
echo "  OOM failures:    $oom_count (will escalate memory)"
echo "  TIMEOUT:         $timeout_count (will escalate time)"
echo "  Other failures:  $other_failed_count (not retrying)"
echo ""

# ============================================================
# 2. Check if any escalation needed
# ============================================================
if [[ -z "$oom_indices" ]] && [[ -z "$timeout_indices" ]]; then
    echo "No OOM or TIMEOUT tasks detected."
    echo "Chain $CHAIN_ID complete - all tasks succeeded."

    # Update checkpoint
    if [[ -n "$CHECKPOINT_DIR" ]] && [[ -f "${CHECKPOINT_DIR}/${CHAIN_ID}.checkpoint" ]]; then
        echo "Updating checkpoint to COMPLETED..."
        python3 "$PYLIB" mark-completed "${CHECKPOINT_DIR}/${CHAIN_ID}.checkpoint" "$completed_count"
    fi

    # Log to external DB
    if [[ "$LOGGING_ENABLED" == "true" ]] && [[ -n "$LOGGING_DB_PATH" ]]; then
        python3 "$PYLIB" log-action "$LOGGING_DB_PATH" "$CHAIN_ID" "COMPLETE" \
            --job-id "$PARENT_JOB" --memory-level "$CURRENT_LEVEL" --time-level "$CURRENT_TIME_LEVEL" 2>/dev/null || true
    fi

    exit 0
fi

# Track what we're escalating
escalate_memory=false
escalate_time=false
next_level="$CURRENT_LEVEL"
next_time_level="$CURRENT_TIME_LEVEL"

# ============================================================
# 3. Handle OOM escalation (memory)
# ============================================================
current_mem=$(get_memory_for_level "$CURRENT_LEVEL")

if [[ -n "$oom_indices" ]]; then
    next_level=$((CURRENT_LEVEL + 1))

    if (( next_level > MAX_LEVEL )) || (( next_level >= ${#LADDER[@]} )); then
        echo "ERROR: Already at maximum memory level ($current_mem)."
        echo "Cannot escalate memory further. $oom_count OOM tasks failed."
        echo "OOM Failed indices: $oom_indices"

        # Update checkpoint as FAILED_MAX_MEMORY
        if [[ -n "$CHECKPOINT_DIR" ]] && [[ -f "${CHECKPOINT_DIR}/${CHAIN_ID}.checkpoint" ]]; then
            python3 "$PYLIB" mark-failed "${CHECKPOINT_DIR}/${CHAIN_ID}.checkpoint" "$oom_indices" MEMORY 2>/dev/null || true
        fi

        # If we also have timeout failures, we can still process those
        if [[ -z "$timeout_indices" ]]; then
            exit 1
        fi
    else
        escalate_memory=true
        next_mem=$(get_memory_for_level "$next_level")
        echo "Will escalate memory: $current_mem -> $next_mem for $oom_count tasks"
    fi
fi

# ============================================================
# 4. Handle TIMEOUT escalation (time)
# ============================================================
current_time=$(get_time_for_level "$CURRENT_TIME_LEVEL")

if [[ -n "$timeout_indices" ]]; then
    next_time_level=$((CURRENT_TIME_LEVEL + 1))

    if (( next_time_level > TIME_MAX_LEVEL )) || (( next_time_level >= ${#TIME_LADDER_ARR[@]} )); then
        echo "ERROR: Already at maximum time level ($current_time)."
        echo "Cannot escalate time further. $timeout_count TIMEOUT tasks failed."
        echo "TIMEOUT Failed indices: $timeout_indices"

        # Update checkpoint as FAILED_MAX_TIME
        if [[ -n "$CHECKPOINT_DIR" ]] && [[ -f "${CHECKPOINT_DIR}/${CHAIN_ID}.checkpoint" ]]; then
            python3 "$PYLIB" mark-failed "${CHECKPOINT_DIR}/${CHAIN_ID}.checkpoint" "$timeout_indices" TIME 2>/dev/null || true
        fi

        # If we also have OOM failures being processed, continue with those
        if [[ "$escalate_memory" != "true" ]]; then
            exit 1
        fi
    else
        escalate_time=true
        next_time=$(get_time_for_level "$next_time_level")
        echo "Will escalate time: $current_time -> $next_time for $timeout_count tasks"
    fi
fi

# Check if we have anything to retry
if [[ "$escalate_memory" != "true" ]] && [[ "$escalate_time" != "true" ]]; then
    echo "No tasks to retry after max level checks."
    exit 1
fi

# ============================================================
# 5. Helper function to submit a retry job with handler chain
# ============================================================
# Usage: submit_retry_with_handler <indices> <mem> <time> <mem_level> <time_level> <type>
# Returns: job_id or exits on failure
submit_retry_with_handler() {
    local indices="$1"
    local mem="$2"
    local time="$3"
    local mem_level="$4"
    local time_level="$5"
    local escalation_type="$6"  # "OOM" or "TIMEOUT"

    local task_count=$(count_indices "$indices")
    echo ""
    echo "--- Submitting $escalation_type retry ---"
    echo "  Indices: $indices ($task_count tasks)"
    echo "  Memory: $mem (level $mem_level)"
    echo "  Time: $time (level $time_level)"

    # Submit the retry job
    local job_id
    job_id=$(submit_array_batched "$indices" "$mem" "$time" "$ARRAY_THROTTLE" "$SCRIPT" $SCRIPT_ARGS)
    local status=$?

    if [[ $status -ne 0 ]] || [[ ! "$job_id" =~ ^[0-9]+$ ]]; then
        echo "ERROR: Failed to submit $escalation_type retry job: $job_id"
        return 1
    fi
    echo "  Retry job submitted: $job_id"

    # Build export string for next handler
    local export_vars="PARENT_JOB=$job_id"
    export_vars+=",CURRENT_LEVEL=$mem_level"
    export_vars+=",CURRENT_TIME_LEVEL=$time_level"
    export_vars+=",CURRENT_TIME=$time"
    export_vars+=",CHAIN_ID=$CHAIN_ID"
    export_vars+=",SCRIPT=$SCRIPT"
    export_vars+=",SCRIPT_ARGS=$SCRIPT_ARGS"
    export_vars+=",MAX_LEVEL=$MAX_LEVEL"
    export_vars+=",TIME_MAX_LEVEL=$TIME_MAX_LEVEL"
    export_vars+=",MEMORY_LADDER=$MEMORY_LADDER"
    export_vars+=",TIME_LADDER=$TIME_LADDER"
    export_vars+=",CHECKPOINT_DIR=$CHECKPOINT_DIR"
    export_vars+=",HANDLER_SCRIPT=$HANDLER_SCRIPT"
    export_vars+=",PYLIB=$PYLIB"
    export_vars+=",LOGGING_ENABLED=$LOGGING_ENABLED"
    export_vars+=",LOGGING_DB_PATH=$LOGGING_DB_PATH"
    [[ -n "$ARRAY_THROTTLE" ]] && export_vars+=",ARRAY_THROTTLE=$ARRAY_THROTTLE"

    # Submit next handler with afternotok dependency
    local handler_id
    handler_id=$(sbatch --parsable \
        --dependency=afternotok:$job_id \
        --export="$export_vars" \
        "$HANDLER_SCRIPT" 2>&1)

    if [[ ! "$handler_id" =~ ^[0-9]+$ ]]; then
        echo "  WARNING: Failed to submit handler: $handler_id"
        handler_id="0"
    else
        echo "  Handler submitted: $handler_id"
    fi

    # Submit success handler
    local script_dir="$(dirname "$HANDLER_SCRIPT")"
    local success_script="${script_dir}/mem-escalate-success.sh"

    if [[ -f "$success_script" ]]; then
        local success_export="PARENT_JOB=$job_id"
        success_export+=",CHAIN_ID=$CHAIN_ID"
        success_export+=",CHECKPOINT_DIR=$CHECKPOINT_DIR"
        success_export+=",PYLIB=$PYLIB"
        success_export+=",CURRENT_LEVEL=$mem_level"
        success_export+=",ARRAY_SPEC=$indices"
        success_export+=",LOGGING_ENABLED=$LOGGING_ENABLED"
        success_export+=",LOGGING_DB_PATH=$LOGGING_DB_PATH"

        local success_id
        success_id=$(sbatch --parsable \
            --job-name="escalate-success" \
            --output="${CHECKPOINT_DIR}/../success-handler-%j.out" \
            --time=00:05:00 \
            --mem=100M \
            --dependency=afterok:$job_id \
            --export="$success_export" \
            "$success_script" 2>&1)

        [[ "$success_id" =~ ^[0-9]+$ ]] && echo "  Success handler: $success_id"
    fi

    # Return job_id and handler_id via stdout
    echo "JOB_RESULT:$job_id:$handler_id"
}

# ============================================================
# 6. Submit separate retry jobs for OOM and TIMEOUT
# ============================================================
oom_job=""
oom_handler=""
timeout_job=""
timeout_handler=""

# Submit OOM retry job (escalated memory, current time)
if [[ "$escalate_memory" == "true" ]]; then
    result=$(submit_retry_with_handler "$oom_indices" "$next_mem" "$current_time" "$next_level" "$CURRENT_TIME_LEVEL" "OOM")
    echo "$result" | grep -v "^JOB_RESULT:"
    job_line=$(echo "$result" | grep "^JOB_RESULT:")
    oom_job=$(echo "$job_line" | cut -d: -f2)
    oom_handler=$(echo "$job_line" | cut -d: -f3)
fi

# Submit TIMEOUT retry job (current memory, escalated time)
if [[ "$escalate_time" == "true" ]]; then
    result=$(submit_retry_with_handler "$timeout_indices" "$current_mem" "$next_time" "$CURRENT_LEVEL" "$next_time_level" "TIMEOUT")
    echo "$result" | grep -v "^JOB_RESULT:"
    job_line=$(echo "$result" | grep "^JOB_RESULT:")
    timeout_job=$(echo "$job_line" | cut -d: -f2)
    timeout_handler=$(echo "$job_line" | cut -d: -f3)
fi

# ============================================================
# 7. Update checkpoint with separate job IDs
# ============================================================
if [[ -n "$CHECKPOINT_DIR" ]] && [[ -f "${CHECKPOINT_DIR}/${CHAIN_ID}.checkpoint" ]]; then
    echo ""
    echo "Updating checkpoint..."
    comp_count="${completed_count:-0}"

    if [[ "$escalate_memory" == "true" ]] && [[ -n "$oom_job" ]]; then
        python3 "$PYLIB" update-escalation "${CHECKPOINT_DIR}/${CHAIN_ID}.checkpoint" \
            "$next_level" "$next_mem" "$oom_indices" "$oom_job" "${oom_handler:-0}" "$comp_count" "$oom_count" 2>/dev/null || \
            echo "Warning: Could not update OOM escalation in checkpoint"
    fi

    if [[ "$escalate_time" == "true" ]] && [[ -n "$timeout_job" ]]; then
        python3 "$PYLIB" update-time-escalation "${CHECKPOINT_DIR}/${CHAIN_ID}.checkpoint" \
            "$next_time_level" "$next_time" "$timeout_indices" "$timeout_job" "${timeout_handler:-0}" "$comp_count" "$timeout_count" 2>/dev/null || \
            echo "Warning: Could not update TIMEOUT escalation in checkpoint"
    fi
fi

# Log to external DB
if [[ "$LOGGING_ENABLED" == "true" ]] && [[ -n "$LOGGING_DB_PATH" ]]; then
    if [[ "$escalate_memory" == "true" ]] && [[ -n "$oom_job" ]]; then
        python3 "$PYLIB" log-action "$LOGGING_DB_PATH" "$CHAIN_ID" "ESCALATE_MEM" \
            --job-id "$oom_job" --memory-level "$next_level" --indices "$oom_indices" 2>/dev/null || true
    fi

    if [[ "$escalate_time" == "true" ]] && [[ -n "$timeout_job" ]]; then
        python3 "$PYLIB" log-action "$LOGGING_DB_PATH" "$CHAIN_ID" "ESCALATE_TIME" \
            --job-id "$timeout_job" --time-level "$next_time_level" --indices "$timeout_indices" 2>/dev/null || true
    fi
fi

# ============================================================
# Summary
# ============================================================
echo ""
echo "========================================"
echo "Escalation Summary"
echo "========================================"
echo "Chain ID:        $CHAIN_ID"
echo "Previous job:    $PARENT_JOB"
echo "  Memory:        level $CURRENT_LEVEL ($current_mem)"
echo "  Time:          level $CURRENT_TIME_LEVEL ($current_time)"
echo ""
if [[ "$escalate_memory" == "true" ]] && [[ -n "$oom_job" ]]; then
    echo "OOM Retry:       Job $oom_job ($oom_count tasks)"
    echo "  Memory:        level $next_level ($next_mem)"
    echo "  Handler:       ${oom_handler:-NONE}"
fi
if [[ "$escalate_time" == "true" ]] && [[ -n "$timeout_job" ]]; then
    echo "TIMEOUT Retry:   Job $timeout_job ($timeout_count tasks)"
    echo "  Time:          level $next_time_level ($next_time)"
    echo "  Handler:       ${timeout_handler:-NONE}"
fi
echo ""
echo "Monitor:"
[[ -n "$oom_job" ]] && echo "  sacct -j $oom_job --format=JobID,State,MaxRSS,Elapsed"
[[ -n "$timeout_job" ]] && echo "  sacct -j $timeout_job --format=JobID,State,Elapsed,Timelimit"
echo "========================================"
