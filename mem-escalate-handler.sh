#!/bin/bash
#SBATCH --job-name=escalate-handler
#SBATCH --output=/data/tracker/handler-%j.log
#SBATCH --time=00:10:00
#SBATCH --mem=100M

# Escalation Handler Script (Levels-Based)
# =========================================
# This script runs as a Slurm job with --dependency=afternotok:<parent_job>
# It detects OOM and TIMEOUT failures and escalates to the next level.
#
# With levels-based config, each level defines partition + memory + time together.
# Both OOM and TIMEOUT failures escalate to the next level.
#
# Environment variables received:
#   PARENT_JOB        - Job ID to check for failures
#   CURRENT_LEVEL     - Current escalation level (0-based)
#   CHAIN_ID          - Unique chain identifier for tracking
#   SCRIPT            - Original script to run
#   PARTITION         - Current partition
#   MAX_LEVEL         - Maximum escalation level
#   LEVEL_N_PARTITION - Partition for level N
#   LEVEL_N_MEM       - Memory for level N
#   LEVEL_N_TIME      - Time for level N
#   ARRAY_THROTTLE    - Optional: max concurrent array tasks
#   CHECKPOINT_DIR    - Directory for checkpoint files
#   HANDLER_SCRIPT    - Path to this handler script
#   PYLIB             - Path to Python library
#   LOGGING_ENABLED   - Whether to log to database
#   DB_PATH           - Path to SQLite database
#   OUTPUT_DIR        - Directory for job outputs
#   OUTPUT_PATTERN    - Pattern for stdout files
#   ERROR_PATTERN     - Pattern for stderr files

set -o pipefail

# Decode partition values (| was used to escape commas in --export)
PARTITION="${PARTITION//|/,}"
for i in $(seq 0 ${MAX_LEVEL:-10}); do
    pvar="LEVEL_${i}_PARTITION"
    if [[ -n "${!pvar}" ]]; then
        eval "$pvar=\"\${!pvar//|/,}\""
    fi
done

# Find Python library
if [[ -z "$PYLIB" ]]; then
    SCRIPT_DIR="$(dirname "$(readlink -f "$0")")"
    PYLIB="$SCRIPT_DIR/mem-escalate-lib.py"
fi

# Disable Python bytecode caching
export PYTHONDONTWRITEBYTECODE=1

echo "========================================"
echo "Escalation Handler"
echo "========================================"
echo "Handler Job ID:  $SLURM_JOB_ID"
echo "Parent Job:      $PARENT_JOB"
echo "Current Level:   $CURRENT_LEVEL (max: $MAX_LEVEL)"
echo "Chain ID:        $CHAIN_ID"
echo "Script:          $SCRIPT"
echo "Timestamp:       $(date -Iseconds)"
echo ""

# Display levels configuration
echo "Escalation Levels:"
for i in $(seq 0 $MAX_LEVEL); do
    pvar="LEVEL_${i}_PARTITION"
    mvar="LEVEL_${i}_MEM"
    tvar="LEVEL_${i}_TIME"
    if [[ -n "${!pvar}" ]] || [[ -n "${!mvar}" ]] || [[ -n "${!tvar}" ]]; then
        marker=""
        [[ "$i" == "$CURRENT_LEVEL" ]] && marker=" <-- current"
        printf "  Level %d: partition=%-12s mem=%-6s time=%s%s\n" \
            "$i" "${!pvar:-?}" "${!mvar:-?}" "${!tvar:-?}" "$marker"
    fi
done
echo ""

# Load checkpoint to get safe SCRIPT_ARGS
if [[ -f "${CHECKPOINT_DIR}/${CHAIN_ID}.checkpoint" ]]; then
    eval "$(python3 "$PYLIB" load-checkpoint "${CHECKPOINT_DIR}/${CHAIN_ID}.checkpoint")"
fi

# Compress indices to ranges using Python library
compress_indices_to_ranges() {
    local indices="$1"
    [[ -z "$indices" ]] && return
    python3 "$PYLIB" compress-indices "$indices"
}

# Submit array job (handles batching if needed)
# Use exported value from mem-escalate.sh or default to 10000
MAX_ARRAY_SPEC_LEN="${MAX_ARRAY_SPEC_LEN:-10000}"

submit_array_batched() {
    local indices="$1"
    local mem="$2"
    local time="$3"
    local throttle="$4"
    local script="$5"
    shift 5
    local script_args=("$@")

    local compressed=$(compress_indices_to_ranges "$indices")
    local len=${#compressed}

    local partition_opt=""
    [[ -n "$PARTITION" ]] && partition_opt="--partition=$PARTITION"

    if (( len <= MAX_ARRAY_SPEC_LEN )); then
        local array_opt="--array=$compressed"
        [[ -n "$throttle" ]] && array_opt="--array=${compressed}%${throttle}"

        # Build export with user-specified vars
        local export_opt="ALL"
        [[ -n "$EXPORT_VARS" ]] && export_opt="ALL,$EXPORT_VARS"

        local sbatch_output
        sbatch_output=$(sbatch --parsable \
            $partition_opt \
            "$array_opt" \
            --mem="$mem" \
            --time="$time" \
            --spread-job \
            --export="$export_opt" \
            "$script" "${script_args[@]}" 2>&1)

        # Extract job ID from last line (sbatch may print warnings before the ID)
        echo "$sbatch_output" | tail -1
        return $?
    fi

    # Split into batches if too long
    echo "Array spec too long ($len chars), splitting into batches..." >&2

    local -a nums
    IFS=',' read -ra nums <<< "$indices"
    local total=${#nums[@]}
    local batch_size=500
    local all_jobs=""

    for ((start=0; start<total; start+=batch_size)); do
        local end=$((start + batch_size))
        (( end > total )) && end=$total

        local batch_indices=""
        for ((j=start; j<end; j++)); do
            batch_indices+="${nums[$j]},"
        done
        batch_indices="${batch_indices%,}"

        local batch_compressed=$(compress_indices_to_ranges "$batch_indices")
        local array_opt="--array=$batch_compressed"
        [[ -n "$throttle" ]] && array_opt="--array=${batch_compressed}%${throttle}"

        # Build export with user-specified vars
        local export_opt="ALL"
        [[ -n "$EXPORT_VARS" ]] && export_opt="ALL,$EXPORT_VARS"

        local job_id sbatch_output
        sbatch_output=$(sbatch --parsable \
            $partition_opt \
            "$array_opt" \
            --mem="$mem" \
            --time="$time" \
            --spread-job \
            --export="$export_opt" \
            "$script" "${script_args[@]}" 2>&1)

        # Extract job ID from last line (sbatch may print warnings before the ID)
        job_id=$(echo "$sbatch_output" | tail -1)

        if [[ ! "$job_id" =~ ^[0-9]+$ ]]; then
            echo "ERROR: Batch submission failed: $sbatch_output" >&2
            return 1
        fi

        echo "  Submitted batch $((start/batch_size + 1)): job $job_id" >&2
        # Collect all job IDs
        [[ -n "$all_jobs" ]] && all_jobs+=","
        all_jobs+="$job_id"
    done

    # Return all job IDs (comma-separated)
    echo "$all_jobs"
}

# ============================================================
# 1. Query failure indices from parent job
# ============================================================
echo "Checking parent job $PARENT_JOB for failures..."

sleep 2  # Wait for sacct to update

eval "$(python3 "$PYLIB" analyze-job "$PARENT_JOB")"

# Variables set: TOTAL_COUNT, COMPLETED_COUNT, OOM_COUNT, TIMEOUT_COUNT
# OOM_INDICES, TIMEOUT_INDICES, OTHER_FAILED_INDICES, OTHER_FAILED_COUNT

echo "Results for job $PARENT_JOB:"
echo "  Total tasks:     $TOTAL_COUNT"
echo "  Completed:       $COMPLETED_COUNT"
echo "  OOM failures:    $OOM_COUNT"
echo "  TIMEOUT:         $TIMEOUT_COUNT"
echo "  Other failures:  $OTHER_FAILED_COUNT (not retrying)"
echo ""

# Update database with parent job results
if [[ "$LOGGING_ENABLED" == "true" ]] && [[ -n "$DB_PATH" ]]; then
    python3 "$PYLIB" db-update-round "$DB_PATH" "$CHAIN_ID" "$PARENT_JOB" "ESCALATING" \
        --oom-count "$OOM_COUNT" \
        --timeout-count "$TIMEOUT_COUNT" \
        --oom-indices "$OOM_INDICES" \
        --timeout-indices "$TIMEOUT_INDICES" 2>/dev/null || true
    # Save task metrics with output paths
    python3 "$PYLIB" db-save-tasks "$DB_PATH" "$CHAIN_ID" "$PARENT_JOB" 2>/dev/null || true
fi

# ============================================================
# Save failed indices to dedicated log folder for future analysis
# ============================================================
INDICES_DIR="${CHECKPOINT_DIR}/../indices/${CHAIN_ID}"
mkdir -p "$INDICES_DIR"

# Save each category of failures
echo "Saving failure indices to $INDICES_DIR ..."
{
    echo "# Round $((CURRENT_LEVEL + 1)) - Job $PARENT_JOB"
    echo "# Generated: $(date -Iseconds)"
    echo "# OOM: $OOM_COUNT, TIMEOUT: $TIMEOUT_COUNT, NOT_RETRIED: $OTHER_FAILED_COUNT"
} > "$INDICES_DIR/round-$((CURRENT_LEVEL + 1))-summary.txt"

# Save OOM indices (one per line for easy processing)
if [[ -n "$OOM_INDICES" ]]; then
    echo "$OOM_INDICES" | tr ',' '\n' > "$INDICES_DIR/round-$((CURRENT_LEVEL + 1))-oom.txt"
    echo "  OOM indices: $INDICES_DIR/round-$((CURRENT_LEVEL + 1))-oom.txt ($OOM_COUNT tasks)"
fi

# Save TIMEOUT indices
if [[ -n "$TIMEOUT_INDICES" ]]; then
    echo "$TIMEOUT_INDICES" | tr ',' '\n' > "$INDICES_DIR/round-$((CURRENT_LEVEL + 1))-timeout.txt"
    echo "  TIMEOUT indices: $INDICES_DIR/round-$((CURRENT_LEVEL + 1))-timeout.txt ($TIMEOUT_COUNT tasks)"
fi

# Save NOT_RETRIED indices (code errors)
if [[ -n "$OTHER_FAILED_INDICES" ]]; then
    echo "$OTHER_FAILED_INDICES" | tr ',' '\n' > "$INDICES_DIR/round-$((CURRENT_LEVEL + 1))-not-retried.txt"
    echo "  NOT_RETRIED indices: $INDICES_DIR/round-$((CURRENT_LEVEL + 1))-not-retried.txt ($OTHER_FAILED_COUNT tasks)"
fi

# ============================================================
# 2. Combine all failed indices (OOM + TIMEOUT)
# ============================================================
failed_indices=""
if [[ -n "$OOM_INDICES" ]]; then
    failed_indices="$OOM_INDICES"
fi
if [[ -n "$TIMEOUT_INDICES" ]]; then
    if [[ -n "$failed_indices" ]]; then
        failed_indices="${failed_indices},${TIMEOUT_INDICES}"
    else
        failed_indices="$TIMEOUT_INDICES"
    fi
fi

failed_count=$((OOM_COUNT + TIMEOUT_COUNT))

# ============================================================
# 3. Check if escalation is needed
# ============================================================
if [[ -z "$failed_indices" ]]; then
    echo "No OOM or TIMEOUT tasks detected."
    echo "Chain $CHAIN_ID complete - all tasks succeeded."

    if [[ -n "$CHECKPOINT_DIR" ]] && [[ -f "${CHECKPOINT_DIR}/${CHAIN_ID}.checkpoint" ]]; then
        python3 "$PYLIB" mark-completed "${CHECKPOINT_DIR}/${CHAIN_ID}.checkpoint" "$COMPLETED_COUNT"
    fi

    if [[ "$LOGGING_ENABLED" == "true" ]] && [[ -n "$DB_PATH" ]]; then
        python3 "$PYLIB" log-action "$DB_PATH" "$CHAIN_ID" "COMPLETE" \
            --job-id "$PARENT_JOB" --memory-level "$CURRENT_LEVEL" 2>/dev/null || true
    fi

    exit 0
fi

# ============================================================
# 4. Check if we can escalate to next level
# ============================================================
next_level=$((CURRENT_LEVEL + 1))

if (( next_level > MAX_LEVEL )); then
    echo "ERROR: Already at maximum level ($CURRENT_LEVEL)."
    echo "Cannot escalate further. $failed_count tasks failed."
    echo "Failed indices: $(compress_indices_to_ranges "$failed_indices")"

    if [[ -n "$CHECKPOINT_DIR" ]] && [[ -f "${CHECKPOINT_DIR}/${CHAIN_ID}.checkpoint" ]]; then
        python3 "$PYLIB" mark-failed "${CHECKPOINT_DIR}/${CHAIN_ID}.checkpoint" "$failed_indices" LEVEL 2>/dev/null || true
    fi

    exit 1
fi

# Get next level configuration
next_partition_var="LEVEL_${next_level}_PARTITION"
next_mem_var="LEVEL_${next_level}_MEM"
next_time_var="LEVEL_${next_level}_TIME"

next_partition="${!next_partition_var}"
next_mem="${!next_mem_var}"
next_time="${!next_time_var}"

# Update PARTITION for job submission
[[ -n "$next_partition" ]] && PARTITION="$next_partition"

echo "Escalating to level $next_level:"
echo "  Partition: $PARTITION"
echo "  Memory:    $next_mem"
echo "  Time:      $next_time"
echo "  Tasks:     $failed_count"
echo ""

# ============================================================
# 5. Submit retry job at next level
# ============================================================
echo "Submitting retry job..."

# job_ids may be comma-separated if batched (e.g., "12345,12346,12347")
job_ids=$(submit_array_batched "$failed_indices" "$next_mem" "$next_time" "$ARRAY_THROTTLE" "$SCRIPT" "${SCRIPT_ARGS[@]}")
status=$?

# Validate - at least one job ID should be numeric
first_job="${job_ids%%,*}"
if [[ $status -ne 0 ]] || [[ ! "$first_job" =~ ^[0-9]+$ ]]; then
    echo "ERROR: Failed to submit retry job: $job_ids"
    exit 1
fi
echo "Retry job submitted: $job_ids"

# Build export string for next handler (pass all job IDs)
# Note: Partition values may contain commas (e.g., "day,week,pi_jetz")
# Replace commas with | to avoid breaking Slurm's --export parsing
export_vars="PARENT_JOB=$job_ids"
export_vars+=",CURRENT_LEVEL=$next_level"
export_vars+=",CHAIN_ID=$CHAIN_ID"
export_vars+=",SCRIPT=$SCRIPT"
export_vars+=",PARTITION=${PARTITION//,/|}"
export_vars+=",MAX_LEVEL=$MAX_LEVEL"
export_vars+=",CHECKPOINT_DIR=$CHECKPOINT_DIR"
export_vars+=",HANDLER_SCRIPT=$HANDLER_SCRIPT"
export_vars+=",PYLIB=$PYLIB"
export_vars+=",LOGGING_ENABLED=$LOGGING_ENABLED"
export_vars+=",DB_PATH=$DB_PATH"
export_vars+=",OUTPUT_DIR=$OUTPUT_DIR"
export_vars+=",OUTPUT_PATTERN=$OUTPUT_PATTERN"
export_vars+=",ERROR_PATTERN=$ERROR_PATTERN"
[[ -n "$ARRAY_THROTTLE" ]] && export_vars+=",ARRAY_THROTTLE=$ARRAY_THROTTLE"

# Pass all level configs (partition commas escaped as |)
for i in $(seq 0 $MAX_LEVEL); do
    pvar="LEVEL_${i}_PARTITION"
    mvar="LEVEL_${i}_MEM"
    tvar="LEVEL_${i}_TIME"
    [[ -n "${!pvar}" ]] && export_vars+=",${pvar}=${!pvar//,/|}"
    [[ -n "${!mvar}" ]] && export_vars+=",${mvar}=${!mvar}"
    [[ -n "${!tvar}" ]] && export_vars+=",${tvar}=${!tvar}"
done

# Submit next handler with dependency on ALL job IDs
partition_opt=""
[[ -n "$PARTITION" ]] && partition_opt="--partition=$PARTITION"

# For batch jobs (multiple job IDs), use afterany to avoid DependencyNeverSatisfied
# when some batches complete 100% successfully. Handler will check for failures.
# For single job, use afternotok for efficiency.
dep_str=""
job_count=$(echo "$job_ids" | tr ',' '\n' | wc -l)
if (( job_count > 1 )); then
    # Multiple batches: use afterany (handler checks for failures inside)
    for jid in ${job_ids//,/ }; do
        [[ -n "$dep_str" ]] && dep_str+=","
        dep_str+="afterany:$jid"
    done
else
    # Single job: use afternotok (more efficient)
    dep_str="afternotok:$job_ids"
fi

sbatch_output=$(sbatch --parsable \
    $partition_opt \
    --dependency="$dep_str" \
    --export="$export_vars" \
    "$HANDLER_SCRIPT" 2>&1)

# Extract job ID from last line (sbatch may print warnings before the ID)
handler_id=$(echo "$sbatch_output" | tail -1)

if [[ ! "$handler_id" =~ ^[0-9]+$ ]]; then
    echo "WARNING: Failed to submit handler: $sbatch_output"
    handler_id="0"
else
    echo "Handler submitted: $handler_id"
fi

# Submit success handler
script_dir="$(dirname "$HANDLER_SCRIPT")"
success_script="${script_dir}/mem-escalate-success.sh"

if [[ -f "$success_script" ]]; then
    success_export="PARENT_JOB=$job_ids"
    success_export+=",CHAIN_ID=$CHAIN_ID"
    success_export+=",CHECKPOINT_DIR=$CHECKPOINT_DIR"
    success_export+=",PYLIB=$PYLIB"
    success_export+=",CURRENT_LEVEL=$next_level"
    success_export+=",ARRAY_SPEC=$failed_indices"
    success_export+=",LOGGING_ENABLED=$LOGGING_ENABLED"
    success_export+=",DB_PATH=$DB_PATH"
    success_export+=",HANDLER_JOB_ID=${handler_id:-0}"

    # Build dependency for all jobs
    # For batch jobs, use afterany to avoid DependencyNeverSatisfied
    ok_dep_str=""
    if (( job_count > 1 )); then
        # Multiple batches: use afterany (success handler checks inside)
        for jid in ${job_ids//,/ }; do
            [[ -n "$ok_dep_str" ]] && ok_dep_str+=","
            ok_dep_str+="afterany:$jid"
        done
    else
        # Single job: use afterok
        ok_dep_str="afterok:$job_ids"
    fi

    sbatch_output=$(sbatch --parsable \
        $partition_opt \
        --job-name="escalate-success" \
        --output="${CHECKPOINT_DIR}/../success-handler-%j.out" \
        --time=00:05:00 \
        --mem=100M \
        --dependency="$ok_dep_str" \
        --export="$success_export" \
        "$success_script" 2>&1)

    # Extract job ID from last line (sbatch may print warnings before the ID)
    success_id=$(echo "$sbatch_output" | tail -1)
    [[ "$success_id" =~ ^[0-9]+$ ]] && echo "Success handler: $success_id"
fi

# ============================================================
# 6. Update checkpoint and database
# ============================================================
if [[ -n "$CHECKPOINT_DIR" ]] && [[ -f "${CHECKPOINT_DIR}/${CHAIN_ID}.checkpoint" ]]; then
    echo ""
    echo "Updating checkpoint..."
    python3 "$PYLIB" update-escalation "${CHECKPOINT_DIR}/${CHAIN_ID}.checkpoint" \
        "$next_level" "$next_mem" "$failed_indices" "$job_ids" "${handler_id:-0}" \
        "$COMPLETED_COUNT" "$failed_count" \
        --oom-count "$OOM_COUNT" \
        --timeout-count "$TIMEOUT_COUNT" \
        --failed-count "$OTHER_FAILED_COUNT" 2>/dev/null || \
        echo "Warning: Could not update checkpoint"
fi

# Add new round to database
if [[ "$LOGGING_ENABLED" == "true" ]] && [[ -n "$DB_PATH" ]]; then
    array_spec=$(compress_indices_to_ranges "$failed_indices")

    python3 "$PYLIB" db-add-round "$DB_PATH" "$CHAIN_ID" "$first_job" "${handler_id:-0}" "$array_spec" \
        "$next_level" "$next_mem" \
        --time-level "$next_level" \
        --time "$next_time" \
        --partition "$PARTITION" \
        --output-pattern "$OUTPUT_PATTERN" \
        --error-pattern "$ERROR_PATTERN" 2>/dev/null || true

    python3 "$PYLIB" log-action "$DB_PATH" "$CHAIN_ID" "ESCALATE" \
        --job-id "$first_job" --memory-level "$next_level" --indices "$failed_indices" 2>/dev/null || true
fi

# ============================================================
# Summary
# ============================================================
echo ""
echo "========================================"
echo "Escalation Summary"
echo "========================================"
echo "Chain ID:     $CHAIN_ID"
echo "Previous:     Job $PARENT_JOB (level $CURRENT_LEVEL)"
echo "  Failed:     $failed_count tasks (OOM: $OOM_COUNT, TIMEOUT: $TIMEOUT_COUNT)"
echo ""
echo "Retry:        Job(s) $job_ids (level $next_level)"
echo "  Partition:  $PARTITION"
echo "  Memory:     $next_mem"
echo "  Time:       $next_time"
echo "  Handler:    ${handler_id:-NONE}"
if [[ -n "$OTHER_FAILED_INDICES" ]]; then
    echo ""
    echo "NOT RETRYING: $OTHER_FAILED_COUNT tasks (other failures)"
fi
echo ""
echo "Monitor: sacct -j $first_job --format=JobID,State,MaxRSS,Elapsed"
echo "========================================"
