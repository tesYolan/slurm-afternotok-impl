#!/bin/bash
# mem-escalate - Automatic memory/time escalation for Slurm jobs
set -o pipefail

VERSION="0.0.1"

usage() {
    cat <<EOF
mem-escalate v${VERSION} - Automatic resource escalation for Slurm jobs

USAGE:
    ${0##*/} [OPTIONS] <script> [script-args...]

OPTIONS:
    -a, --array <spec>        Job array specification (e.g., 0-999) [REQUIRED]
    -t, --throttle <N>        Max concurrent array tasks (default: unlimited)
    -c, --config <path>       Path to YAML config file
    -l, --max-level <N>       Maximum escalation level
    -s, --status <chain_id>   Show status of an escalation chain
    -w, --watch [SEC]         Watch mode - refresh status every SEC seconds (default: 5)
        --list                List all checkpoints
        --no-wait             Submit and exit without monitoring
    -h, --help                Show this help message
    -v, --version             Show version

EXAMPLES:
    # Array job (use 0-0 for single job)
    ${0##*/} -a 0-999 ./my-job.sh

    # Array job with throttle (max 100 concurrent)
    ${0##*/} -a 0-4999 -t 100 ./my-job.sh

    # Check chain status
    ${0##*/} -s 20251210-123456-abcd

    # Watch chain status (refresh every 5s)
    ${0##*/} -s 20251210-123456-abcd -w

    # Watch with custom interval (2s)
    ${0##*/} -s 20251210-123456-abcd --watch=2

For more info, see: README.md
EOF
}

die() { echo "ERROR: $*" >&2; exit 1; }

# Get the directory where this script lives (for finding the Python library)
SCRIPT_DIR="$(dirname "$(readlink -f "$0")")"
PYLIB="$SCRIPT_DIR/mem-escalate-lib.py"

# Disable Python bytecode caching (scripts are mounted from host)
export PYTHONDONTWRITEBYTECODE=1

# ============================================================
# DEFAULT CONFIGURATION (overridden by YAML config)
# ============================================================
MAX_LEVEL=3
SACCT_DELAY=2
TRACKER_DIR="/data/tracker"
HISTORY_LOG="/data/tracker/history.log"
CHECKPOINT_DIR="/data/tracker/checkpoints"
OUTPUT_DIR="/data/tracker/outputs"
DB_PATH="/data/tracker/escalation.db"
DEFAULT_PARTITION="devel"
LOGGING_ENABLED=true

# Levels-based config flag (set by load_config)
LEVELS_CONFIG=false

# ============================================================
# ARGUMENT PARSING
# ============================================================
CONFIG_FILE=""
PARTITION=""
NO_WAIT=false
LIST_CHECKPOINTS=false
ARRAY_SPEC=""
ARRAY_THROTTLE=""
EXPORT_VARS=""
STATUS_CHAIN=""
WATCH_INTERVAL=""
SCRIPT=""
SCRIPT_ARGS=()

while [[ $# -gt 0 ]]; do
    case $1 in
        -c|--config)
            CONFIG_FILE="$2"
            shift 2
            ;;
        -p|--partition)
            PARTITION="$2"
            shift 2
            ;;
        --partition=*|-p=*)
            PARTITION="${1#*=}"
            shift
            ;;
        -l|--max-level)
            MAX_LEVEL="$2"
            shift 2
            ;;
        --no-wait)
            NO_WAIT=true
            shift
            ;;
        --list)
            LIST_CHECKPOINTS=true
            shift
            ;;
        -a|--array)
            ARRAY_SPEC="$2"
            shift 2
            ;;
        --array=*|-a=*)
            ARRAY_SPEC="${1#*=}"
            shift
            ;;
        -t|--throttle)
            ARRAY_THROTTLE="$2"
            shift 2
            ;;
        --throttle=*|-t=*)
            ARRAY_THROTTLE="${1#*=}"
            shift
            ;;
        -e|--export)
            EXPORT_VARS="$2"
            shift 2
            ;;
        --export=*|-e=*)
            EXPORT_VARS="${1#*=}"
            shift
            ;;
        -s|--status)
            STATUS_CHAIN="$2"
            shift 2
            ;;
        -w|--watch)
            # Check if next arg exists and is a number
            if [[ -n "$2" ]] && [[ "$2" =~ ^[0-9]+$ ]]; then
                WATCH_INTERVAL="$2"
                shift 2
            else
                WATCH_INTERVAL=5
                shift
            fi
            ;;
        --watch=*)
            WATCH_INTERVAL="${1#*=}"
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        -v|--version)
            echo "mem-escalate v${VERSION}"
            exit 0
            ;;
        -*)
            die "Unknown option: $1. Use --help for usage."
            ;;
        *)
            SCRIPT="$1"
            shift
            SCRIPT_ARGS=("$@")
            break
            ;;
    esac
done

# ============================================================
# INPUT VALIDATION
# ============================================================
validate_inputs() {
    # Check Python library exists
    [[ ! -f "$PYLIB" ]] && die "Python library not found: $PYLIB"

    # If script provided, validate it and handler exist
    if [[ -n "$SCRIPT" ]]; then
        [[ ! -f "$SCRIPT" ]] && die "Script not found: $SCRIPT"
        [[ ! -x "$SCRIPT" ]] && echo "WARNING: Script is not executable: $SCRIPT" >&2

        # Check handler script exists (needed for job submission)
        local handler="$SCRIPT_DIR/mem-escalate-handler.sh"
        [[ ! -f "$handler" ]] && die "Handler script not found: $handler"
        [[ ! -x "$handler" ]] && die "Handler script not executable: $handler"
    fi

    # Validate array spec format if provided
    if [[ -n "$ARRAY_SPEC" ]]; then
        if [[ ! "$ARRAY_SPEC" =~ ^[0-9]+([-,][0-9]+)*$ ]]; then
            die "Invalid array spec: $ARRAY_SPEC (expected format: 0-99 or 1,2,3)"
        fi
    fi

    # Validate throttle is a number
    if [[ -n "$ARRAY_THROTTLE" ]] && [[ ! "$ARRAY_THROTTLE" =~ ^[0-9]+$ ]]; then
        die "Invalid throttle value: $ARRAY_THROTTLE (must be a number)"
    fi

    # Validate max-level is a number
    if [[ -n "$MAX_LEVEL" ]] && [[ ! "$MAX_LEVEL" =~ ^[0-9]+$ ]]; then
        die "Invalid max-level: $MAX_LEVEL (must be a number)"
    fi
}

# ============================================================
# YAML CONFIG PARSING (via Python)
# ============================================================
load_config() {
    # Use --config if provided, otherwise default to script directory
    local config_path="${CONFIG_FILE:-$SCRIPT_DIR/escalation-target.yaml}"

    if [[ ! -f "$config_path" ]]; then
        echo "INFO: No config file found at $config_path, using defaults" >&2
        return 0
    fi

    echo "INFO: Loading config from $config_path" >&2
    eval "$(python3 "$PYLIB" load-config "$config_path")"
}

# ============================================================
# CHECKPOINT FUNCTIONS
# ============================================================
generate_chain_id() {
    # Generate unique chain ID: timestamp + random suffix
    echo "$(date +%Y%m%d-%H%M%S)-$(head /dev/urandom | tr -dc 'a-z0-9' | head -c 4)"
}

create_checkpoint() {
    local chain_id=$1
    local script=$2
    shift 2
    local args=("$@")

    # Get initial mem/time from level 0
    local initial_mem="${LEVEL_0_MEM:-1G}"
    local initial_time="${LEVEL_0_TIME:-00:05:00}"

    # Use Python library to create checkpoint (levels-based)
    python3 "$PYLIB" create-checkpoint "$CHECKPOINT_DIR" "$chain_id" "${PARTITION:-devel}" "$script" \
        "$MAX_LEVEL" "$initial_mem" "$initial_time" "${args[@]}"
}

load_checkpoint() {
    local chain_id=$1
    local checkpoint_file="${CHECKPOINT_DIR}/${chain_id}.checkpoint"

    if [[ ! -f "$checkpoint_file" ]]; then
        echo "ERROR: Checkpoint not found: $checkpoint_file" >&2
        return 1
    fi

    # Parse checkpoint and set variables
    eval "$(python3 "$PYLIB" load-checkpoint "$checkpoint_file")"
}

list_checkpoints() {
    python3 "$PYLIB" list-checkpoints "$CHECKPOINT_DIR"
}

# ============================================================
# TRACKER FUNCTIONS
# ============================================================
init_tracker() {
    mkdir -p "$CHECKPOINT_DIR"
    touch "$HISTORY_LOG"
}

log_event() {
    echo "$(date -Iseconds) $*" >> "$HISTORY_LOG"
}

# ============================================================
# OOM DETECTION
# ============================================================
detect_oom() {
    local job_id=$1
    sleep "$SACCT_DELAY"

    # Single sacct call to check state. exit code, and steps
    local result
    result=$(sacct -n -j "$job_id" -o State,ExitCode --parsable2 2>/dev/null)

    # Check for OUT_OF_MEMORY state or exit code 137 (killed by OOM)
    echo "$result" | grep -qE "OUT_OF_MEMORY|137:0"
}

check_job_success() {
    local job_id=$1
    local state
    state=$(sacct -nX -j "$job_id" -o State --parsable2 2>/dev/null | head -1)

    [[ "$state" == "COMPLETED" ]]
}

# ============================================================
# ARRAY JOB ESCALATION (handler-based)
# ============================================================
# For array jobs (--array=0-999): submits once, then a separate
# handler job runs after failures to detect OOM/TIMEOUT and
# resubmit failed tasks with escalated resources.
# Use this for large batch workflows with many tasks.

# Count indices in an array spec (e.g., "0-99" = 100, "1,5,10" = 3)
count_array_indices() {
    local spec=$1
    local count=0

    # Split by comma
    IFS=',' read -ra parts <<< "$spec"
    for part in "${parts[@]}"; do
        if [[ "$part" =~ ^([0-9]+)-([0-9]+)$ ]]; then
            # Range: start-end
            local start="${BASH_REMATCH[1]}"
            local end="${BASH_REMATCH[2]}"
            count=$((count + end - start + 1))
        else
            # Single index
            count=$((count + 1))
        fi
    done

    echo "$count"
}

# Create checkpoint for array/handler chain mode
create_array_checkpoint() {
    local chain_id=$1
    local array_spec=$2
    local total_tasks=$3
    local script=$4
    shift 4
    local args=("$@")

    # Get initial mem/time from level 0
    local initial_mem="${LEVEL_0_MEM:-1G}"
    local initial_time="${LEVEL_0_TIME:-00:05:00}"

    # Use Python library to create array checkpoint (levels-based)
    python3 "$PYLIB" create-array-checkpoint "$CHECKPOINT_DIR" "$chain_id" "${PARTITION:-devel}" \
        "$array_spec" "$total_tasks" "$script" "$MAX_LEVEL" "$initial_mem" "$initial_time" "${args[@]}"
}

# Submit array job with escalation handler chain
submit_array_with_escalation() {
    local level=$1
    local array_spec=$2
    local chain_id=$3
    local checkpoint_file=$4

    # Get config from level
    local mem="${LEVEL_0_MEM:-1G}"
    local current_time="${LEVEL_0_TIME:-00:01:00}"
    [[ -n "$PARTITION" ]] || PARTITION="${LEVEL_0_PARTITION:-devel}"

    local task_count
    task_count=$(count_array_indices "$array_spec")

    echo "========================================"
    echo "Array Job with Level-Based Escalation"
    echo "========================================"
    echo "Chain ID:    $chain_id"
    echo "Script:      $SCRIPT ${SCRIPT_ARGS[*]}"
    echo "Array:       $array_spec ($task_count tasks)"
    echo ""

    # Display levels
    echo "Escalation Levels:"
    for i in $(seq 0 $MAX_LEVEL); do
        local pvar="LEVEL_${i}_PARTITION"
        local mvar="LEVEL_${i}_MEM"
        local tvar="LEVEL_${i}_TIME"
        if [[ -n "${!pvar}" ]] || [[ -n "${!mvar}" ]] || [[ -n "${!tvar}" ]]; then
            local marker=""
            [[ "$i" == "$level" ]] && marker=" <-- start"
            printf "  Level %d: partition=%-12s mem=%-6s time=%s%s\n" \
                "$i" "${!pvar:-?}" "${!mvar:-?}" "${!tvar:-?}" "$marker"
        fi
    done
    echo ""

    # Handler script path (already validated in validate_inputs)
    local handler_script="$SCRIPT_DIR/mem-escalate-handler.sh"

    # Create output directory for this chain
    local chain_output_dir="${OUTPUT_DIR}/${chain_id}"
    mkdir -p "$chain_output_dir"

    # Output/error path patterns (Slurm will expand %A and %a)
    local output_pattern="${chain_output_dir}/%A_%a.out"
    local error_pattern="${chain_output_dir}/%A_%a.err"
    echo "Outputs:     $chain_output_dir/"

    # Build export vars for the user's job
    local export_vars="ALL"
    export_vars+=",MEM_ESCALATE_LEVEL=$level"
    export_vars+=",MEM_ESCALATE_CHAIN_ID=$chain_id"
    # Add user-specified exports
    [[ -n "$EXPORT_VARS" ]] && export_vars+=",$EXPORT_VARS"

    # Compress array spec and check if batching needed
    local compressed_spec
    compressed_spec=$(python3 "$PYLIB" compress-indices "$array_spec")
    local spec_len=${#compressed_spec}
    # Use config value or default to 10000
    local max_spec_len="${MAX_ARRAY_SPEC_LEN:-10000}"

    echo ""
    if (( spec_len > max_spec_len )); then
        echo "Array spec too long ($spec_len chars), batching initial submission..."
    fi
    echo "Submitting main array job with --mem=$mem --time=$current_time --spread-job ..."

    local main_job=""
    local all_jobs=""

    if (( spec_len <= max_spec_len )); then
        # Single submission
        local array_opt="--array=$compressed_spec"
        [[ -n "$ARRAY_THROTTLE" ]] && array_opt="--array=${compressed_spec}%${ARRAY_THROTTLE}"

        local sbatch_output
        sbatch_output=$(sbatch --parsable \
            --partition="$PARTITION" \
            "$array_opt" \
            --mem="$mem" \
            --time="$current_time" \
            --output="$output_pattern" \
            --error="$error_pattern" \
            --spread-job \
            --export="$export_vars" \
            "$SCRIPT" "${SCRIPT_ARGS[@]}" 2>&1)

        main_job=$(echo "$sbatch_output" | tail -1)
        if [[ ! "$main_job" =~ ^[0-9]+$ ]]; then
            echo "ERROR: sbatch failed: $sbatch_output" >&2
            exit 1
        fi
        all_jobs="$main_job"
    else
        # Batch submission - split into chunks of 500 indices
        local -a indices_arr
        IFS=',' read -ra indices_arr <<< "$array_spec"
        local batch_size=500
        local total=${#indices_arr[@]}
        local batch_num=0

        for ((start=0; start<total; start+=batch_size)); do
            local end=$((start + batch_size))
            (( end > total )) && end=$total

            local batch_indices=""
            for ((j=start; j<end; j++)); do
                [[ -n "$batch_indices" ]] && batch_indices+=","
                batch_indices+="${indices_arr[$j]}"
            done

            local batch_compressed
            batch_compressed=$(python3 "$PYLIB" compress-indices "$batch_indices")
            local array_opt="--array=$batch_compressed"
            [[ -n "$ARRAY_THROTTLE" ]] && array_opt="--array=${batch_compressed}%${ARRAY_THROTTLE}"

            local sbatch_output job_id
            sbatch_output=$(sbatch --parsable \
                --partition="$PARTITION" \
                "$array_opt" \
                --mem="$mem" \
                --time="$current_time" \
                --output="$output_pattern" \
                --error="$error_pattern" \
                --spread-job \
                --export="$export_vars" \
                "$SCRIPT" "${SCRIPT_ARGS[@]}" 2>&1)

            job_id=$(echo "$sbatch_output" | tail -1)
            if [[ ! "$job_id" =~ ^[0-9]+$ ]]; then
                echo "ERROR: Batch $((batch_num+1)) failed: $sbatch_output" >&2
                exit 1
            fi

            ((batch_num++))
            echo "  Batch $batch_num: job $job_id"
            [[ -n "$all_jobs" ]] && all_jobs+=","
            all_jobs+="$job_id"
            [[ -z "$main_job" ]] && main_job="$job_id"
        done
        echo "Total: $batch_num batches"
    fi

    echo "Main job submitted: $all_jobs"

    # Build export vars for handler (levels-only)
    # Build export string (escape commas in partition values with |)
    local handler_export="PARENT_JOB=$all_jobs"
    handler_export+=",CURRENT_LEVEL=$level"
    handler_export+=",CHAIN_ID=$chain_id"
    handler_export+=",SCRIPT=$SCRIPT"
    handler_export+=",PARTITION=${PARTITION//,/|}"
    handler_export+=",MAX_LEVEL=$MAX_LEVEL"
    handler_export+=",CHECKPOINT_DIR=$CHECKPOINT_DIR"
    handler_export+=",HANDLER_SCRIPT=$handler_script"
    handler_export+=",PYLIB=$PYLIB"
    handler_export+=",LOGGING_ENABLED=$LOGGING_ENABLED"
    handler_export+=",DB_PATH=$DB_PATH"
    handler_export+=",OUTPUT_DIR=$OUTPUT_DIR"
    handler_export+=",OUTPUT_PATTERN=$output_pattern"
    handler_export+=",ERROR_PATTERN=$error_pattern"
    [[ -n "$ARRAY_THROTTLE" ]] && handler_export+=",ARRAY_THROTTLE=$ARRAY_THROTTLE"
    [[ -n "$EXPORT_VARS" ]] && handler_export+=",EXPORT_VARS=$EXPORT_VARS"
    [[ -n "$MAX_ARRAY_SPEC_LEN" ]] && handler_export+=",MAX_ARRAY_SPEC_LEN=$MAX_ARRAY_SPEC_LEN"

    # Pass all level configs to handler (partition commas escaped as |)
    for i in $(seq 0 $MAX_LEVEL); do
        local pvar="LEVEL_${i}_PARTITION"
        local mvar="LEVEL_${i}_MEM"
        local tvar="LEVEL_${i}_TIME"
        [[ -n "${!pvar}" ]] && handler_export+=",${pvar}=${!pvar//,/|}"
        [[ -n "${!mvar}" ]] && handler_export+=",${mvar}=${!mvar}"
        [[ -n "${!tvar}" ]] && handler_export+=",${tvar}=${!tvar}"
    done

    # Build dependency string - use afterany for batches, afternotok for single
    local job_count dep_str dep_type
    job_count=$(echo "$all_jobs" | tr ',' '\n' | wc -l)
    if (( job_count > 1 )); then
        dep_type="afterany"
        dep_str=""
        for jid in ${all_jobs//,/ }; do
            [[ -n "$dep_str" ]] && dep_str+=","
            dep_str+="afterany:$jid"
        done
    else
        dep_type="afternotok"
        dep_str="afternotok:$all_jobs"
    fi

    # Submit handler
    echo "Submitting handler with --dependency=$dep_type ..."
    local handler_job
    sbatch_output=$(sbatch --parsable \
        --partition="$PARTITION" \
        --dependency="$dep_str" \
        --export="$handler_export" \
        "$handler_script" 2>&1)

    # Extract job ID from last line (sbatch may print warnings before the ID)
    handler_job=$(echo "$sbatch_output" | tail -1)

    if [[ ! "$handler_job" =~ ^[0-9]+$ ]]; then
        echo "ERROR: Handler submission failed: $sbatch_output"
        echo "Cancelling main jobs $all_jobs..."
        scancel ${all_jobs//,/ } 2>/dev/null
        die "Cannot proceed without handler - escalation system requires it"
    fi
    echo "Handler job submitted: $handler_job (dependency: $dep_type)"

    # Submit success handler (afterok for single, afterany for batches)
    local success_script="${SCRIPT_DIR}/mem-escalate-success.sh"
    local success_export="PARENT_JOB=$all_jobs"
    success_export+=",CHAIN_ID=$chain_id"
    success_export+=",CHECKPOINT_DIR=$CHECKPOINT_DIR"
    success_export+=",PYLIB=$PYLIB"
    success_export+=",CURRENT_LEVEL=$level"
    success_export+=",ARRAY_SPEC=$array_spec"
    success_export+=",LOGGING_ENABLED=$LOGGING_ENABLED"
    success_export+=",LOGGING_DB_PATH=$LOGGING_DB_PATH"
    success_export+=",DB_PATH=$DB_PATH"
    success_export+=",HANDLER_JOB_ID=${handler_job:-0}"

    # Build success dependency (afterok for single, afterany for batches)
    local ok_dep_str ok_dep_type
    if (( job_count > 1 )); then
        ok_dep_type="afterany"
        ok_dep_str=""
        for jid in ${all_jobs//,/ }; do
            [[ -n "$ok_dep_str" ]] && ok_dep_str+=","
            ok_dep_str+="afterany:$jid"
        done
    else
        ok_dep_type="afterok"
        ok_dep_str="afterok:$all_jobs"
    fi

    echo "Submitting success handler with --dependency=$ok_dep_type ..."
    local success_job
    sbatch_output=$(sbatch --parsable \
        --partition="$PARTITION" \
        --job-name="escalate-success" \
        --output="${CHECKPOINT_DIR}/../success-handler-%j.out" \
        --time=00:05:00 \
        --mem=100M \
        --dependency="$ok_dep_str" \
        --export="$success_export" \
        "$success_script" 2>&1)

    # Extract job ID from last line (sbatch may print warnings before the ID)
    success_job=$(echo "$sbatch_output" | tail -1)

    if [[ ! "$success_job" =~ ^[0-9]+$ ]]; then
        echo "ERROR: Success handler submission failed: $sbatch_output"
        echo "Cancelling main jobs $all_jobs and handler $handler_job..."
        scancel ${all_jobs//,/ } "$handler_job" 2>/dev/null
        die "Cannot proceed without success handler"
    fi
    echo "Success handler submitted: $success_job (runs when all tasks complete)"

    # Update checkpoint with job info
    local handler_id=${handler_job}
    python3 "$PYLIB" update-array-round "$checkpoint_file" "$main_job" "$handler_id" "$array_spec" "$level" "$mem"

    # Dual-write to SQLite database
    if [[ "$LOGGING_ENABLED" == "true" ]]; then
        # Create chain in database
        python3 "$PYLIB" db-create-chain "$DB_PATH" "$chain_id" "handler_chain" "$PARTITION" "$SCRIPT" \
            --script-args "${SCRIPT_ARGS[@]}" \
            --array-spec "$array_spec" \
            --total-tasks "$task_count" \
            --memory "$mem" \
            --time "$current_time"

        # Add first round to database
        python3 "$PYLIB" db-add-round "$DB_PATH" "$chain_id" "$main_job" "$handler_id" "$array_spec" \
            "$level" "$mem" \
            --time-level 0 \
            --time "$current_time" \
            --partition "$PARTITION" \
            --output-pattern "$output_pattern" \
            --error-pattern "$error_pattern"
    fi

    # Log event
    log_event "SUBMIT_ARRAY job=$main_job handler=$handler_job array=$array_spec mem=$mem level=$level chain=$chain_id"

    echo ""
    echo "========================================"
    echo "Escalation Chain Submitted"
    echo "========================================"
    echo "Chain ID:       $chain_id"
    echo "Main Job:       $main_job"
    echo "Handler Job:    $handler_job"
    echo ""
    echo "The chain will automatically escalate memory if tasks fail with OOM."
    echo ""
    echo "Monitor progress:"
    echo "  sacct -j $main_job --format=JobID,State,MaxRSS"
    echo "  ./mem-escalate.sh --status $chain_id"
    echo ""
    echo "Handler logs: /data/tracker/handler-*.log"
}

# Show status of an escalation chain
show_chain_status() {
    local chain_id=$1
    local checkpoint_file="${CHECKPOINT_DIR}/${chain_id}.checkpoint"

    if [[ ! -f "$checkpoint_file" ]]; then
        echo "ERROR: Checkpoint not found: $checkpoint_file" >&2
        return 1
    fi

    python3 "$PYLIB" show-status "$checkpoint_file"

    # Show live queue status
    echo ""
    echo "Live Queue:"
    echo "--------------------------------------------------"
    local queue_output
    queue_output=$(squeue -u "$(whoami)" -o "%.8i %.10P %.12j %.4t %.8M %.4D %R" 2>/dev/null)
    if [[ -n "$queue_output" ]]; then
        echo "$queue_output"
    else
        echo "  (no jobs in queue)"
    fi
    echo ""
}

# Monitor chain with live updates
monitor_chain() {
    local chain_id=$1
    local interval=${2:-5}
    local checkpoint_file="${CHECKPOINT_DIR}/${chain_id}.checkpoint"

    if [[ ! -f "$checkpoint_file" ]]; then
        echo "ERROR: Checkpoint not found: $checkpoint_file" >&2
        return 1
    fi

    echo "Monitoring chain: $chain_id (refresh: ${interval}s, Ctrl+C to stop)"
    echo ""

    while true; do
        clear
        echo "========================================"
        echo "Escalation Monitor: $chain_id"
        echo "Time: $(date '+%Y-%m-%d %H:%M:%S')"
        echo "========================================"
        echo ""

        # Show chain status
        python3 "$PYLIB" show-status "$checkpoint_file"

        # Show live queue
        echo ""
        echo "Live Queue:"
        echo "--------------------------------------------------"
        local queue_output
        queue_output=$(squeue -u "$(whoami)" -o "%.8i %.10P %.12j %.4t %.8M %.4D %R" 2>/dev/null)
        if [[ -n "$queue_output" ]]; then
            echo "$queue_output"
        else
            echo "  (no jobs in queue - chain may be complete)"
        fi

        # Check if chain is done
        local status
        status=$(python3 "$PYLIB" get-chain-state "$checkpoint_file" 2>/dev/null)
        if [[ "$status" == "COMPLETED" ]] || [[ "$status" == "FAILED"* ]]; then
            echo ""
            echo "========================================"
            echo "Chain finished with status: $status"
            echo "========================================"
            break
        fi

        echo ""
        echo "Press Ctrl+C to stop monitoring..."
        sleep "$interval"
    done
}

# ============================================================
# MAIN
# ============================================================
main() {
    # Validate inputs
    validate_inputs

    # Load configuration
    load_config

    # Initialize tracker directories
    init_tracker

    # Handle --list-checkpoints
    if $LIST_CHECKPOINTS; then
        list_checkpoints
        exit 0
    fi

    # Handle --status
    if [[ -n "$STATUS_CHAIN" ]]; then
        if [[ -n "$WATCH_INTERVAL" ]]; then
            # Watch mode - use monitor function
            monitor_chain "$STATUS_CHAIN" "$WATCH_INTERVAL"
        else
            show_chain_status "$STATUS_CHAIN"
        fi
        exit $?
    fi

    # Validate script argument
    if [[ -z "$SCRIPT" ]]; then
        usage >&2
        die "No script specified"
    fi

    # Start at level 0 (use config levels for escalation)
    local level=0

    # Generate chain ID
    local chain_id
    chain_id=$(generate_chain_id)

    # Check if array mode
    if [[ -n "$ARRAY_SPEC" ]]; then
        # Array job with handler chain
        local total_tasks
        total_tasks=$(count_array_indices "$ARRAY_SPEC")
        local checkpoint_file
        checkpoint_file=$(create_array_checkpoint "$chain_id" "$ARRAY_SPEC" "$total_tasks" "$SCRIPT" "${SCRIPT_ARGS[@]}")

        submit_array_with_escalation "$level" "$ARRAY_SPEC" "$chain_id" "$checkpoint_file"
        exit $?
    else
        # Single job mode: use --array=0-0 for single jobs
        die "Single job mode not supported. Use --array=0-0 for single jobs with escalation."
    fi
}

main
