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
    -a, --array <spec>        Job array specification (e.g., 0-999)
    -t, --throttle <N>        Max concurrent array tasks (default: unlimited)
    -c, --config <path>       Path to YAML config file
    -m, --start-mem <MEM>     Starting memory (e.g., 2G)
    -l, --max-level <N>       Maximum escalation level
    -s, --status <chain_id>   Show status of an escalation chain
    -w, --watch [SEC]         Watch mode - refresh status every SEC seconds (default: 5)
    -r, --resume <chain_id>   Resume from checkpoint
        --list                List all checkpoints
        --no-wait             Submit and exit without monitoring
    -h, --help                Show this help message
    -v, --version             Show version

EXAMPLES:
    # Single job with escalation
    ${0##*/} ./my-job.sh

    # Array job (recommended for batch workloads)
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
MEMORY_LADDER=(1G 2G 4G 8G 16G)
MAX_LEVEL=4
# NOTE: Slurm has 1-minute minimum resolution - these match escalation-config.yaml
TIME_LADDER=(00:01:00 00:02:00 00:04:00 00:08:00 00:15:00)
TIME_MAX_LEVEL=4
JOB_TIMEOUT="1:00:00"
POLL_INTERVAL=5
SACCT_DELAY=2
MAX_RETRIES=5
RETRY_DELAY=5
TRACKER_DIR="/data/tracker"
HISTORY_LOG="/data/tracker/history.log"
CHECKPOINT_DIR="/data/tracker/checkpoints"
DEFAULT_PARTITION="normal"
OUTPUT_PATTERN="/data/%x-%j.out"
LOGGING_ENABLED=false
LOGGING_DB_PATH=""

# ============================================================
# ARGUMENT PARSING
# ============================================================
CONFIG_FILE=""
PARTITION=""
START_MEM=""
NO_WAIT=false
RESUME_CHAIN=""
LIST_CHECKPOINTS=false
ARRAY_SPEC=""
ARRAY_THROTTLE=""
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
        -m|--start-mem)
            START_MEM="$2"
            shift 2
            ;;
        -l|--max-level)
            MAX_LEVEL="$2"
            shift 2
            ;;
        --no-wait)
            NO_WAIT=true
            shift
            ;;
        -r|--resume)
            RESUME_CHAIN="$2"
            shift 2
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
    local config_path="${CONFIG_FILE:-$SCRIPT_DIR/escalation-config.yaml}"

    if [[ ! -f "$config_path" ]]; then
        echo "INFO: No config file found at $config_path, using defaults" >&2
        return 0
    fi

    local partition_arg=""
    [[ -n "$PARTITION" ]] && partition_arg="--partition=$PARTITION"

    echo "INFO: Loading config from $config_path" >&2
    eval "$(python3 "$PYLIB" load-config "$config_path" $partition_arg)"
}

# ============================================================
# CHECKPOINT FUNCTIONS
# ============================================================
generate_chain_id() {
    # or UUID? later. 
    echo "$(date +%Y%m%d-%H%M%S)-$(head /dev/urandom | tr -dc 'a-z0-9' | head -c 4)"
}

create_checkpoint() {
    local chain_id=$1
    local script=$2
    shift 2
    local args=("$@")

    # Use Python library to create checkpoint
    python3 "$PYLIB" create-checkpoint "$CHECKPOINT_DIR" "$chain_id" "${PARTITION:-normal}" "$script" "${MEMORY_LADDER[*]}" "${TIME_LADDER[*]}" "${args[@]}"
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
# UTILITY FUNCTIONS
# ============================================================
get_memory_for_level() {
    local level=$1
    echo "${MEMORY_LADDER[$level]}"
}

find_level_for_memory() {
    local mem=$1
    for i in "${!MEMORY_LADDER[@]}"; do
        if [[ "${MEMORY_LADDER[$i]}" == "$mem" ]]; then
            echo "$i"
            return
        fi
    done
    echo "0"
}

# ============================================================
# SINGLE JOB ESCALATION (synchronous, polling-based)
# ============================================================
# For individual jobs: submits, waits, detects OOM, and retries
# with more memory in a loop until success or max level reached.
# Use this for simple single-job workflows.

run_escalation() {
    local level=$1
    local chain=$2
    local original_job=$3
    local chain_id=$4
    local checkpoint_file=$5

    local mem
    mem=$(get_memory_for_level "$level")

    echo "========================================"
    echo "Memory Escalation Wrapper"
    echo "========================================"
    echo "Chain ID:    $chain_id"
    echo "Script:      $SCRIPT ${SCRIPT_ARGS[*]}"
    echo "Level:       $level / $MAX_LEVEL"
    echo "Memory:      $mem"
    echo "Ladder:      ${MEMORY_LADDER[*]}"
    [[ -n "$chain" ]] && echo "Job Chain:   $chain"
    echo ""

    # Build export vars
    local export_vars="ALL"
    export_vars+=",MEM_ESCALATE_LEVEL=$level"
    export_vars+=",MEM_ESCALATE_CHAIN_ID=$chain_id"
    [[ -n "$chain" ]] && export_vars+=",MEM_ESCALATE_CHAIN=$chain"
    [[ -n "$original_job" ]] && export_vars+=",MEM_ESCALATE_ORIGINAL_JOB=$original_job"

    # Submit the job
    echo "Submitting job with --mem=$mem ..."
    local job_id
    job_id=$(sbatch --parsable --partition="$PARTITION" --mem="$mem" --time="$JOB_TIMEOUT" --export="$export_vars" "$SCRIPT" "${SCRIPT_ARGS[@]}" 2>&1)

    if [[ ! "$job_id" =~ ^[0-9]+$ ]]; then
        echo "ERROR: sbatch failed: $job_id" >&2
        python3 "$PYLIB" update-job-status "$checkpoint_file" 0 "SUBMIT_FAILED"
        exit 1
    fi

    echo "Job submitted: $job_id"

    # Update chain
    if [[ -z "$chain" ]]; then
        chain="$job_id"
        original_job="$job_id"
    else
        chain="${chain},${job_id}"
    fi

    # Update tracking
    log_event "SUBMIT job=$job_id mem=$mem level=$level chain=$chain_id script=$SCRIPT"
    python3 "$PYLIB" update-checkpoint "$checkpoint_file" "$job_id" "$level" "$mem" "RUNNING"

    if $NO_WAIT; then
        echo ""
        echo "Job submitted (no-wait mode)."
        echo "Chain ID: $chain_id"
        echo "To resume: ./mem-escalate.sh --resume $chain_id"
        echo "Check status: sacct -j $job_id --format=JobID,State,ExitCode,MaxRSS"
        exit 0
    fi

    # Monitor job
    echo "Monitoring job $job_id ..."
    while squeue -j "$job_id" -h 2>/dev/null | grep -q .; do
        sleep "$POLL_INTERVAL"
    done

    echo ""
    echo "Job $job_id completed. Checking status..."

    # Check outcome
    if check_job_success "$job_id"; then
        echo "SUCCESS: Job completed successfully at level $level ($mem)"
        python3 "$PYLIB" update-job-status "$checkpoint_file" "$job_id" "COMPLETED"
        log_event "COMPLETE job=$job_id mem=$mem level=$level chain=$chain_id"

        echo ""
        echo "Chain ID: $chain_id"
        echo "Job chain: $chain"
        echo "Final memory: $mem"
        sacct -j "$job_id" --format=JobID%15,State%15,ExitCode,MaxRSS%12,ReqMem%10 2>/dev/null
        return 0
    fi

    if detect_oom "$job_id"; then
        echo "OOM DETECTED: Job $job_id ran out of memory at $mem"
        python3 "$PYLIB" update-job-status "$checkpoint_file" "$job_id" "OOM"
        log_event "OOM job=$job_id mem=$mem level=$level chain=$chain_id"

        # Check if we can escalate
        local next_level=$((level + 1))

        if (( next_level > MAX_LEVEL )) || (( next_level >= ${#MEMORY_LADDER[@]} )); then
            echo ""
            echo "ERROR: Maximum memory level reached ($mem). Cannot escalate further."
            python3 "$PYLIB" update-job-status "$checkpoint_file" "$job_id" "FAILED_MAX_MEMORY"
            log_event "FAILED_MAX_MEMORY job=$job_id mem=$mem level=$level chain=$chain_id"
            echo ""
            echo "Chain ID: $chain_id"
            echo "Job chain: $chain"
            return 1
        fi

        local next_mem
        next_mem=$(get_memory_for_level "$next_level")
        echo "Escalating to level $next_level ($next_mem)..."
        log_event "ESCALATE job=$job_id mem=$mem -> $next_mem level=$level -> $next_level chain=$chain_id"

        sleep "$RETRY_DELAY"

        # Recursive call with updated state
        run_escalation "$next_level" "$chain" "$original_job" "$chain_id" "$checkpoint_file"
        return $?
    else
        # Non-OOM failure
        local state
        state=$(sacct -nX -j "$job_id" -o State --parsable2 2>/dev/null | head -1)
        echo "FAILED: Job failed with state: $state (not OOM)"
        python3 "$PYLIB" update-job-status "$checkpoint_file" "$job_id" "FAILED"
        log_event "FAILED job=$job_id mem=$mem level=$level state=$state chain=$chain_id"

        echo ""
        echo "Chain ID: $chain_id"
        echo "Job chain: $chain"
        return 1
    fi
}

# ============================================================
# ARRAY JOB ESCALATION (asynchronous, handler-based)
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

    # Use Python library to create array checkpoint
    python3 "$PYLIB" create-array-checkpoint "$CHECKPOINT_DIR" "$chain_id" "${PARTITION:-normal}" "$array_spec" "$total_tasks" "$script" "${MEMORY_LADDER[*]}" "${TIME_LADDER[*]}" "${args[@]}"
}

# Submit array job with escalation handler chain
submit_array_with_escalation() {
    local level=$1
    local array_spec=$2
    local chain_id=$3
    local checkpoint_file=$4

    local mem
    mem=$(get_memory_for_level "$level")
    local task_count
    task_count=$(count_array_indices "$array_spec")

    echo "========================================"
    echo "Array Job with Memory Escalation"
    echo "========================================"
    echo "Chain ID:    $chain_id"
    echo "Script:      $SCRIPT ${SCRIPT_ARGS[*]}"
    echo "Array:       $array_spec ($task_count tasks)"
    echo "Level:       $level / $MAX_LEVEL"
    echo "Memory:      $mem"
    echo "Ladder:      ${MEMORY_LADDER[*]}"
    echo ""

    # Handler script path (already validated in validate_inputs)
    local handler_script="$SCRIPT_DIR/mem-escalate-handler.sh"
    echo "Handler:     $handler_script"
    echo ""

    # Build array option
    local array_opt="--array=$array_spec"
    if [[ -n "$ARRAY_THROTTLE" ]]; then
        array_opt="--array=${array_spec}%${ARRAY_THROTTLE}"
        echo "Throttle:    $ARRAY_THROTTLE concurrent tasks"
    fi

    # Build export vars for the user's job
    local export_vars="ALL"
    export_vars+=",MEM_ESCALATE_LEVEL=$level"
    export_vars+=",MEM_ESCALATE_CHAIN_ID=$chain_id"

    # Get initial time from time ladder
    local current_time="${TIME_LADDER[0]}"

    # Submit main array job
    echo "Submitting main array job with --mem=$mem --time=$current_time --spread-job ..."
    local main_job
    main_job=$(sbatch --parsable \
        --partition="$PARTITION" \
        "$array_opt" \
        --mem="$mem" \
        --time="$current_time" \
        --spread-job \
        --export="$export_vars" \
        "$SCRIPT" "${SCRIPT_ARGS[@]}" 2>&1)

    if [[ ! "$main_job" =~ ^[0-9]+$ ]]; then
        echo "ERROR: sbatch failed: $main_job" >&2
        exit 1
    fi

    echo "Main job submitted: $main_job"

    # Build export vars for handler
    local handler_export="PARENT_JOB=$main_job"
    handler_export+=",CURRENT_LEVEL=$level"
    handler_export+=",CURRENT_TIME_LEVEL=0"
    handler_export+=",CURRENT_TIME=$current_time"
    handler_export+=",CHAIN_ID=$chain_id"
    handler_export+=",SCRIPT=$SCRIPT"
    # SCRIPT_ARGS is loaded from checkpoint by the handler
    handler_export+=",PARTITION=$PARTITION"
    handler_export+=",MAX_LEVEL=$MAX_LEVEL"
    handler_export+=",TIME_MAX_LEVEL=$TIME_MAX_LEVEL"
    handler_export+=",MEMORY_LADDER=${MEMORY_LADDER[*]}"
    handler_export+=",TIME_LADDER=${TIME_LADDER[*]}"
    handler_export+=",CHECKPOINT_DIR=$CHECKPOINT_DIR"
    handler_export+=",HANDLER_SCRIPT=$handler_script"
    handler_export+=",PYLIB=$PYLIB"
    handler_export+=",LOGGING_ENABLED=$LOGGING_ENABLED"
    handler_export+=",LOGGING_DB_PATH=$LOGGING_DB_PATH"
    [[ -n "$ARRAY_THROTTLE" ]] && handler_export+=",ARRAY_THROTTLE=$ARRAY_THROTTLE"

    # Submit handler with afternotok dependency
    echo "Submitting handler with --dependency=afternotok:$main_job ..."
    local handler_job
    handler_job=$(sbatch --parsable \
        --partition="$PARTITION" \
        --dependency=afternotok:$main_job \
        --export="$handler_export" \
        "$handler_script" 2>&1)

    if [[ ! "$handler_job" =~ ^[0-9]+$ ]]; then
        echo "ERROR: Handler submission failed: $handler_job"
        echo "Cancelling main job $main_job..."
        scancel "$main_job" 2>/dev/null
        die "Cannot proceed without handler - escalation system requires it"
    fi
    echo "Handler job submitted: $handler_job (runs only if main job fails)"

    # Submit success handler with afterok dependency (runs if all tasks complete)
    local success_script="${SCRIPT_DIR}/mem-escalate-success.sh"
    local success_export="PARENT_JOB=$main_job"
    success_export+=",CHAIN_ID=$chain_id"
    success_export+=",CHECKPOINT_DIR=$CHECKPOINT_DIR"
    success_export+=",PYLIB=$PYLIB"
    success_export+=",CURRENT_LEVEL=$level"
    success_export+=",ARRAY_SPEC=$array_spec"
    success_export+=",LOGGING_ENABLED=$LOGGING_ENABLED"
    success_export+=",LOGGING_DB_PATH=$LOGGING_DB_PATH"

    echo "Submitting success handler with --dependency=afterok:$main_job ..."
    local success_job
    success_job=$(sbatch --parsable \
        --partition="$PARTITION" \
        --job-name="escalate-success" \
        --output="${CHECKPOINT_DIR}/../success-handler-%j.out" \
        --time=00:05:00 \
        --mem=100M \
        --dependency=afterok:$main_job \
        --export="$success_export" \
        "$success_script" 2>&1)

    if [[ ! "$success_job" =~ ^[0-9]+$ ]]; then
        echo "ERROR: Success handler submission failed: $success_job"
        echo "Cancelling main job $main_job and handler $handler_job..."
        scancel "$main_job" "$handler_job" 2>/dev/null
        die "Cannot proceed without success handler"
    fi
    echo "Success handler submitted: $success_job (runs when all tasks complete)"

    # Update checkpoint with job info
    local handler_id=${handler_job}
    python3 "$PYLIB" update-array-round "$checkpoint_file" "$main_job" "$handler_id" "$array_spec" "$level" "$mem"

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
            # Watch mode - loop with clear
            while true; do
                clear
                echo "Watching chain: $STATUS_CHAIN (refresh: ${WATCH_INTERVAL}s, Ctrl+C to stop)"
                echo "Time: $(date '+%Y-%m-%d %H:%M:%S')"
                echo ""
                show_chain_status "$STATUS_CHAIN"
                sleep "$WATCH_INTERVAL"
            done
        else
            show_chain_status "$STATUS_CHAIN"
        fi
        exit $?
    fi

    # Handle --resume
    if [[ -n "$RESUME_CHAIN" ]]; then
        echo "Resuming from checkpoint: $RESUME_CHAIN"
        load_checkpoint "$RESUME_CHAIN"

        if [[ "$RESUME_STATUS" == "COMPLETED" ]]; then
            echo "Chain already completed successfully."
            exit 0
        fi

        if [[ "$RESUME_STATUS" == "FAILED_MAX_MEMORY" ]]; then
            echo "Chain failed at maximum memory. Cannot resume."
            exit 1
        fi

        # Resume from next level
        local next_level=$((RESUME_LEVEL + 1))
        if (( next_level > MAX_LEVEL )); then
            echo "Already at maximum level. Cannot resume."
            exit 1
        fi

        echo "Resuming at level $next_level..."
        run_escalation "$next_level" "$RESUME_CHAIN" "" "$RESUME_CHAIN" "$CHECKPOINT_FILE"
        exit $?
    fi

    # Validate script argument for new chains
    if [[ -z "$SCRIPT" ]]; then
        usage >&2
        die "No script specified"
    fi

    # Determine starting level
    local level=0
    if [[ -n "$START_MEM" ]]; then
        level=$(find_level_for_memory "$START_MEM")
    fi

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
        # Single job with polling escalation (existing behavior)
        local checkpoint_file
        checkpoint_file=$(create_checkpoint "$chain_id" "$SCRIPT" "${SCRIPT_ARGS[@]}")

        echo "Created checkpoint: $chain_id"

        # Run escalation
        run_escalation "$level" "" "" "$chain_id" "$checkpoint_file"
        exit $?
    fi
}

main
