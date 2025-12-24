#!/bin/bash
# Test Scenarios for Escalation System
# =====================================
# Submit various job types to test the memory/time escalation system.
#
# Uses escalation-target.yaml with levels-based config:
#   Level 0: devel,day  / 1G  / 1min
#   Level 1: week       / 4G  / 2min
#   Level 2: week       / 8G  / 4min
#   Level 3: pi_jetz    / 16G / 8min
#
# Usage:
#   ./test-scenarios.sh <scenario>
#
# Scenarios:
#   levels      - 20 tasks testing all 4 escalation levels (recommended)
#   quick       - 100 quick jobs (all complete at level 0)
#   timeout     - 100 jobs that will timeout and need escalation
#   oom         - 100 jobs that will OOM and need memory escalation
#   mixed       - 1000 jobs with mixed behavior
#   mixed-fail  - 30 jobs: some complete, some escalate, some code errors
#   large       - 5000 jobs stress test
#   max         - 10000 jobs (Slurm MaxArraySize limit test)
#   all         - Run quick, timeout, oom, levels scenarios

set -e

SCRIPT_DIR="$(dirname "$(readlink -f "$0")")"
ESCALATE_SCRIPT="$SCRIPT_DIR/../mem-escalate.sh"
VARIABLE_JOB="$SCRIPT_DIR/variable-job.sh"
CONFIG_FILE="$SCRIPT_DIR/../escalation-target.yaml"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

print_header() {
    echo ""
    echo -e "${GREEN}========================================"
    echo "$1"
    echo -e "========================================${NC}"
    echo ""
}

print_info() {
    echo -e "${YELLOW}[INFO]${NC} $1"
}

# Store the last chain ID for visualizer URL
LAST_CHAIN_ID=""

# Run escalation script and capture chain ID
run_escalation() {
    local output
    local exit_code

    # Run script and capture both output and exit code
    set +e  # Temporarily disable exit on error
    output=$("$ESCALATE_SCRIPT" --config "$CONFIG_FILE" "$@" 2>&1)
    exit_code=$?
    set -e

    # Always print the output
    echo "$output"

    # Check for failure
    if [[ $exit_code -ne 0 ]]; then
        echo ""
        echo -e "${RED}========================================${NC}"
        echo -e "${RED}ERROR: Job submission failed (exit code $exit_code)${NC}"
        echo -e "${RED}========================================${NC}"

        # Check common issues
        if echo "$output" | grep -q "Invalid job array specification"; then
            echo -e "${RED}CAUSE: MaxArraySize is too small for this array size.${NC}"
            echo -e "${YELLOW}FIX: Run these commands:${NC}"
            echo "  docker cp config/25.05/slurm.conf slurmctld:/etc/slurm/slurm.conf"
            echo "  docker exec slurmctld scontrol reconfigure"
        fi
        return $exit_code
    fi

    # Extract chain ID from output (looks for "Chain ID: XXXXX")
    LAST_CHAIN_ID=$(echo "$output" | grep -oP "Chain ID: \K[^\s]+" | head -1)

    if [[ -n "$LAST_CHAIN_ID" ]]; then
        echo ""
        print_info "Jobs submitted! Monitor with:"
        echo "  squeue"
        echo "  sacct -j <job_id>"
        echo ""
        print_info "View in visualizer:"
        echo -e "  (on host) python3 examples/jobs/visualizer-server.py --data-dir ./data"
        echo -e "  Open ${GREEN}http://localhost:8080${NC}"
    fi
}

# Ensure we're inside Docker
check_docker() {
    if [[ ! -f /etc/slurm/slurm.conf ]]; then
        echo -e "${RED}Error: This script must be run inside the Slurm Docker container.${NC}"
        echo "Run: docker exec -it slurmctld bash"
        exit 1
    fi
}

# Scenario: Levels test (tests all 4 escalation levels)
scenario_levels() {
    print_header "Scenario: Levels Test (20 tasks)"
    print_info "Tests all 4 escalation levels with partition switching"
    print_info ""
    print_info "Task behavior (by ID mod 10):"
    print_info "  0-4: Quick (5s)    → completes at Level 0 (devel,day/1G/1min)"
    print_info "  5-6: Medium (75s)  → Level 0 timeout → Level 1 (week/4G/2min)"
    print_info "  7:   Slow (150s)   → Level 0-1 timeout → Level 2 (week/8G/4min)"
    print_info "  8:   OOM (1.5GB)   → Level 0 OOM → Level 1 (week/4G/2min)"
    print_info "  9:   Very slow (5min) → Level 0-2 timeout → Level 3 (pi_jetz/16G/8min)"
    echo ""
    print_info "Expected duration: ~10-12 minutes (waiting for very slow tasks)"
    echo ""

    run_escalation --array=0-19 --throttle=10 "$VARIABLE_JOB"
}

# Scenario: Mixed with code errors (some complete, some escalate, some fail)
scenario_mixed_fail() {
    print_header "Scenario: Mixed with Failures (30 tasks)"
    print_info "Tests escalation + non-retryable code errors"
    print_info ""
    print_info "Task behavior:"
    print_info "  0-9:   Quick (5s)    → completes at Level 0"
    print_info "  10-14: CODE ERROR    → exit 1, NOT retried (reported failed)"
    print_info "  15-19: OOM (1.5GB)   → Level 0 OOM → escalates to Level 1"
    print_info "  20-24: Quick (5s)    → completes at Level 0"
    print_info "  25-29: TIMEOUT (75s) → Level 0 timeout → escalates to Level 1"
    echo ""
    print_info "Expected: 20 complete, 5 code errors (not retried), 10 escalated"
    echo ""

    run_escalation --array=0-29 --throttle=10 --export="FAIL_TASKS=10:11:12:13:14" "$VARIABLE_JOB"
}

# Scenario: Quick jobs (all complete fast)
scenario_quick() {
    print_header "Scenario: Quick Jobs (100 tasks)"
    print_info "All jobs sleep for 5s, should complete within 30s limit"
    print_info "Expected: All complete on first try, no escalation"
    echo ""

    # Create a simple quick job
    cat > /data/quick-job.sh << 'EOF'
#!/bin/bash
#SBATCH --job-name=quick-test
#SBATCH --output=/data/%x-%A_%a.out

echo "Quick job task $SLURM_ARRAY_TASK_ID starting at $(date)"
sleep 5
echo "Quick job task $SLURM_ARRAY_TASK_ID completed at $(date)"
EOF
    chmod +x /data/quick-job.sh

    run_escalation --array=0-99 --throttle=20 /data/quick-job.sh
}

# Scenario: Timeout jobs (need time escalation)
scenario_timeout() {
    print_header "Scenario: Timeout Jobs (100 tasks)"
    print_info "All jobs sleep for 45s, need 1m limit"
    print_info "Expected: Timeout at 30s, escalate to 1m, then complete"
    echo ""

    cat > /data/timeout-job.sh << 'EOF'
#!/bin/bash
#SBATCH --job-name=timeout-test
#SBATCH --output=/data/%x-%A_%a.out

echo "Timeout test task $SLURM_ARRAY_TASK_ID starting at $(date)"
echo "Will sleep for 45s (needs 1m limit)"
sleep 45
echo "Timeout test task $SLURM_ARRAY_TASK_ID completed at $(date)"
EOF
    chmod +x /data/timeout-job.sh

    run_escalation --array=0-99 --throttle=20 /data/timeout-job.sh
}

# Scenario: OOM jobs (need memory escalation)
scenario_oom() {
    print_header "Scenario: OOM Jobs (100 tasks)"
    print_info "All jobs allocate ~1.5GB, need 2G memory"
    print_info "Expected: OOM at 1G, escalate to 2G, then complete"
    echo ""

    cat > /data/oom-job.sh << 'EOF'
#!/bin/bash
#SBATCH --job-name=oom-test
#SBATCH --output=/data/%x-%A_%a.out

echo "OOM test task $SLURM_ARRAY_TASK_ID starting at $(date)"
echo "Will allocate ~1.5GB of memory"

python3 -c "
import time
print('Allocating 1.5GB...')
data = 'x' * (1536 * 1024 * 1024)
print(f'Allocated {len(data)} bytes')
time.sleep(30)
print('Done')
"

echo "OOM test task $SLURM_ARRAY_TASK_ID completed at $(date)"
EOF
    chmod +x /data/oom-job.sh

    run_escalation --array=0-99 --throttle=20 /data/oom-job.sh
}

# Scenario: Mixed jobs (variable behavior)
scenario_mixed() {
    print_header "Scenario: Mixed Jobs (1000 tasks)"
    print_info "Tasks have variable behavior based on task ID:"
    print_info "  0-4: Quick (5s) - complete in 30s"
    print_info "  5-6: Medium (15s) - need 1m"
    print_info "  7:   Slow (30s) - need 2m"
    print_info "  8:   Memory hog - need 2G"
    print_info "  9:   Very slow (45s) - need 4m"
    print_info "Expected: Multiple escalation rounds for time and memory"
    echo ""

    run_escalation --array=0-999 --throttle=50 "$VARIABLE_JOB"
}

# Scenario: Large scale test
scenario_large() {
    print_header "Scenario: Large Scale (5000 tasks)"
    print_info "Same as mixed but with 5000 tasks"
    print_info "Using throttle of 100 concurrent tasks"
    echo ""

    run_escalation --array=0-4999 --throttle=100 "$VARIABLE_JOB"
}

# Scenario: Maximum scale test (Slurm MaxArraySize limit)
scenario_max() {
    print_header "Scenario: Maximum Scale (10000 tasks)"
    print_info "Testing Slurm MaxArraySize limit (10000)"
    print_info "Same as mixed but with 10000 tasks"
    print_info "Using throttle of 100 concurrent tasks"
    echo ""

    run_escalation --array=0-9999 --throttle=100 "$VARIABLE_JOB"
}

# Scenario: Maximum scale with code errors
scenario_max_fail() {
    print_header "Scenario: Maximum Scale + Code Errors (10000 tasks)"
    print_info "10000 tasks with FAIL_MOD=17 (tasks divisible by 17 fail)"
    print_info "Expected: 588 tasks fail with exit 1 (NOT retried)"
    print_info "Using throttle of 100 concurrent tasks"
    echo ""

    run_escalation --array=0-9999 --throttle=100 --export="FAIL_MOD=17" "$VARIABLE_JOB"
}

# Scenario: Argument Passing
scenario_args() {
    print_header "Scenario: Argument Passing"
    print_info "Testing handling of spaces and quotes in arguments (Security Fix Validation)"
    
    # Create a job that prints its arguments
    cat > /data/args-job.sh << 'EOF'
#!/bin/bash
#SBATCH --job-name=args-test
#SBATCH --output=/data/%x-%j.out

echo "Argument 1: >$1<"
echo "Argument 2: >$2<"
echo "Argument 3: >$3<"
EOF
    chmod +x /data/args-job.sh

    # Use explicit partition to test -p flag
    # Arguments: "Spaces" "Quotes" "Simple"
    run_escalation -p normal --array=0-0 /data/args-job.sh "Arg with spaces" "Arg'with'quotes" "Simple"
}

# Show usage
usage() {
    echo "Usage: $0 <scenario>"
    echo ""
    echo "Escalation Levels (from escalation-target.yaml):"
    echo "  Level 0: devel,day  / 1G  / 1min"
    echo "  Level 1: week       / 4G  / 2min"
    echo "  Level 2: week       / 8G  / 4min"
    echo "  Level 3: pi_jetz    / 16G / 8min"
    echo ""
    echo "Scenarios:"
    echo "  levels      - 20 tasks testing all 4 levels (RECOMMENDED)"
    echo "  quick       - 100 quick jobs (all complete at level 0)"
    echo "  timeout     - 100 jobs that timeout and escalate time"
    echo "  oom         - 100 jobs that OOM and escalate memory"
    echo "  mixed       - 1000 jobs with mixed behavior"
    echo "  mixed-fail  - 30 jobs with code errors (not retried)"
    echo "  args        - Test argument passing with spaces/quotes"
    echo "  large       - 5000 jobs stress test"
    echo "  max         - 10000 jobs (Slurm MaxArraySize limit)"
    echo "  max-fail    - 10000 jobs with FAIL_MOD=17 (588 code errors)"
    echo "  all         - Run levels, quick, timeout, oom scenarios"
    echo ""
    echo "Examples:"
    echo "  $0 levels   # Best for testing all escalation levels"
    echo "  $0 quick    # Fast sanity check"
    echo "  $0 all      # Run comprehensive tests"
}

# Main
main() {
    check_docker

    case "${1:-}" in
        levels)
            scenario_levels
            ;;
        quick)
            scenario_quick
            ;;
        timeout)
            scenario_timeout
            ;;
        oom)
            scenario_oom
            ;;
        mixed)
            scenario_mixed
            ;;
        mixed-fail)
            scenario_mixed_fail
            ;;
        args)
            scenario_args
            ;;
        large)
            scenario_large
            ;;
        max)
            scenario_max
            ;;
        max-fail)
            scenario_max_fail
            ;;
        all)
            print_header "Running All Scenarios"
            print_info "This will run: levels, quick, timeout, oom"
            print_info "Estimated time: ~15-20 minutes"
            echo ""

            scenario_levels
            echo ""
            print_info "Levels test submitted. Waiting for completion..."
            print_info "Monitor: squeue -u root"
            echo ""
            read -p "Press Enter when queue is empty to continue..."

            scenario_quick
            echo ""
            read -p "Press Enter to continue to timeout scenario..."

            scenario_timeout
            echo ""
            read -p "Press Enter to continue to OOM scenario..."

            scenario_oom
            ;;
        *)
            usage
            exit 1
            ;;
    esac
}

main "$@"
