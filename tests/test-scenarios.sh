#!/bin/bash
# Test Scenarios for Escalation System
# =====================================
# Submit various job types to test the memory/time escalation system.
#
# Usage:
#   ./test-scenarios.sh <scenario>
#
# Scenarios (all use array jobs):
#   fast        - 500 quick test (under 5 min with all escalations)
#   quick       - 100 quick jobs (all complete within 30s)
#   timeout     - 100 jobs that will timeout and need escalation
#   oom         - 100 jobs that will OOM and need memory escalation
#   mixed       - 1000 jobs with mixed behavior
#   large       - 5000 jobs stress test
#   max         - 10000 jobs (Slurm MaxArraySize limit test)
#   all         - Run all scenarios (excluding large/max)

set -e

SCRIPT_DIR="$(dirname "$(readlink -f "$0")")"
ESCALATE_SCRIPT="$SCRIPT_DIR/../mem-escalate.sh"
VARIABLE_JOB="$SCRIPT_DIR/variable-job.sh"

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
    output=$("$ESCALATE_SCRIPT" "$@" 2>&1)
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

# Scenario: Fast test (under 5 min with escalations)
scenario_fast() {
    print_header "Scenario: Fast Test (500 tasks)"
    print_info "Quick escalation test - target under 5 min"
    print_info "Uses variable-job.sh with fast timings"
    echo ""

    run_escalation --array=0-499 --throttle=50 "$VARIABLE_JOB"
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

# Show usage
usage() {
    echo "Usage: $0 <scenario>"
    echo ""
    echo "Scenarios (all use array jobs):"
    echo "  fast        - 500 tasks quick test (under 5 min with escalations)"
    echo "  quick       - 100 quick jobs (all complete within 30s)"
    echo "  timeout     - 100 jobs that will timeout and need escalation"
    echo "  oom         - 100 jobs that will OOM and need memory escalation"
    echo "  mixed       - 1000 jobs with mixed behavior"
    echo "  large       - 5000 jobs stress test"
    echo "  max         - 10000 jobs (Slurm MaxArraySize limit test)"
    echo "  all         - Run all scenarios (excluding large/max)"
    echo ""
    echo "Examples:"
    echo "  $0 fast     # Quick test under 5 min"
    echo "  $0 mixed"
    echo "  $0 max"
}

# Main
main() {
    check_docker

    case "${1:-}" in
        fast)
            scenario_fast
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
        large)
            scenario_large
            ;;
        max)
            scenario_max
            ;;
        all)
            print_header "Running All Scenarios"
            scenario_quick
            echo ""
            read -p "Press Enter to continue to timeout scenario..."
            scenario_timeout
            echo ""
            read -p "Press Enter to continue to OOM scenario..."
            scenario_oom
            echo ""
            read -p "Press Enter to continue to mixed scenario..."
            scenario_mixed
            ;;
        *)
            usage
            exit 1
            ;;
    esac

    echo ""
    print_info "Jobs submitted! Monitor with:"
    echo "  squeue"
    echo "  sacct -j <job_id>"
    echo ""
    print_info "View in visualizer:"
    echo "  (on host) python3 examples/jobs/visualizer-server.py --data-dir ./data"
    echo "  Open http://localhost:8080"
}

main "$@"
