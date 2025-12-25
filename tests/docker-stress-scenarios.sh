#!/bin/bash
# Docker Stress Test Scenarios
# =============================
# Aggressive memory/time testing for Mac Docker cluster
#
# These tests FORCE actual memory consumption to trigger OOM kills
# by writing to every memory page and preventing swap.
#
# Cluster Configuration:
#   devel:   10 nodes @ 1GB  (hard limit, no swap) | 1min timeout
#   day:      5 nodes @ 2GB  (hard limit, no swap) | 2min timeout
#   week:     2 nodes @ 8GB  (hard limit, no swap) | 4min timeout
#   pi_jetz:  1 node  @ 16GB (hard limit, no swap) | 8min timeout
#
# Usage:
#   docker exec slurmctld bash -c 'cd /data/jobs/tests && ./docker-stress-scenarios.sh <scenario>'
#
# Scenarios:
#   quick      - 10 tasks, fast completion (1-2 min)
#   oom-basic  - 20 tasks, basic OOM testing (3-5 min)
#   oom-heavy  - 50 tasks, heavy OOM stress (5-10 min)
#   timeout    - 20 tasks, timeout escalation (5-10 min)
#   full       - 100 tasks, all behaviors (10-20 min)
#   extreme    - 200 tasks, maximum stress (20-40 min)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STRESS_JOB="$SCRIPT_DIR/docker-stress-job.sh"
ESCALATE_SCRIPT="/data/jobs/mem-escalate.sh"
CONFIG_FILE="/data/jobs/tests/docker-escalation.yaml"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

print_header() {
    echo ""
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN}$1${NC}"
    echo -e "${GREEN}========================================${NC}"
    echo ""
}

print_info() {
    echo -e "${YELLOW}[INFO]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if running inside Docker
check_docker() {
    if [[ ! -f /etc/slurm/slurm.conf ]]; then
        print_error "This script must run inside the Docker container"
        echo "Run: docker exec slurmctld bash -c 'cd /data/jobs/tests && ./docker-stress-scenarios.sh <scenario>'"
        exit 1
    fi
}

# Verify cluster is ready
check_cluster() {
    print_info "Verifying cluster status..."

    if ! sinfo &>/dev/null; then
        print_error "Slurm controller not responding"
        exit 1
    fi

    local idle_nodes=$(sinfo -h -o "%D" -t idle)
    local total_nodes=$(sinfo -h -o "%D")

    echo "Cluster status:"
    sinfo
    echo ""

    if [[ $idle_nodes -lt 10 ]]; then
        print_error "Not enough idle nodes (found $idle_nodes, need at least 10)"
        echo "Wait for jobs to complete or run: scancel --user=root"
        exit 1
    fi

    print_info "Cluster ready: $idle_nodes/$total_nodes nodes idle"
}

# Run escalation with output capture
run_test() {
    local array_spec="$1"
    local throttle="${2:-10}"

    print_info "Submitting jobs: --array=$array_spec --throttle=$throttle"
    echo "Command: $ESCALATE_SCRIPT --config $CONFIG_FILE --array=$array_spec --throttle=$throttle $STRESS_JOB"
    echo ""

    local output
    if output=$("$ESCALATE_SCRIPT" --config "$CONFIG_FILE" --array="$array_spec" --throttle="$throttle" "$STRESS_JOB" 2>&1); then
        echo "$output"

        # Extract chain ID
        local chain_id=$(echo "$output" | grep -oP "Chain ID: \K[^\s]+" | head -1)

        if [[ -n "$chain_id" ]]; then
            echo ""
            print_info "Jobs submitted successfully!"
            print_info "Chain ID: $chain_id"
            echo ""
            echo "Monitor with:"
            echo "  squeue                              # View job queue"
            echo "  /data/jobs/mem-escalate.sh --status $chain_id       # View escalation status"
            echo "  /data/jobs/mem-escalate.sh --status $chain_id --watch  # Watch live"
            echo ""
        fi
    else
        print_error "Job submission failed"
        echo "$output"
        return 1
    fi
}

# Scenario: Quick test (10 tasks, mostly success)
scenario_quick() {
    print_header "Quick Test - 10 Tasks"
    print_info "Fast sanity check with minimal stress"
    print_info ""
    print_info "Task breakdown (by ID mod 10):"
    print_info "  Task 0: Success (100MB) - completes at L0"
    print_info "  Task 1: OOM (1.2GB) - escalates to L1"
    print_info "  Task 2: OOM (1.5GB) - escalates to L1"
    print_info "  Task 3: Timeout (90s) - escalates to L1"
    print_info "  Task 4: OOM (2.5GB) - escalates to L2"
    print_info "  Task 5: Success (500MB) - completes at L0"
    print_info "  Task 6: OOM (3GB) - escalates to L3"
    print_info "  Task 7: Timeout (150s) - escalates to L2"
    print_info "  Task 8: OOM (10GB) - escalates to L4"
    print_info "  Task 9: Success (200MB) - completes at L0"
    print_info ""
    print_info "Expected: 3 immediate success, 7 escalations"
    print_info "Duration: ~1-2 minutes"
    echo ""

    run_test "0-9" 10
}

# Scenario: Basic OOM (20 tasks)
scenario_oom_basic() {
    print_header "Basic OOM Test - 20 Tasks"
    print_info "Focus on memory exhaustion at different levels"
    print_info ""
    print_info "20 tasks = 2x each behavior pattern"
    print_info "Expected:"
    print_info "  6 tasks succeed at L0"
    print_info "  6 tasks OOM at L0 → escalate"
    print_info "  2 tasks timeout at L0 → escalate"
    print_info "Duration: ~3-5 minutes"
    echo ""

    run_test "0-19" 10
}

# Scenario: Heavy OOM (50 tasks)
scenario_oom_heavy() {
    print_header "Heavy OOM Test - 50 Tasks"
    print_info "Aggressive memory stress with 50 concurrent tasks"
    print_info ""
    print_info "50 tasks = 5x each behavior pattern"
    print_info "Tests sustained memory pressure on all partitions"
    print_info ""
    print_info "Expected:"
    print_info "  15 tasks succeed at L0 (IDs ending in 0,5,9)"
    print_info "  15 tasks OOM at L0 → escalate (IDs ending in 1,2,4)"
    print_info "  10 tasks OOM multiple times (IDs ending in 6,8)"
    print_info "  5 tasks timeout at L0 (IDs ending in 3)"
    print_info "  5 tasks timeout multiple times (IDs ending in 7)"
    print_info "Duration: ~5-10 minutes"
    echo ""

    run_test "0-49" 15
}

# Scenario: Timeout focus
scenario_timeout() {
    print_header "Timeout Test - 20 Tasks"
    print_info "Focus on time-based escalation"
    print_info ""
    print_info "Mix of quick, medium, and slow tasks"
    print_info "Expected:"
    print_info "  Tasks 3,13: Timeout at L0 (1min) → complete at L1 (2min)"
    print_info "  Tasks 7,17: Timeout at L0-L1 → complete at L2 (4min)"
    print_info "Duration: ~5-10 minutes (waiting for slow tasks)"
    echo ""

    run_test "0-19" 10
}

# Scenario: Full test (100 tasks)
scenario_full() {
    print_header "Full Stress Test - 100 Tasks"
    print_info "Comprehensive test with all behavior patterns"
    print_info ""
    print_info "100 tasks = 10x each behavior (mod 10)"
    print_info ""
    print_info "Distribution:"
    print_info "  30 tasks: Quick success (100-500MB)"
    print_info "  30 tasks: OOM escalation (1.2-2.5GB)"
    print_info "  20 tasks: Timeout escalation (90-150s)"
    print_info "  20 tasks: Heavy OOM escalation (3-10GB)"
    print_info ""
    print_info "Tests all escalation levels:"
    print_info "  L0→L1: Most tasks (1GB→2GB, 1min→2min)"
    print_info "  L1→L2: Medium tasks (2GB→4GB, 2min→4min)"
    print_info "  L2→L3: Heavy tasks (4GB→8GB)"
    print_info "  L3→L4: Extreme tasks (8GB→16GB)"
    print_info ""
    print_info "Duration: ~10-20 minutes"
    print_info "Peak memory: ~30GB (multiple nodes)"
    echo ""

    run_test "0-99" 20
}

# Scenario: Extreme stress (200 tasks)
scenario_extreme() {
    print_header "Extreme Stress Test - 200 Tasks"
    print_info "⚠️  WARNING: Maximum stress test"
    print_info "⚠️  Requires all 18 nodes operational"
    print_info "⚠️  May take 20-40 minutes to complete"
    print_info ""
    print_info "200 tasks testing cluster limits:"
    print_info "  - Simultaneous OOM triggers"
    print_info "  - Queue backpressure"
    print_info "  - Escalation chain depth"
    print_info "  - Resource contention"
    print_info ""

    read -p "Continue with extreme test? (y/N) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        print_info "Cancelled"
        return 0
    fi

    run_test "0-199" 30
}

# Show usage
usage() {
    echo "Docker Stress Test Scenarios for Mac"
    echo ""
    echo "Usage: $0 <scenario>"
    echo ""
    echo "Scenarios:"
    echo "  quick      - 10 tasks, fast sanity check (1-2 min)"
    echo "  oom-basic  - 20 tasks, basic OOM testing (3-5 min)"
    echo "  oom-heavy  - 50 tasks, heavy OOM stress (5-10 min)"
    echo "  timeout    - 20 tasks, timeout escalation (5-10 min)"
    echo "  full       - 100 tasks, comprehensive test (10-20 min)"
    echo "  extreme    - 200 tasks, maximum stress (20-40 min)"
    echo ""
    echo "Examples:"
    echo "  $0 quick        # Quick sanity check"
    echo "  $0 oom-basic    # Test OOM triggers"
    echo "  $0 full         # Comprehensive test"
    echo ""
    echo "Run from host:"
    echo "  docker exec slurmctld bash -c 'cd /data/jobs/tests && ./docker-stress-scenarios.sh quick'"
}

# Main
main() {
    case "${1:-}" in
        quick)
            check_docker
            check_cluster
            scenario_quick
            ;;
        oom-basic)
            check_docker
            check_cluster
            scenario_oom_basic
            ;;
        oom-heavy)
            check_docker
            check_cluster
            scenario_oom_heavy
            ;;
        timeout)
            check_docker
            check_cluster
            scenario_timeout
            ;;
        full)
            check_docker
            check_cluster
            scenario_full
            ;;
        extreme)
            check_docker
            check_cluster
            scenario_extreme
            ;;
        *)
            usage
            exit 1
            ;;
    esac
}

main "$@"
