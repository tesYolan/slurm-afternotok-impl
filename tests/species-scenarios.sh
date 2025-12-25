#!/bin/bash
# Species Demographics Test Scenarios
# ====================================
# Realistic scientific workflow tests for Slurm escalation system
#
# These tests simulate population demographic modeling with species-specific
# resource requirements using both Python and R implementations.
#
# Cluster Configuration:
#   devel:   10 nodes @ 1GB  (hard limit, no swap) | 1min timeout
#   day:      5 nodes @ 2GB  (hard limit, no swap) | 2min timeout
#   week:     2 nodes @ 8GB  (hard limit, no swap) | 4min timeout
#   pi_jetz:  1 node  @ 16GB (hard limit, no swap) | 8min timeout
#
# Usage:
#   docker exec slurmctld bash -c 'cd /data/jobs/tests && ./species-scenarios.sh <scenario>'
#
# Scenarios:
#   quick-python    - 10 species, Python only (2-3 min)
#   quick-r         - 10 species, R only (2-3 min)
#   realistic       - 50 species, mixed Python+R (5-10 min)
#   stress          - 100 species, mixed Python+R (10-20 min)
#   lang-compare    - 20 species × 2 languages (performance comparison)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_JOB="$SCRIPT_DIR/species-demo-python.sh"
R_JOB="$SCRIPT_DIR/species-demo-r.sh"
ESCALATE_SCRIPT="/data/jobs/mem-escalate.sh"
CONFIG_FILE="/data/jobs/tests/docker-escalation.yaml"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
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

print_note() {
    echo -e "${BLUE}[NOTE]${NC} $1"
}

# Check if running inside Docker
check_docker() {
    if [[ ! -f /etc/slurm/slurm.conf ]]; then
        print_error "This script must run inside the Docker container"
        echo "Run: docker exec slurmctld bash -c 'cd /data/jobs/tests && ./species-scenarios.sh <scenario>'"
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

# Print species behavior table
print_species_table() {
    echo -e "${BLUE}Species Behavior by ID (mod 10):${NC}"
    echo "┌────────┬─────────────────┬──────────┬────────────┬─────────────────────────────────┐"
    echo "│ ID     │ Species         │ Memory   │ Time       │ Expected Outcome                │"
    echo "├────────┼─────────────────┼──────────┼────────────┼─────────────────────────────────┤"
    echo "│ 0,5,9  │ Small (bird)    │ ~100MB   │ 5s         │ Completes at L0 (1GB)           │"
    echo "│ 1,2    │ Medium (mammal) │ ~1.5GB   │ 10s        │ OOM at L0 → completes at L1     │"
    echo "│ 3,7    │ Slow (reptile)  │ ~100MB   │ 90s/150s   │ Timeout L0 → completes at L1/L2 │"
    echo "│ 4      │ Large (mammal)  │ ~3GB     │ 10s        │ OOM at L0-L1 → completes at L2  │"
    echo "│ 6      │ Very large      │ ~5GB     │ 10s        │ OOM at L0-L2 → completes at L3  │"
    echo "│ 8      │ Huge            │ ~7GB     │ 10s        │ OOM at L0-L2 → completes at L3  │"
    echo "└────────┴─────────────────┴──────────┴────────────┴─────────────────────────────────┘"
    echo ""
}

# Run escalation with output capture
run_test() {
    local job_script="$1"
    local array_spec="$2"
    local throttle="${3:-10}"
    local lang_label="${4:-}"

    print_info "Submitting ${lang_label}jobs: --array=$array_spec --throttle=$throttle"
    echo "Command: $ESCALATE_SCRIPT --config $CONFIG_FILE --array=$array_spec --throttle=$throttle $job_script"
    echo ""

    local output
    if output=$("$ESCALATE_SCRIPT" --config "$CONFIG_FILE" --array="$array_spec" --throttle="$throttle" "$job_script" 2>&1); then
        echo "$output"

        # Extract chain ID
        local chain_id=$(echo "$output" | grep -oP "Chain ID: \K[^\s]+" | head -1)

        if [[ -n "$chain_id" ]]; then
            echo ""
            print_info "Jobs submitted successfully! ${lang_label}"
            print_info "Chain ID: $chain_id"
            echo ""
            echo "Monitor with:"
            echo "  squeue                              # View job queue"
            echo "  /data/jobs/mem-escalate.sh --status $chain_id       # View escalation status"
            echo "  /data/jobs/mem-escalate.sh --status $chain_id --watch  # Watch live"
            echo ""

            return 0
        fi
    else
        print_error "Job submission failed ${lang_label}"
        echo "$output"
        return 1
    fi
}

# Scenario: Quick Python test (10 species)
scenario_quick_python() {
    print_header "Quick Python Test - 10 Species"
    print_info "Fast sanity check using Python demographic model"
    echo ""

    print_species_table

    print_info "Expected escalation flow (10 species):"
    print_info "  L0 (1G/1min):  Species 0,5,9 complete | 1,2,4,6,8 OOM | 3,7 timeout"
    print_info "  L1 (2G/2min):  Species 1,2,3,7 complete | 4,6,8 OOM"
    print_info "  L2 (4G/4min):  Species 4 completes | 6,8 OOM"
    print_info "  L3 (8G/8min):  Species 6,8 complete"
    print_info ""
    print_info "Duration: ~2-3 minutes"
    echo ""

    run_test "$PYTHON_JOB" "0-9" 10 "(Python) "
}

# Scenario: Quick R test (10 species)
scenario_quick_r() {
    print_header "Quick R Test - 10 Species"
    print_info "Fast sanity check using R demographic model"
    echo ""

    print_species_table

    print_info "Expected escalation flow (10 species):"
    print_info "  L0 (1G/1min):  Species 0,5,9 complete | 1,2,4,6,8 OOM | 3,7 timeout"
    print_info "  L1 (2G/2min):  Species 1,2,3,7 complete | 4,6,8 OOM"
    print_info "  L2 (4G/4min):  Species 4 completes | 6,8 OOM"
    print_info "  L3 (8G/8min):  Species 6,8 complete"
    print_info ""
    print_info "Duration: ~2-3 minutes"
    echo ""

    run_test "$R_JOB" "0-9" 10 "(R) "
}

# Scenario: Realistic test (50 species, mixed)
scenario_realistic() {
    print_header "Realistic Test - 50 Species (Mixed Python+R)"
    print_info "Moderate-scale test with both languages"
    echo ""

    print_species_table

    print_info "50 species = 5x each behavior pattern"
    print_info ""
    print_info "Distribution:"
    print_info "  15 species: Quick success (~100-200MB) at L0"
    print_info "  10 species: Medium (~1.5GB) escalate L0→L1"
    print_info "  10 species: Timeout escalate L0→L1 or L0→L2"
    print_info "   5 species: Large (~3GB) escalate L0→L1→L2"
    print_info "   5 species: Very large (~5GB) escalate L0→L1→L2→L3"
    print_info "   5 species: Huge (~7GB) escalate L0→L1→L2→L3"
    print_info ""
    print_info "Duration: ~5-10 minutes"
    echo ""

    # Submit both Python and R jobs
    print_info "Submitting 25 Python jobs..."
    run_test "$PYTHON_JOB" "0-24" 15 "(Python) " || return 1

    echo ""
    print_info "Submitting 25 R jobs..."
    run_test "$R_JOB" "25-49" 15 "(R) " || return 1
}

# Scenario: Stress test (100 species, mixed)
scenario_stress() {
    print_header "Stress Test - 100 Species (Mixed Python+R)"
    print_info "Comprehensive test with all behavior patterns"
    echo ""

    print_species_table

    print_info "100 species = 10x each behavior (mod 10)"
    print_info ""
    print_info "Distribution:"
    print_info "  30 species: Quick success (100-200MB)"
    print_info "  20 species: Medium (~1.5GB) escalation"
    print_info "  20 species: Timeout escalation"
    print_info "  10 species: Large (~3GB) escalation"
    print_info "  10 species: Very large (~5GB) escalation"
    print_info "  10 species: Huge (~7GB) escalation"
    print_info ""
    print_info "Tests all escalation levels:"
    print_info "  L0→L1: Most species (1GB→2GB, 1min→2min)"
    print_info "  L1→L2: Medium+ species (2GB→4GB, 2min→4min)"
    print_info "  L2→L3: Heavy species (4GB→8GB)"
    print_info ""
    print_info "Duration: ~10-20 minutes"
    print_info "Peak memory: ~30GB (distributed across nodes)"
    echo ""

    # Submit both Python and R jobs
    print_info "Submitting 50 Python jobs..."
    run_test "$PYTHON_JOB" "0-49" 20 "(Python) " || return 1

    echo ""
    print_info "Submitting 50 R jobs..."
    run_test "$R_JOB" "50-99" 20 "(R) " || return 1
}

# Scenario: Language comparison (20 species × 2 languages)
scenario_lang_compare() {
    print_header "Language Comparison - 20 Species × 2 Languages"
    print_info "Compare Python vs R performance on same workloads"
    echo ""

    print_species_table

    print_info "40 total jobs (20 Python + 20 R):"
    print_info "  - Same species configurations (0-19)"
    print_info "  - Identical computational workload"
    print_info "  - Compare memory usage patterns"
    print_info "  - Compare execution times"
    print_info ""
    print_info "Expected:"
    print_info "  Both languages should escalate identically"
    print_info "  Python may be slightly faster (NumPy)"
    print_info "  R may use slightly more memory (overhead)"
    print_info ""
    print_info "Duration: ~5-10 minutes"
    echo ""

    # Submit both Python and R jobs with same array range
    print_info "Submitting 20 Python jobs..."
    run_test "$PYTHON_JOB" "0-19" 10 "(Python) " || return 1

    echo ""
    print_info "Submitting 20 R jobs..."
    run_test "$R_JOB" "0-19" 10 "(R) " || return 1

    echo ""
    print_note "After completion, compare outputs in /data/tracker/outputs/"
    print_note "  species-py-*.out vs species-r-*.out"
}

# Show usage
usage() {
    echo "Species Demographics Test Scenarios"
    echo ""
    echo "Usage: $0 <scenario>"
    echo ""
    echo "Scenarios:"
    echo "  quick-python   - 10 species, Python only (2-3 min)"
    echo "  quick-r        - 10 species, R only (2-3 min)"
    echo "  realistic      - 50 species, mixed Python+R (5-10 min)"
    echo "  stress         - 100 species, mixed Python+R (10-20 min)"
    echo "  lang-compare   - 20 species × 2 languages (5-10 min)"
    echo ""
    echo "Examples:"
    echo "  $0 quick-python    # Quick Python sanity check"
    echo "  $0 realistic       # Moderate-scale mixed test"
    echo "  $0 lang-compare    # Compare Python vs R"
    echo ""
    echo "Run from host:"
    echo "  docker exec slurmctld bash -c 'cd /data/jobs/tests && ./species-scenarios.sh quick-python'"
}

# Main
main() {
    case "${1:-}" in
        quick-python)
            check_docker
            check_cluster
            scenario_quick_python
            ;;
        quick-r)
            check_docker
            check_cluster
            scenario_quick_r
            ;;
        realistic)
            check_docker
            check_cluster
            scenario_realistic
            ;;
        stress)
            check_docker
            check_cluster
            scenario_stress
            ;;
        lang-compare)
            check_docker
            check_cluster
            scenario_lang_compare
            ;;
        *)
            usage
            exit 1
            ;;
    esac
}

main "$@"
