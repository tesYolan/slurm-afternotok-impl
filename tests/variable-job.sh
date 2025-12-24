#!/bin/bash
#SBATCH --job-name=variable-test
#SBATCH --output=/data/%x-%A_%a.out

# Variable Test Job
# =================
# Each array task has different resource requirements based on task ID.
# This creates a mix of outcomes for testing the escalation system.
#
# NOTE: Slurm has 1-minute minimum time resolution, so sleep times are set
# to create predictable timeouts at each escalation level.
#
# Behavior by task ID (mod 10) - default mode:
#   0-4: Quick completion (5s) - completes at level 0 (1m)
#   5-6: Medium duration (75s) - timeouts at level 0, completes at level 1 (2m)
#   7:   Slow duration (150s) - timeouts at level 0-1, completes at level 2 (4m)
#   8:   Memory hog - OOMs at 1G, needs 2G
#   9:   Very slow (300s) - timeouts at level 0-2, completes at level 3 (8m)
#
# With FAIL_TASKS env var (colon-separated task IDs, e.g. "10:11:12"):
#   Listed tasks exit with code 1 (not retried, reported as failed)
#
# Usage:
#   sbatch --array=0-99 variable-job.sh      # 100 tasks with mixed behavior
#   sbatch --array=0-999 variable-job.sh     # 1000 tasks for stress test

echo "========================================"
echo "Variable Test Job"
echo "========================================"
echo "Job ID:       $SLURM_JOB_ID"
echo "Array Job ID: $SLURM_ARRAY_JOB_ID"
echo "Task ID:      $SLURM_ARRAY_TASK_ID"
echo "Memory:       $SLURM_MEM_PER_NODE"
echo "Time Limit:   $SLURM_TIMELIMIT"
echo "Hostname:     $(hostname)"
echo "Start Time:   $(date -Iseconds)"
echo "Escalation Info:"
echo "  Level:        ${MEM_ESCALATE_LEVEL:-0}"
echo "  Chain:        ${MEM_ESCALATE_CHAIN_ID:-none}"
echo "  Partition:    ${SLURM_JOB_PARTITION}"
echo ""

# Check if this task should fail with code error (not retried)
# FAIL_TASKS uses colons as delimiter (commas conflict with Slurm --export)
# FAIL_MOD=N makes tasks divisible by N fail (e.g., FAIL_MOD=17 fails 0,17,34,...)
if [[ -n "$FAIL_TASKS" ]] && echo ":$FAIL_TASKS:" | grep -q ":$SLURM_ARRAY_TASK_ID:"; then
    echo "Behavior: CODE ERROR (exit 1)"
    echo "Expected: Fails immediately, NOT retried (reported as failed)"
    echo ""
    echo "========================================"
    echo "End Time: $(date -Iseconds)"
    echo "Status: FAILED (exit code 1)"
    echo "========================================"
    exit 1
fi

if [[ -n "$FAIL_MOD" ]] && (( SLURM_ARRAY_TASK_ID % FAIL_MOD == 0 )); then
    echo "Behavior: CODE ERROR (divisible by $FAIL_MOD)"
    echo "Expected: Fails immediately, NOT retried (reported as failed)"
    echo ""
    echo "========================================"
    echo "End Time: $(date -Iseconds)"
    echo "Status: FAILED (exit code 1)"
    echo "========================================"
    exit 1
fi

# Determine behavior based on task ID
task_mod=$((SLURM_ARRAY_TASK_ID % 10))

# Rare tasks that need pi_jetz level (1 in 1000 each)
# Task ID ending in 999: needs 8 min TIME (timeout at L0-L2, complete L3)
if (( SLURM_ARRAY_TASK_ID % 1000 == 999 )); then
    echo "Behavior: VERY SLOW (250s) - needs pi_jetz for TIME"
    echo "Expected: Timeout at L0-L2 (4min max), completes at L3 (8min)"
    sleep 250
    echo ""
    echo "========================================"
    echo "End Time: $(date -Iseconds)"
    echo "Status: COMPLETED"
    echo "========================================"
    exit 0
fi

# Task ID ending in 997: needs 8G MEMORY (OOM at L0-L2, complete L3)
if (( SLURM_ARRAY_TASK_ID % 1000 == 997 )); then
    echo "Behavior: MEMORY MONSTER (6GB) - needs pi_jetz for MEMORY"
    echo "Expected: OOM at L0-L2 (4G max), completes at L3 (8G)"
    python3 -c "
import time
print('Allocating 6GB of memory...')
data = 'x' * (6 * 1024 * 1024 * 1024)  # 6GB
print(f'Allocated {len(data)} bytes')
time.sleep(5)
"
    echo ""
    echo "========================================"
    echo "End Time: $(date -Iseconds)"
    echo "Status: COMPLETED"
    echo "========================================"
    exit 0
fi

# Escalation test with 4 levels
# Level 0: 1G, 1min  |  Level 1: 2G, 2min  |  Level 2: 4G, 4min  |  Level 3: 8G, 8min
case $task_mod in
    0|1|2|3|4)
        echo "Behavior: QUICK (5s sleep)"
        echo "Expected: Completes at level 0 (1m limit)"
        sleep 5
        ;;
    5|6)
        echo "Behavior: MEDIUM (65s sleep)"
        echo "Expected: Timeout at level 0 (1m), completes at level 1 (2m)"
        sleep 65
        ;;
    7|9)
        echo "Behavior: SLOW (130s sleep)"
        echo "Expected: Timeout at level 0-1, completes at level 2 (4m)"
        sleep 130
        ;;
    8)
        echo "Behavior: MEMORY HOG"
        echo "Expected: OOM at 1G, needs 4G"
        # Allocate ~1.5GB of memory
        python3 -c "
import time
print('Allocating 1.5GB of memory...')
data = 'x' * (1536 * 1024 * 1024)  # 1.5GB string
print(f'Allocated {len(data)} bytes')
time.sleep(5)
print('Done')
"
        ;;
esac

echo ""
echo "========================================"
echo "End Time: $(date -Iseconds)"
echo "Status: COMPLETED"
echo "========================================"
