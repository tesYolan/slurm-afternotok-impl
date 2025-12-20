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
# Behavior by task ID (mod 10):
#   0-4: Quick completion (5s) - completes at level 0 (1m)
#   5-6: Medium duration (75s) - timeouts at level 0, completes at level 1 (2m)
#   7:   Slow duration (150s) - timeouts at level 0-1, completes at level 2 (4m)
#   8:   Memory hog - OOMs at 1G, needs 2G
#   9:   Very slow (300s) - timeouts at level 0-2, completes at level 3 (8m)
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

# Determine behavior based on task ID
task_mod=$((SLURM_ARRAY_TASK_ID % 10))

case $task_mod in
    0|1|2|3|4)
        echo "Behavior: QUICK (5s sleep)"
        echo "Expected: Completes at level 0 (1m limit)"
        sleep 5
        ;;
    5|6)
        echo "Behavior: MEDIUM (75s sleep)"
        echo "Expected: Timeout at level 0 (1m), completes at level 1 (2m)"
        sleep 75
        ;;
    7)
        echo "Behavior: SLOW (150s sleep)"
        echo "Expected: Timeout at level 0-1, completes at level 2 (4m)"
        sleep 150
        ;;
    8)
        echo "Behavior: MEMORY HOG"
        echo "Expected: OOM at 1G, needs 2G"
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
    9)
        echo "Behavior: VERY SLOW (300s sleep)"
        echo "Expected: Timeout at level 0-2, completes at level 3 (8m)"
        sleep 300
        ;;
esac

echo ""
echo "========================================"
echo "End Time: $(date -Iseconds)"
echo "Status: COMPLETED"
echo "========================================"
