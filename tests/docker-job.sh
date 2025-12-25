#!/bin/bash
#SBATCH --job-name=docker-test
#SBATCH --output=/data/tracker/outputs/%x-%A_%a.out
#SBATCH --error=/data/tracker/outputs/%x-%A_%a.err

# Docker Test Job
# ===============
# Optimized for Docker/Mac quick testing.
# Partition time limits: devel=1min, day=2min, week=4min, pi_jetz=15min
#
# Behavior by task ID (mod 5):
#   0,1: Quick (5s)       - completes at level 0 (1G/1min)
#   2:   Medium (75s)     - timeout at L0 (1min), completes at L1 (2min)
#   3:   Memory (1.5GB)   - OOM at L0 (1G), completes at L1 (2G)
#   4:   Slow (150s)      - timeout at L0-L1, completes at L2 (4min)
#
# Expected escalation flow for 10 tasks (0-9):
#   Level 0: 0,1,5,6 complete | 2,7 timeout | 3,8 OOM | 4,9 timeout
#   Level 1: 2,7 complete | 3,8 complete | 4,9 timeout
#   Level 2: 4,9 complete
#
# Total test time: ~5-6 minutes
#
# Usage:
#   ./mem-escalate.sh --config tests/docker-escalation.yaml --array=0-9 tests/docker-job.sh

echo "========================================"
echo "Docker Test Job"
echo "========================================"
echo "Task ID:    $SLURM_ARRAY_TASK_ID"
echo "Memory:     ${SLURM_MEM_PER_NODE}MB"
echo "Partition:  $SLURM_JOB_PARTITION"
echo "Level:      ${MEM_ESCALATE_LEVEL:-0}"
echo "Hostname:   $(hostname)"
echo "Start:      $(date '+%H:%M:%S')"
echo ""

task_mod=$((SLURM_ARRAY_TASK_ID % 5))

case $task_mod in
    0|1)
        echo "Behavior: QUICK (5s)"
        echo "Expected: Completes at level 0"
        sleep 5
        ;;
    2)
        echo "Behavior: MEDIUM (75s)"
        echo "Expected: Timeout at L0 (1min), completes at L1 (2min)"
        sleep 75
        ;;
    3)
        echo "Behavior: MEMORY HOG (1.5GB)"
        echo "Expected: OOM at L0 (1G), completes at L1 (2G)"
        python3 -c "
import time
print('Allocating 1.5GB...')
data = bytearray(1536 * 1024 * 1024)
time.sleep(3)
print('Done')
"
        ;;
    4)
        echo "Behavior: SLOW (150s)"
        echo "Expected: Timeout at L0-L1, completes at L2 (4min)"
        sleep 150
        ;;
esac

echo ""
echo "========================================"
echo "End:    $(date '+%H:%M:%S')"
echo "Status: COMPLETED"
echo "========================================"
