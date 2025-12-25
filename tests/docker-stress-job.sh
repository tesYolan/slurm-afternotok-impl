#!/bin/bash
#SBATCH --job-name=stress-test
#SBATCH --output=/data/tracker/outputs/%x-%A_%a.out
#SBATCH --error=/data/tracker/outputs/%x-%A_%a.err

# Docker Stress Test Job - Aggressive Memory Testing
# ===================================================
# This test FORCES memory consumption to trigger OOM kills
#
# Partition limits (Docker):
#   devel: 1GB (hard limit, no swap)
#   day:   2GB (hard limit, no swap)
#   week:  8GB (hard limit, no swap)
#   pi_jetz: 16GB (hard limit, no swap)
#
# Test behaviors by task ID (mod 10):
#   0: Success - Low memory (100MB)
#   1: OOM - 1.2GB allocation (exceeds 1GB limit)
#   2: OOM - 1.5GB allocation (exceeds 1GB limit)
#   3: Timeout - 90s runtime (exceeds 1min limit)
#   4: OOM - 2.5GB allocation (exceeds 2GB limit at L1)
#   5: Success - Medium memory (500MB)
#   6: OOM - 3GB allocation (needs week partition)
#   7: Timeout - 150s runtime (needs week 4min limit)
#   8: OOM - 10GB allocation (needs pi_jetz)
#   9: Success - Low memory (200MB)
#
# Expected escalation flow for --array=0-99:
#   L0 (1GB/1min):  Tasks 0,5,9 succeed | 1,2,4,6,8 OOM | 3,7 timeout
#   L1 (2GB/2min):  Tasks 1,2,3 succeed | 4,6,8 OOM | 7 timeout
#   L2 (4GB/4min):  Tasks 4,7 succeed | 6,8 OOM
#   L3 (8GB/8min):  Tasks 6 succeed | 8 OOM
#   L4 (16GB/15min): Task 8 succeeds
#
# Usage:
#   ./mem-escalate.sh --config tests/docker-escalation.yaml --array=0-99 tests/docker-stress-job.sh

set -e

echo "========================================"
echo "STRESS TEST - Aggressive Memory Testing"
echo "========================================"
echo "Task ID:      $SLURM_ARRAY_TASK_ID"
echo "Memory Limit: ${SLURM_MEM_PER_NODE}MB"
echo "Partition:    $SLURM_JOB_PARTITION"
echo "Escalation:   Level ${MEM_ESCALATE_LEVEL:-0}"
echo "Hostname:     $(hostname)"
echo "Start Time:   $(date '+%Y-%m-%d %H:%M:%S')"
echo "========================================"
echo ""

# Determine behavior based on task ID
task_mod=$((SLURM_ARRAY_TASK_ID % 10))

# Function to aggressively allocate memory
# This FORCES the kernel to actually use the memory (not lazy allocation)
allocate_memory() {
    local size_mb=$1
    local duration=${2:-10}

    echo ">>> Allocating ${size_mb}MB for ${duration}s (AGGRESSIVE MODE)"
    echo ">>> Method: Python with forced page faults"

    python3 << EOF
import sys
import time

size_mb = $size_mb
duration = $duration

print(f"Allocating {size_mb}MB of memory...")
print("Phase 1: Allocating bytearray...")

# Allocate the memory
data = bytearray(size_mb * 1024 * 1024)

print("Phase 2: Writing to every page to force allocation...")
# Write to every 4KB page to force the kernel to actually allocate physical memory
page_size = 4096
for i in range(0, len(data), page_size):
    data[i] = 0xFF  # Write to force page fault
    if i % (100 * 1024 * 1024) == 0:
        print(f"  Written {i // (1024*1024)}MB...")

print(f"Phase 3: Memory allocated and touched! Holding for {duration}s...")
print(f"Actual size: {len(data) / (1024*1024):.1f}MB")

# Hold the memory
for i in range(duration):
    # Keep touching memory to prevent swapping
    if i % 2 == 0:
        for j in range(0, len(data), page_size * 100):
            data[j] = (data[j] + 1) % 256
    time.sleep(1)
    if (i + 1) % 5 == 0:
        print(f"  Holding... {i+1}/{duration}s")

print("Memory test completed successfully!")
EOF
}

# Execute behavior based on task mod
case $task_mod in
    0)
        echo "Behavior: SUCCESS - Low memory (100MB)"
        echo "Expected: Completes at all levels"
        allocate_memory 100 5
        ;;

    1)
        echo "Behavior: OOM - 1.2GB allocation"
        echo "Expected: OOM at L0 (1GB), completes at L1 (2GB)"
        allocate_memory 1200 10
        ;;

    2)
        echo "Behavior: OOM - 1.5GB allocation"
        echo "Expected: OOM at L0 (1GB), completes at L1 (2GB)"
        allocate_memory 1500 10
        ;;

    3)
        echo "Behavior: TIMEOUT - 90s runtime"
        echo "Expected: Timeout at L0 (1min), completes at L1 (2min)"
        allocate_memory 100 90
        ;;

    4)
        echo "Behavior: OOM - 2.5GB allocation"
        echo "Expected: OOM at L0 (1GB) and L1 (2GB), completes at L2 (4GB)"
        allocate_memory 2500 10
        ;;

    5)
        echo "Behavior: SUCCESS - Medium memory (500MB)"
        echo "Expected: Completes at all levels"
        allocate_memory 500 5
        ;;

    6)
        echo "Behavior: OOM - 3GB allocation"
        echo "Expected: OOM at L0-L2, completes at L3 (8GB)"
        allocate_memory 3000 10
        ;;

    7)
        echo "Behavior: TIMEOUT - 150s runtime"
        echo "Expected: Timeout at L0-L1, completes at L2 (4min)"
        allocate_memory 100 150
        ;;

    8)
        echo "Behavior: OOM - 6GB allocation"
        echo "Expected: OOM at L0-L2 (4GB), completes at L3 (8GB)"
        allocate_memory 6000 10
        ;;

    9)
        echo "Behavior: SUCCESS - Low memory (200MB)"
        echo "Expected: Completes at all levels"
        allocate_memory 200 5
        ;;
esac

echo ""
echo "========================================"
echo "End Time:   $(date '+%Y-%m-%d %H:%M:%S')"
echo "Status:     COMPLETED SUCCESSFULLY"
echo "========================================"
