#!/bin/bash
# Test: afterany batching scenario
# ================================
# This test creates an array job with indices that don't compress well,
# forcing the system to batch and use afterany dependencies.
#
# The key is to create indices with NO stride pattern - random gaps that
# defeat the stride-grouping compression algorithm.
#
# Usage: ./test-afterany-batching.sh [--submit]
#   Without --submit: just shows what would happen (dry run)
#   With --submit: actually submits the job

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
JOBS_DIR="$(dirname "$SCRIPT_DIR")"
PYLIB="$JOBS_DIR/mem-escalate-lib.py"

echo "============================================"
echo "Test: afterany Batching Scenario"
echo "============================================"
echo ""

# Generate indices that won't compress well
# Strategy: Use prime-spaced indices to break stride patterns
generate_uncompressible_indices() {
    local count="$1"
    local indices=""
    local primes=(2 3 5 7 11 13 17 19 23 29 31 37 41 43 47)
    local idx=0
    local added=0

    while (( added < count )); do
        # Add index if it passes our "random" filter
        # Use modulo with different primes to create irregular gaps
        if (( (idx * 7 + 3) % 11 != 0 )) && (( (idx * 13 + 5) % 17 != 0 )); then
            [[ -n "$indices" ]] && indices+=","
            indices+="$idx"
            ((added++))
        fi
        ((idx++))
    done
    echo "$indices"
}

echo "Step 1: Generate indices that resist compression..."
# Need enough indices that even with compression, we exceed 10K chars
# With ~2 chars per index average after failed compression, need ~5000+ indices
INDICES=$(generate_uncompressible_indices 6000)
INDEX_COUNT=$(echo "$INDICES" | tr ',' '\n' | wc -l)
echo "  Generated $INDEX_COUNT indices"
echo "  First 20: $(echo "$INDICES" | cut -d',' -f1-20)"
echo ""

echo "Step 2: Test compression..."
COMPRESSED=$(python3 "$PYLIB" compress-indices "$INDICES")
COMPRESSED_LEN=${#COMPRESSED}
echo "  Compressed length: $COMPRESSED_LEN chars"
echo "  First 100 chars: ${COMPRESSED:0:100}..."
echo ""

if (( COMPRESSED_LEN > 10000 )); then
    echo "  ✓ Exceeds 10K limit - will trigger batching + afterany"
else
    echo "  ✗ Under 10K limit - would use single job + afternotok"
    echo "    (Increase index count or make pattern more random)"
fi
echo ""

echo "Step 3: Simulate batch splitting..."
# Count how many batches would be created (500 indices per batch)
BATCH_COUNT=$(( (INDEX_COUNT + 499) / 500 ))
echo "  Would create $BATCH_COUNT batches (500 indices each)"
echo "  Dependency type: afterany (for batch jobs)"
echo ""

echo "Step 4: Show expected dependency chain..."
echo "  Job submission:"
echo "    Batch 1: sbatch --array=<indices_1-500>"
echo "    Batch 2: sbatch --array=<indices_501-1000>"
echo "    ..."
echo "    Batch $BATCH_COUNT: sbatch --array=<indices_...>"
echo ""
echo "  Handler dependencies:"
echo "    Failure handler: --dependency=afterany:job1,afterany:job2,...,afterany:job$BATCH_COUNT"
echo "    Success handler: --dependency=afterany:job1,afterany:job2,...,afterany:job$BATCH_COUNT"
echo ""

if [[ "$1" == "--submit" ]]; then
    echo "Step 5: Submitting test job..."

    # Create a simple test script that completes quickly
    TEST_SCRIPT="/data/test-afterany-job.sh"
    cat > "$TEST_SCRIPT" << 'EOF'
#!/bin/bash
#SBATCH --job-name=afterany-test
#SBATCH --output=/data/afterany-test-%A_%a.out
#SBATCH --time=00:01:00
#SBATCH --mem=100M

echo "Task $SLURM_ARRAY_TASK_ID starting at $(date)"

# ~20% of tasks fail to trigger escalation
if (( SLURM_ARRAY_TASK_ID % 5 == 0 )); then
    echo "Simulating timeout..."
    sleep 70  # Will timeout with 1 min limit
fi

echo "Task $SLURM_ARRAY_TASK_ID completed at $(date)"
EOF
    chmod +x "$TEST_SCRIPT"

    # Submit via mem-escalate.sh
    cd "$JOBS_DIR"
    CHAIN_ID=$(./mem-escalate.sh --array="$INDICES" "$TEST_SCRIPT" 2>&1 | grep "Chain ID:" | awk '{print $3}')

    echo "  Chain ID: $CHAIN_ID"
    echo ""
    echo "  Monitor with:"
    echo "    ./mem-escalate.sh --status $CHAIN_ID"
    echo "    ./mem-escalate.sh --status $CHAIN_ID --watch"
else
    echo "Step 5: Dry run complete"
    echo ""
    echo "  To actually submit this test:"
    echo "    ./test-afterany-batching.sh --submit"
fi

echo ""
echo "============================================"
echo "Test complete"
echo "============================================"
