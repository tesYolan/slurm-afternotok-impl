#!/bin/bash
# Short test: afterany batching with escalation
# ==============================================
# Forces batching by using large sparse indices that produce long array specs.
# Quick test: ~100 tasks, 1 min timeout, ~20% fail to trigger escalation.
#
# Usage: ./test-afterany-short.sh [--submit]

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
JOBS_DIR="$(dirname "$SCRIPT_DIR")"
PYLIB="$JOBS_DIR/mem-escalate-lib.py"

echo "============================================"
echo "Short Test: afterany Batching + Escalation"
echo "============================================"
echo ""

# Generate indices 0-9999 with irregular gaps to prevent stride compression
# Need enough indices that compressed form exceeds 10K chars
generate_uncompressible_indices() {
    local count="$1"
    local indices=""
    local added=0

    for idx in $(seq 0 9999); do
        # Skip indices that would create stride patterns
        # Use prime-based filter to create irregular gaps
        if (( (idx * 7 + 3) % 11 != 0 )) && (( (idx * 13 + 5) % 17 != 0 )); then
            [[ -n "$indices" ]] && indices+=","
            indices+="$idx"
            ((added++))
            (( added >= count )) && break
        fi
    done
    echo "$indices"
}

echo "Step 1: Generate uncompressible indices (0-9999 range)..."
INDICES=$(generate_uncompressible_indices 6000)
INDEX_COUNT=$(echo "$INDICES" | tr ',' '\n' | wc -l)
echo "  Generated $INDEX_COUNT indices"
echo "  Sample: $(echo "$INDICES" | cut -d',' -f1-5)..."
echo ""

echo "Step 2: Test compression..."
COMPRESSED=$(python3 "$PYLIB" compress-indices "$INDICES")
COMPRESSED_LEN=${#COMPRESSED}
echo "  Compressed length: $COMPRESSED_LEN chars"

if (( COMPRESSED_LEN > 10000 )); then
    echo "  ✓ Exceeds 10K - will use batching + afterany"
    BATCH_COUNT=$(( (INDEX_COUNT + 499) / 500 ))
    echo "  Expected batches: $BATCH_COUNT"
else
    echo "  ✗ Under 10K - adjusting..."
    # Add more indices if needed
    INDICES=$(generate_sparse_indices 2000)
    COMPRESSED=$(python3 "$PYLIB" compress-indices "$INDICES")
    echo "  Retry with 2000 indices: ${#COMPRESSED} chars"
fi
echo ""

if [[ "$1" == "--submit" ]]; then
    echo "Step 3: Submitting test..."

    # Use the existing variable-job.sh which has built-in failure modes
    cd "$JOBS_DIR"

    # Submit with the sparse indices
    # Export FAIL_MOD to make ~20% fail immediately (not escalated)
    OUTPUT=$(./mem-escalate.sh \
        --config escalation-target.yaml \
        --array="$INDICES" \
        --export="FAIL_MOD=5" \
        tests/variable-job.sh 2>&1)

    CHAIN_ID=$(echo "$OUTPUT" | grep "Chain ID:" | awk '{print $3}')

    echo "$OUTPUT" | tail -20
    echo ""
    echo "Chain ID: $CHAIN_ID"
    echo ""
    echo "Monitor:"
    echo "  ./mem-escalate.sh --status $CHAIN_ID"
    echo "  ./mem-escalate.sh --status $CHAIN_ID --watch"
    echo ""
    echo "Verify afterany is used:"
    echo "  Look for 'afterany' in the Failure/Success Handler lines"
else
    echo "Step 3: Dry run complete"
    echo ""
    echo "Run with --submit to execute:"
    echo "  ./test-afterany-short.sh --submit"
fi

echo ""
echo "============================================"
