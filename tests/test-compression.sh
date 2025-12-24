#!/bin/bash
# Test cases for compress_indices function

PYLIB="/data/jobs/mem-escalate-lib.py"

test_compress() {
    local name="$1"
    local input="$2"
    local expected_max_len="${3:-0}"

    result=$(python3 "$PYLIB" compress-indices "$input")
    len=${#result}
    input_count=$(echo "$input" | tr ',' '\n' | wc -l)

    echo "=== $name ==="
    echo "Input: $input_count indices"
    echo "Output ($len chars): ${result:0:100}..."
    if [[ $expected_max_len -gt 0 ]] && [[ $len -gt $expected_max_len ]]; then
        echo "FAIL: Expected max $expected_max_len chars, got $len"
    else
        echo "OK"
    fi
    echo ""
}

echo "============================================"
echo "Compression Tests"
echo "============================================"
echo ""

# Test 1: Perfect stride (should compress to ~15 chars)
test_compress "Perfect stride 10" \
    "8,18,28,38,48,58,68,78,88,98,108,118,128,138,148" \
    20

# Test 2: Stride with 1 gap (should still use stride)
test_compress "Stride with 1 gap" \
    "8,18,28,38,48,58,78,88,98,108,118,128,138,148" \
    50

# Test 3: Multiple interleaved strides (perfect)
test_compress "Interleaved 5,6,7,8,9 perfect" \
    "5,6,7,8,9,15,16,17,18,19,25,26,27,28,29,35,36,37,38,39,45,46,47,48,49" \
    60

# Test 4: Interleaved with gaps (the real scenario)
test_compress "Interleaved with gaps" \
    "5,6,7,8,9,15,16,18,19,25,26,27,28,29,35,36,37,38,39,45,46,47,49" \
    100

# Test 5: Large interleaved with many gaps
indices=""
for i in $(seq 5 10 1000); do indices+="$i,"; done
for i in $(seq 6 10 1000); do indices+="$i,"; done
for i in $(seq 7 10 1000); do indices+="$i,"; done
for i in $(seq 8 10 1000); do indices+="$i,"; done
for i in $(seq 9 10 1000); do
    # Skip every 10th to simulate gaps
    if (( i % 100 != 9 )); then
        indices+="$i,"
    fi
done
indices="${indices%,}"
test_compress "Large interleaved (~500 indices)" "$indices" 200

# Test 6: Real failing indices from checkpoint (first 200)
echo "=== Real checkpoint data (sample) ==="
CHECKPOINT_SAMPLE="8,18,28,38,48,58,78,88,98,108,118,128,138,148,158,168,178,188,198,208,218,228,248,258,268,278,288,298,308,318,328,338,348,358,368,378,388,398,418,428,438,448,458,468,478,488,498"
result=$(python3 "$PYLIB" compress-indices "$CHECKPOINT_SAMPLE")
input_count=$(echo "$CHECKPOINT_SAMPLE" | tr ',' '\n' | wc -l)
echo "Input: $input_count indices (mod 8 only, with gaps)"
echo "Output (${#result} chars): $result"
echo ""

# Test 7: What the ideal output should be
echo "=== Ideal output comparison ==="
echo "Current output for mod-8 sample: $result"
echo "Ideal output: 8-58:10,78-228:10,248-398:10,418-498:10"
echo "Ideal length: $(echo -n '8-58:10,78-228:10,248-398:10,418-498:10' | wc -c)"
echo ""

# Test 8: Full checkpoint indices (from container if available)
if [[ -f /data/tracker/checkpoints/20251223-101203-0c6o.checkpoint ]]; then
    echo "=== Full checkpoint compression test ==="
    FULL_INDICES=$(grep "pending_indices:" /data/tracker/checkpoints/20251223-101203-0c6o.checkpoint | sed "s/.*pending_indices: //")
    full_count=$(echo "$FULL_INDICES" | tr ',' '\n' | wc -l)
    result=$(python3 "$PYLIB" compress-indices "$FULL_INDICES")
    echo "Input: $full_count indices"
    echo "Output length: ${#result} chars"
    echo "Target: <10000 chars for single submission"
    if [[ ${#result} -lt 10000 ]]; then
        echo "PASS: Can use single job submission!"
    else
        echo "FAIL: Still needs batching (${#result} > 10000)"
    fi
fi
