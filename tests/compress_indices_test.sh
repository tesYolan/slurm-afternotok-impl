#!/usr/bin/env bash
set -euo pipefail

# Test Python implementation of compression
# This replaces the old bash-based unit test
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYLIB="${ROOT_DIR}/mem-escalate-lib.py"

pass=0
fail=0

assert_eq() {
    local name="$1"
    local expected="$2"
    local input="$3"
    local actual
    actual=$(python3 "$PYLIB" compress-indices "$input")
    
    if [[ "$expected" == "$actual" ]]; then
        printf "✓ %s\n" "$name"
        ((++pass))
    else
        printf "✗ %s\n   input:    %s\n   expected: %s\n   got:      %s\n" "$name" "$input" "$expected" "$actual"
        ((++fail))
    fi
}

echo "Testing compress-indices..."

assert_eq "empty input" "" ""
assert_eq "single element" "42" "42"
assert_eq "two consecutive" "5-6" "5,6"
assert_eq "two non-consecutive" "5,10" "5,10"
assert_eq "consecutive range" "0-4" "0,1,2,3,4"
assert_eq "strided range" "8-38:10" "8,18,28,38"
# Interleaved pattern detection: gaps [1,9,1,9,1] -> period 2, stride 10
assert_eq "interleaved stride" "5-25:10,6-26:10" "5,6,15,16,25,26"
# Period 3 interleaved: 1,2,3,11,12,13,21,22,23 -> gaps [1,1,8,1,1,8,1,1]
assert_eq "interleaved period 3" "1-21:10,2-22:10,3-23:10" "1,2,3,11,12,13,21,22,23"
assert_eq "dedupe and sort" "1-3" "3,2,2,1"
assert_eq "mixed ranges" "1-3,10-14:2" "1,2,3,10,12,14"
# Large stride pattern (OOM failures on nodes)
assert_eq "large stride" "100-300:100" "100,200,300"
# Non-strided sparse (can't be compressed)
assert_eq "non-strided sparse" "5,17,42" "5,17,42"

echo ""
echo "Passed: $pass"
echo "Failed: $fail"

if (( fail > 0 )); then
    exit 1
fi