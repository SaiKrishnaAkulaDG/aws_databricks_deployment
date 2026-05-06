#!/bin/bash
# verification/REGRESSION_SUITE.sh
# Regression Suite — Portable verification across all Sections 10.1-10.5
# Assembles all critical checks from Sessions 1-7
# Run from repo root: bash verification/REGRESSION_SUITE.sh

set -uo pipefail
cd "$(dirname "$0")/.."

TOTAL_PASSED=0
TOTAL_FAILED=0

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║        REGRESSION SUITE — Sections 10.1–10.5 Verification      ║"
echo "║              All Critical Paths Across Sessions 1–7            ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

# Helper function to run a verification script and capture results
run_section() {
    local script=$1
    local section=$2

    echo "Running: $section..."
    RESULT=$(bash "$script" 2>&1)
    EXIT_CODE=$?

    # Extract summary (handle multiple formats)
    # Format 1: "SUMMARY: X passed, Y failed"
    # Format 2: "Checks passed: X" and "Checks failed: Y"
    # Format 3: "PASSED: X" and "FAILED: Y"

    SUMMARY_1=$(echo "$RESULT" | grep "SUMMARY:" | head -1)
    SUMMARY_2=$(echo "$RESULT" | grep "PASSED:" | grep "^PASSED:" | head -1)

    if [ -n "$SUMMARY_1" ]; then
        # Format 1: SUMMARY: X passed, Y failed
        PASSED=$(echo "$SUMMARY_1" | grep -oP '\d+(?= passed)' | head -1)
        FAILED=$(echo "$SUMMARY_1" | grep -oP '\d+(?= failed)' | head -1)
        DISPLAY="$SUMMARY_1"
    elif [ -n "$SUMMARY_2" ]; then
        # Format 3: PASSED: X and FAILED: Y
        PASSED=$(echo "$SUMMARY_2" | grep -oP '\d+$' | head -1)
        FAILED=$(echo "$RESULT" | grep "^FAILED:" | grep -oP '\d+$' | head -1)
        DISPLAY="PASSED: $PASSED, FAILED: $FAILED"
    else
        # Format 2: Checks passed: X / Checks failed: Y
        PASSED=$(echo "$RESULT" | grep "Checks passed:" | grep -oP '\d+' | head -1)
        FAILED=$(echo "$RESULT" | grep "Checks failed:" | grep -oP '\d+' | head -1)
        DISPLAY="Checks passed: $PASSED, Checks failed: $FAILED"
    fi

    if [ $EXIT_CODE -eq 0 ]; then
        echo "  ✅ PASS: $DISPLAY"
    else
        echo "  ❌ FAIL: $DISPLAY"
    fi

    # Add to totals (handle empty counts)
    PASSED=${PASSED:-0}
    FAILED=${FAILED:-0}
    TOTAL_PASSED=$((TOTAL_PASSED + PASSED))
    TOTAL_FAILED=$((TOTAL_FAILED + FAILED))
}

echo "=== Section 10.1: Bronze Layer Completeness ==="
run_section "verification/verify_bronze.sh" "verify_bronze.sh"
echo ""

echo "=== Section 10.2: Silver Transactions Quality ==="
run_section "verification/verify_silver_transactions.sh" "verify_silver_transactions.sh"
echo ""

echo "=== Section 10.2a: Silver Accounts Quality ==="
run_section "verification/verify_silver_accounts.sh" "verify_silver_accounts.sh"
echo ""

echo "=== Section 10.2b: Silver Layer Integration ==="
run_section "verification/verify_silver_integration.sh" "verify_silver_integration.sh"
echo ""

echo "=== Section 10.3: Gold Layer Correctness ==="
run_section "verification/verify_gold.sh" "verify_gold.sh"
echo ""

echo "=== Section 10.4: Idempotency Verification ==="
run_section "verification/verify_idempotency.sh" "verify_idempotency.sh"
echo ""

echo "=== Section 10.5: Audit Trail Verification ==="
run_section "verification/verify_audit_trail.sh" "verify_audit_trail.sh"
echo ""

# Final summary
echo "╔════════════════════════════════════════════════════════════════╗"
echo "║                      REGRESSION SUITE SUMMARY                  ║"
echo "╠════════════════════════════════════════════════════════════════╣"
echo "║                                                                ║"
echo "║  Total Checks Run:    $(printf '%3d' $((TOTAL_PASSED + TOTAL_FAILED)))"
echo "║  Total Passed:        $(printf '%3d' $TOTAL_PASSED)"
echo "║  Total Failed:        $(printf '%3d' $TOTAL_FAILED)"
echo "║                                                                ║"

if [ $TOTAL_FAILED -eq 0 ]; then
    echo "║  ✅ STATUS: ALL REGRESSION TESTS PASSED                        ║"
    echo "║                                                                ║"
    echo "╚════════════════════════════════════════════════════════════════╝"
    exit 0
else
    echo "║  ❌ STATUS: SOME REGRESSION TESTS FAILED                       ║"
    echo "║                                                                ║"
    echo "╚════════════════════════════════════════════════════════════════╝"
    exit 1
fi
