#!/usr/bin/env bash
# verification/verify_gold.sh
# Section 10.3 — Gold layer correctness checks
# Run from repo root: bash verification/verify_gold.sh

set -uo pipefail
cd "$(dirname "$0")/.."

PASSED=0
FAILED=0

pass() { echo "  PASS: $1"; PASSED=$((PASSED + 1)); }
fail() { echo "  FAIL: $1"; FAILED=$((FAILED + 1)); }

echo "=== Gold Layer Verification — Section 10.3 ==="
echo ""

# CHECK 1: GOLD_DAILY_ONE_ROW_PER_DATE
# No duplicate transaction_date in daily summary — expect 0 duplicates.
echo "CHECK 1: GOLD_DAILY_ONE_ROW_PER_DATE"
RESULT=$(docker compose run --rm pipeline python3 - 2>/dev/null << 'PYEOF' || true
import duckdb
conn = duckdb.connect()
result = conn.execute("""
    SELECT COUNT(*) FROM (
        SELECT transaction_date, COUNT(*) c
        FROM read_parquet('/app/data/gold/daily_summary/data.parquet')
        GROUP BY transaction_date
        HAVING c > 1
    )
""").fetchone()[0]
print(result)
PYEOF
)
COUNT=$(echo "$RESULT" | tail -1)
if [ "$COUNT" = "0" ]; then
  pass "No duplicate transaction_date in Gold daily summary (INV-13)"
else
  fail "$COUNT duplicate transaction_date(s) in Gold daily summary"
fi
echo ""

# CHECK 2: GOLD_DAILY_AMOUNT_MATCHES_SILVER
# For each date: Gold total_signed_amount = SUM(_signed_amount) from Silver
# where _is_resolvable=true. Expect 0 mismatches.
echo "CHECK 2: GOLD_DAILY_AMOUNT_MATCHES_SILVER"
RESULT=$(docker compose run --rm pipeline python3 - 2>/dev/null << 'PYEOF' || true
import duckdb
conn = duckdb.connect()
result = conn.execute("""
    SELECT COUNT(*)
    FROM read_parquet('/app/data/gold/daily_summary/data.parquet') g
    LEFT JOIN (
        SELECT transaction_date, SUM(_signed_amount) AS s
        FROM read_parquet('/app/data/silver/transactions/**/*.parquet')
        WHERE _is_resolvable = true
        GROUP BY transaction_date
    ) sv ON g.transaction_date = sv.transaction_date
    WHERE g.total_signed_amount != COALESCE(sv.s, 0)
""").fetchone()[0]
print(result)
PYEOF
)
COUNT=$(echo "$RESULT" | tail -1)
if [ "$COUNT" = "0" ]; then
  pass "Gold total_signed_amount matches Silver resolvable SUM per date (INV-11)"
else
  fail "$COUNT date(s) with total_signed_amount mismatch vs Silver"
fi
echo ""

# CHECK 3: GOLD_WEEKLY_ONE_ROW_PER_ACCOUNT_WEEK
# No duplicate (account_id, week_start_date) — expect 0 duplicates.
echo "CHECK 3: GOLD_WEEKLY_ONE_ROW_PER_ACCOUNT_WEEK"
RESULT=$(docker compose run --rm pipeline python3 - 2>/dev/null << 'PYEOF' || true
import duckdb
conn = duckdb.connect()
result = conn.execute("""
    SELECT COUNT(*) FROM (
        SELECT account_id, week_start_date, COUNT(*) c
        FROM read_parquet('/app/data/gold/weekly_account_summary/data.parquet')
        GROUP BY account_id, week_start_date
        HAVING c > 1
    )
""").fetchone()[0]
print(result)
PYEOF
)
COUNT=$(echo "$RESULT" | tail -1)
if [ "$COUNT" = "0" ]; then
  pass "No duplicate (account_id, week_start_date) in Gold weekly summary (INV-38)"
else
  fail "$COUNT duplicate (account_id, week_start_date) pair(s) in Gold weekly summary"
fi
echo ""

# CHECK 4: GOLD_WEEKLY_PURCHASES_MATCH_SILVER
# For week starting 2024-01-01: Gold total_purchases = COUNT(*) from Silver
# PURCHASE transactions with _is_resolvable=true. Expect match.
echo "CHECK 4: GOLD_WEEKLY_PURCHASES_MATCH_SILVER"
RESULT=$(docker compose run --rm pipeline python3 - 2>/dev/null << 'PYEOF' || true
import duckdb
conn = duckdb.connect()
gold_purchases = conn.execute("""
    SELECT COALESCE(SUM(total_purchases), 0)
    FROM read_parquet('/app/data/gold/weekly_account_summary/data.parquet')
    WHERE week_start_date = DATE '2024-01-01'
""").fetchone()[0]
silver_purchases = conn.execute("""
    SELECT COUNT(*)
    FROM read_parquet('/app/data/silver/transactions/**/*.parquet') st
    JOIN read_parquet('/app/data/silver/transaction_codes/data.parquet') tc
        ON st.transaction_code = tc.transaction_code
    WHERE st._is_resolvable = true
      AND st.transaction_date >= DATE '2024-01-01'
      AND st.transaction_date <= DATE '2024-01-07'
      AND tc.transaction_type = 'PURCHASE'
""").fetchone()[0]
if gold_purchases == silver_purchases:
    print(f"MATCH {gold_purchases}")
else:
    print(f"MISMATCH gold={gold_purchases} silver={silver_purchases}")
PYEOF
)
LINE=$(echo "$RESULT" | tail -1)
OUTCOME=$(echo "$LINE" | awk '{print $1}')
if [ "$OUTCOME" = "MATCH" ]; then
  COUNT=$(echo "$LINE" | awk '{print $2}')
  pass "Gold total_purchases ($COUNT) matches Silver PURCHASE count for week 2024-01-01 (INV-11)"
else
  fail "Gold vs Silver PURCHASE mismatch: $LINE"
fi
echo ""

# CHECK 5: GOLD_CROSS_CONSISTENCY (INV-39)
# Every account in Gold weekly has at least one Gold daily row in same week
# with total_transactions > 0. Expect 0 violations.
echo "CHECK 5: GOLD_CROSS_CONSISTENCY"
RESULT=$(docker compose run --rm pipeline python3 - 2>/dev/null << 'PYEOF' || true
import duckdb
conn = duckdb.connect()
result = conn.execute("""
    SELECT COUNT(*)
    FROM (
        SELECT DISTINCT gw.account_id, gw.week_start_date, gw.week_end_date
        FROM read_parquet('/app/data/gold/weekly_account_summary/data.parquet') gw
    ) gw
    WHERE NOT EXISTS (
        SELECT 1
        FROM read_parquet('/app/data/gold/daily_summary/data.parquet') gd
        WHERE gd.transaction_date BETWEEN gw.week_start_date AND gw.week_end_date
          AND gd.total_transactions > 0
    )
""").fetchone()[0]
print(result)
PYEOF
)
COUNT=$(echo "$RESULT" | tail -1)
if [ "$COUNT" = "0" ]; then
  pass "Every Gold weekly account has at least one daily row with total_transactions > 0 (INV-39)"
else
  fail "$COUNT (account_id, week_start_date) pair(s) with no matching daily row with transactions > 0"
fi
echo ""

# CHECK 6: GOLD_NO_NULL_RUN_ID
# No null _pipeline_run_id in daily_summary or weekly_account_summary. Expect 0.
echo "CHECK 6: GOLD_NO_NULL_RUN_ID"
RESULT=$(docker compose run --rm pipeline python3 - 2>/dev/null << 'PYEOF' || true
import duckdb
conn = duckdb.connect()
null_daily = conn.execute("""
    SELECT COUNT(*) FROM read_parquet('/app/data/gold/daily_summary/data.parquet')
    WHERE _pipeline_run_id IS NULL
""").fetchone()[0]
null_weekly = conn.execute("""
    SELECT COUNT(*) FROM read_parquet('/app/data/gold/weekly_account_summary/data.parquet')
    WHERE _pipeline_run_id IS NULL
""").fetchone()[0]
print(null_daily + null_weekly)
PYEOF
)
COUNT=$(echo "$RESULT" | tail -1)
if [ "$COUNT" = "0" ]; then
  pass "No null _pipeline_run_id in Gold daily or weekly summary (INV-22)"
else
  fail "$COUNT null _pipeline_run_id record(s) across Gold layer"
fi
echo ""

# CHECK 7: GOLD_NO_BRONZE_READS
# grep -r "bronze/" dbt/models/gold/ — expect no output.
echo "CHECK 7: GOLD_NO_BRONZE_READS"
COUNT=$( (grep -r "bronze/" dbt/models/gold/ 2>/dev/null || true) | wc -l | tr -d ' ')
if [ "$COUNT" = "0" ]; then
  pass "No bronze/ path references in dbt/models/gold/ (INV-12)"
else
  fail "$COUNT line(s) in dbt/models/gold/ reference bronze/"
fi
echo ""

# CHECK 8: GOLD_ZERO_COUNT_DATES_PRESENT
# All expected dates must appear in Gold daily summary, including fully-quarantined dates.
echo "CHECK 8: GOLD_ZERO_COUNT_DATES_PRESENT"
RESULT=$(docker compose run --rm pipeline python3 - 2>/dev/null << 'PYEOF' || true
import duckdb
conn = duckdb.connect()
expected = {'2024-01-01','2024-01-02','2024-01-03','2024-01-04',
            '2024-01-05','2024-01-06','2024-01-07'}
present = set(
    str(r[0])
    for r in conn.execute(
        "SELECT DISTINCT transaction_date FROM read_parquet('/app/data/gold/daily_summary/data.parquet')"
    ).fetchall()
)
missing = sorted(expected - present)
print(len(missing))
for d in missing:
    print(f"  MISSING: {d}")
PYEOF
)
COUNT=$(echo "$RESULT" | head -1)
if [ "$COUNT" = "0" ]; then
  pass "All 7 dates (2024-01-01 to 2024-01-07) present in Gold daily summary (INV-44)"
else
  echo "$RESULT" | tail -n +2
  fail "$COUNT date(s) missing from Gold daily summary"
fi
echo ""

# CHECK 9: GOLD_UNRESOLVABLE_AMOUNTS_MATCH_SILVER
# Gold total_unresolvable_transactions and total_unresolvable_amount must match
# Silver _is_resolvable=false counts and signed amounts per date. (INV-11)
echo "CHECK 9: GOLD_UNRESOLVABLE_AMOUNTS_MATCH_SILVER"
RESULT=$(docker compose run --rm pipeline python3 - 2>/dev/null << 'PYEOF' || true
import duckdb
conn = duckdb.connect()
tx_mismatch = conn.execute("""
    SELECT COUNT(*)
    FROM read_parquet('/app/data/gold/daily_summary/data.parquet') g
    LEFT JOIN (
        SELECT transaction_date, COUNT(*) AS cnt
        FROM read_parquet('/app/data/silver/transactions/**/*.parquet')
        WHERE _is_resolvable = false
        GROUP BY transaction_date
    ) sv ON g.transaction_date = sv.transaction_date
    WHERE g.total_unresolvable_transactions != COALESCE(sv.cnt, 0)
""").fetchone()[0]
amt_mismatch = conn.execute("""
    SELECT COUNT(*)
    FROM read_parquet('/app/data/gold/daily_summary/data.parquet') g
    LEFT JOIN (
        SELECT transaction_date, SUM(_signed_amount) AS s
        FROM read_parquet('/app/data/silver/transactions/**/*.parquet')
        WHERE _is_resolvable = false
        GROUP BY transaction_date
    ) sv ON g.transaction_date = sv.transaction_date
    WHERE g.total_unresolvable_amount != COALESCE(sv.s, 0)
""").fetchone()[0]
print(tx_mismatch + amt_mismatch)
PYEOF
)
COUNT=$(echo "$RESULT" | tail -1)
if [ "$COUNT" = "0" ]; then
  pass "Gold unresolvable counts and amounts match Silver _is_resolvable=false per date (INV-11)"
else
  fail "$COUNT mismatch(es) in unresolvable transaction count or amount vs Silver"
fi
echo ""

# CHECK 10: GOLD_WEEKLY_ALL_SILVER_ACCOUNTS_PRESENT
# Every (account_id, week_start_date) with resolvable Silver transactions must
# have a corresponding row in Gold weekly summary. (INV-38 positive inclusion)
echo "CHECK 10: GOLD_WEEKLY_ALL_SILVER_ACCOUNTS_PRESENT"
RESULT=$(docker compose run --rm pipeline python3 - 2>/dev/null << 'PYEOF' || true
import duckdb
conn = duckdb.connect()
result = conn.execute("""
    SELECT COUNT(*)
    FROM (
        SELECT DISTINCT account_id,
               DATE_TRUNC('week', transaction_date)::DATE AS week_start
        FROM read_parquet('/app/data/silver/transactions/**/*.parquet')
        WHERE _is_resolvable = true
    ) sv
    LEFT JOIN read_parquet('/app/data/gold/weekly_account_summary/data.parquet') g
        ON sv.account_id = g.account_id
       AND sv.week_start = g.week_start_date
    WHERE g.account_id IS NULL
""").fetchone()[0]
print(result)
PYEOF
)
COUNT=$(echo "$RESULT" | tail -1)
if [ "$COUNT" = "0" ]; then
  pass "Every resolvable Silver (account_id, week) has a corresponding Gold weekly row (INV-38)"
else
  fail "$COUNT Silver resolvable (account_id, week) pair(s) missing from Gold weekly summary"
fi
echo ""

# CHECK 11: GOLD_NO_INCREMENTAL_MATERIALISATION
# No Gold model may use incremental materialisation. (INV-41)
echo "CHECK 11: GOLD_NO_INCREMENTAL_MATERIALISATION"
COUNT=$( (grep -r "incremental" dbt/models/gold/ 2>/dev/null || true) | wc -l | tr -d ' ')
if [ "$COUNT" = "0" ]; then
  pass "No incremental materialisation references in dbt/models/gold/ (INV-41)"
else
  fail "$COUNT incremental reference(s) found in dbt/models/gold/"
fi
echo ""

# CHECK 12: GOLD_NO_SOURCE_PATH_REFERENCES
# No Gold model may read from source/ CSV paths. (INV-12)
echo "CHECK 12: GOLD_NO_SOURCE_PATH_REFERENCES"
COUNT=$( (grep -r "source/" dbt/models/gold/ 2>/dev/null || true) | wc -l | tr -d ' ')
if [ "$COUNT" = "0" ]; then
  pass "No source/ path references in dbt/models/gold/ (INV-12)"
else
  fail "$COUNT source/ reference(s) found in dbt/models/gold/"
fi
echo ""

# CHECK 13: GOLD_DAILY_TRANSACTION_COUNT_MATCHES_SILVER
# Gold total_transactions must equal Silver COUNT(_is_resolvable=true) per date. (INV-11)
echo "CHECK 13: GOLD_DAILY_TRANSACTION_COUNT_MATCHES_SILVER"
RESULT=$(docker compose run --rm pipeline python3 - 2>/dev/null << 'PYEOF' || true
import duckdb
conn = duckdb.connect()
result = conn.execute("""
    SELECT COUNT(*)
    FROM read_parquet('/app/data/gold/daily_summary/data.parquet') g
    LEFT JOIN (
        SELECT transaction_date, COUNT(*) AS cnt
        FROM read_parquet('/app/data/silver/transactions/**/*.parquet')
        WHERE _is_resolvable = true
        GROUP BY transaction_date
    ) sv ON g.transaction_date = sv.transaction_date
    WHERE g.total_transactions != COALESCE(sv.cnt, 0)
""").fetchone()[0]
print(result)
PYEOF
)
COUNT=$(echo "$RESULT" | tail -1)
if [ "$COUNT" = "0" ]; then
  pass "Gold total_transactions matches Silver resolvable COUNT per date (INV-11)"
else
  fail "$COUNT date(s) with total_transactions mismatch vs Silver resolvable count"
fi
echo ""

# SUMMARY
echo "==================================="
echo "SUMMARY: $PASSED passed, $FAILED failed"
echo "==================================="
if [ "$FAILED" -eq 0 ]; then
  echo "ALL CHECKS PASSED"
  exit 0
else
  echo "SOME CHECKS FAILED"
  exit 1
fi
