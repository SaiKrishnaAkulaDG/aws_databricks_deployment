#!/bin/bash
docker compose run --rm pipeline bash -c "
cd /app && dbt run \
  --project-dir /app/dbt \
  --profiles-dir /app/dbt \
  --select silver_accounts \
  --vars '{\"target_date\": \"2024-01-01\", \"run_id\": \"test-diag-001\"}' \
  2>&1
"
