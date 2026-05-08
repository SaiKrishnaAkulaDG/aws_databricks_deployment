# CC Transactions Data Lake — S3-Direct Runbook

**Stack:** `cc-transactions-lake-stack` | **Region:** `us-east-1` | **Account:** `065317679010`  
**S3:** `cc-transaction-databricks-datalake-2026`  
**Branch:** `feature/s3-direct-writes`

> This runbook covers the **S3-direct-writes** pipeline (branch `feature/s3-direct-writes`).  
> For the original local-disk pipeline see `RUNBOOK.md`.  
> Key difference: pipeline writes directly to S3 during the run — **no manual sync step needed**.

---

## Full Pipeline Run — GitHub Actions (Recommended)

### Run Historical Pipeline (Days 1–6)

```bash
gh workflow run run-pipeline.yml \
  --ref feature/s3-direct-writes \
  -f mode=historical \
  -f start_date=2024-01-01 \
  -f end_date=2024-01-06
```

Watch the run:
```bash
gh run watch <run-id> --exit-status
```

### Run Incremental Pipeline (picks up next date from watermark)

```bash
gh workflow run run-pipeline.yml \
  --ref feature/s3-direct-writes \
  -f mode=incremental
```

### Get the latest run ID
```bash
gh run list --workflow=run-pipeline.yml --limit=5
```

---

## Full Pipeline Run — Manual (SSH)

### Step 1 — Get the EC2 instance ID (after teardown+redeploy, ID changes)

```bash
aws cloudformation describe-stacks \
  --stack-name cc-transactions-lake-stack \
  --region us-east-1 \
  --profile DG4-Developer-065317679010 \
  --query 'Stacks[0].Outputs[?OutputKey==`EC2InstanceId`].OutputValue' \
  --output text
```

### Step 2 — Start the instance

```bash
aws ec2 start-instances \
  --instance-ids <INSTANCE_ID> \
  --region us-east-1 \
  --profile DG4-Developer-065317679010
```

### Step 3 — Get the public IP

```bash
aws cloudformation describe-stacks \
  --stack-name cc-transactions-lake-stack \
  --region us-east-1 \
  --profile DG4-Developer-065317679010 \
  --query 'Stacks[0].Outputs[?OutputKey==`EC2InstancePublicIP`].OutputValue' \
  --output text
```

### Step 4 — Inject AWS credentials into `.env`

SSH into EC2 and run this to populate credentials from IMDS:

```bash
ssh -i Infra-AWS/cc-transactions-lake-key.pem ubuntu@<EC2_IP>

# On EC2:
TOKEN=$(curl -sf -X PUT "http://169.254.169.254/latest/api/token" \
  -H "X-aws-ec2-metadata-token-ttl-seconds: 3600")
ROLE=$(curl -sf -H "X-aws-ec2-metadata-token: $TOKEN" \
  "http://169.254.169.254/latest/meta-data/iam/security-credentials/")
CREDS=$(curl -sf -H "X-aws-ec2-metadata-token: $TOKEN" \
  "http://169.254.169.254/latest/meta-data/iam/security-credentials/$ROLE")
KEY_ID=$(echo "$CREDS" | python3 -c "import sys,json; print(json.load(sys.stdin)['AccessKeyId'])")
SECRET=$(echo "$CREDS" | python3 -c "import sys,json; print(json.load(sys.stdin)['SecretAccessKey'])")
TOKEN_VAL=$(echo "$CREDS" | python3 -c "import sys,json; print(json.load(sys.stdin)['Token'])")

sudo sed -i "/^AWS_ACCESS_KEY_ID=/d; /^AWS_SECRET_ACCESS_KEY=/d; /^AWS_SESSION_TOKEN=/d" /app/.env
echo "AWS_ACCESS_KEY_ID=$KEY_ID" | sudo tee -a /app/.env > /dev/null
echo "AWS_SECRET_ACCESS_KEY=$SECRET" | sudo tee -a /app/.env > /dev/null
echo "AWS_SESSION_TOKEN=$TOKEN_VAL" | sudo tee -a /app/.env > /dev/null
echo "Credentials injected."
```

### Step 5 — Run Historical Pipeline (Days 1–6)

```bash
# On EC2:
cd /app && sudo docker compose run --rm pipeline \
  python -m pipeline.pipeline --mode historical \
  --start-date 2024-01-01 --end-date 2024-01-06
```

### Step 6 — Run Incremental Pipeline (Day 7)

```bash
# On EC2:
cd /app && sudo docker compose run --rm pipeline \
  python -m pipeline.pipeline --mode incremental
```

### Step 7 — Stop the instance

```bash
aws ec2 stop-instances \
  --instance-ids <INSTANCE_ID> \
  --region us-east-1 \
  --profile DG4-Developer-065317679010
```

---

## Verify Pipeline Outputs (Query S3 Directly)

Run these from the EC2 container after the pipeline completes:

```bash
sudo docker compose run --rm pipeline python -c "
import duckdb, os
from pipeline.s3_utils import configure_duckdb_s3

bucket = os.environ['S3_BUCKET']
con = duckdb.connect()
configure_duckdb_s3(con)

wm    = con.execute(f\"SELECT last_processed_date FROM read_parquet('s3://{bucket}/pipeline/control.parquet')\").fetchone()[0]
brnz  = con.execute(f\"SELECT COUNT(*) FROM read_parquet('s3://{bucket}/bronze/transactions/**/*.parquet')\").fetchone()[0]
silv  = con.execute(f\"SELECT COUNT(*) FROM read_parquet('s3://{bucket}/silver/transactions/**/*.parquet')\").fetchone()[0]
gd    = con.execute(f\"SELECT COUNT(*) FROM read_parquet('s3://{bucket}/gold/daily_summary/data.parquet')\").fetchone()[0]
gw    = con.execute(f\"SELECT COUNT(*) FROM read_parquet('s3://{bucket}/gold/weekly_account_summary/data.parquet')\").fetchone()[0]
rl    = con.execute(f\"SELECT COUNT(*) FROM read_parquet('s3://{bucket}/pipeline/run_log.parquet')\").fetchone()[0]

print(f'Watermark:    {wm}')
print(f'Bronze txns:  {brnz}')
print(f'Silver txns:  {silv}')
print(f'Gold daily:   {gd}')
print(f'Gold weekly:  {gw}')
print(f'Run log rows: {rl}')
"
```

**Expected after historical (Jan 1–6) + incremental (Jan 7):**

| Metric | Expected |
|--------|----------|
| Watermark | `2024-01-07` |
| Bronze transactions | 35 |
| Silver transactions | 28 |
| Gold daily rows | 7 |
| Gold weekly rows | 3 |
| Run log rows | ≥ 2 |

Or from local machine:
```bash
aws s3 ls s3://cc-transaction-databricks-datalake-2026/gold/ \
  --recursive --profile DG4-Developer-065317679010
# Expected: daily_summary/data.parquet + weekly_account_summary/data.parquet

aws s3 ls s3://cc-transaction-databricks-datalake-2026/pipeline/ \
  --recursive --profile DG4-Developer-065317679010
# Expected: control.parquet + gold_weekly_control.parquet + run_log.parquet
```

---

## Reset and Rerun

Data lives on S3 — to reset, delete the S3 objects (not local disk):

```bash
aws s3 rm s3://cc-transaction-databricks-datalake-2026/bronze/   --recursive --profile DG4-Developer-065317679010
aws s3 rm s3://cc-transaction-databricks-datalake-2026/silver/   --recursive --profile DG4-Developer-065317679010
aws s3 rm s3://cc-transaction-databricks-datalake-2026/gold/     --recursive --profile DG4-Developer-065317679010
aws s3 rm s3://cc-transaction-databricks-datalake-2026/pipeline/ --recursive --profile DG4-Developer-065317679010
```

Then rerun from Step 5.

**Claude Code prompt:**
```
clear the s3 pipeline data and rerun historical 1 to 6 then incremental on 7
```

---

## After Teardown + Redeploy — Update Instance ID Secret

```bash
NEW_ID=$(aws cloudformation describe-stacks \
  --stack-name cc-transactions-lake-stack \
  --region us-east-1 \
  --profile DG4-Developer-065317679010 \
  --query 'Stacks[0].Outputs[?OutputKey==`EC2InstanceId`].OutputValue' \
  --output text)
echo "New instance ID: $NEW_ID"
echo "$NEW_ID" | gh secret set EC2_INSTANCE_ID
```

---

## Quick Verification Checklist

| Check | Command | Expected |
|-------|---------|----------|
| Stack status | `aws cloudformation describe-stacks --stack-name cc-transactions-lake-stack --query 'Stacks[0].StackStatus' --output text --profile DG4-Developer-065317679010 --region us-east-1` | `CREATE_COMPLETE` |
| S3 gold files | `aws s3 ls s3://cc-transaction-databricks-datalake-2026/gold/ --recursive --profile DG4-Developer-065317679010` | 2 parquet files |
| S3 pipeline files | `aws s3 ls s3://cc-transaction-databricks-datalake-2026/pipeline/ --recursive --profile DG4-Developer-065317679010` | 3 parquet files |
| Latest GH Actions run | `gh run list --workflow=run-pipeline.yml --limit=1` | `completed / success` |

---

## Gotchas

| Issue | Fix |
|-------|-----|
| Docker commands fail on EC2 | Prefix with `sudo` |
| `.env not found` on fresh EC2 | `sudo cp /app/.env.example /app/.env` then run credential injection (Step 4) |
| S3 write returns 403 | Re-run credential injection — IMDS tokens expire after 1 hour |
| EC2 IP changed after restart | Re-fetch from CloudFormation outputs (Step 3) |
| EC2 instance ID changed after redeploy | Run the "Update Instance ID Secret" command above |
| `run_log.parquet` 404 on first run | Expected — handled automatically; pipeline creates the file on first flush |

---

## Cost Control

| Action | Cost |
|--------|------|
| Instance running | ~$0.0104/hr |
| Instance stopped | ~$0.0004/hr (EBS only) |
| Single full pipeline run | ~$0.002 |
| S3 storage (7 days data) | ~$0.001/month |

**Always stop the instance after each run.**

---

**Last Updated:** 2026-05-07
