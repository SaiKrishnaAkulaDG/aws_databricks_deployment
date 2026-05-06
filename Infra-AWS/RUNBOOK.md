# CC Transactions Data Lake — Operational Runbook

**Stack:** `cc-transactions-lake-stack` | **Region:** `us-east-1` | **Account:** `065317679010`  
**EC2:** `i-010a06f920b86bd2a` | **S3:** `cc-transaction-databricks-datalake-2026`  
**Profile:** `DG4-Developer-065317679010` | **Key:** `Infra-AWS/cc-transactions-lake-key.pem`

---

## Full Pipeline Run (Standard Flow)

### Step 1 — Start the Instance

**Command:**
```bash
aws ec2 start-instances \
  --instance-ids i-010a06f920b86bd2a \
  --region us-east-1 \
  --profile DG4-Developer-065317679010
```

**Verify (wait ~60s then check):**
```bash
aws ec2 describe-instance-status \
  --instance-ids i-010a06f920b86bd2a \
  --region us-east-1 \
  --profile DG4-Developer-065317679010 \
  --query "InstanceStatuses[0].InstanceStatus.Status" \
  --output text
# Expected: ok
```

**Claude Code prompt:**
```
start the ec2 instance
```

---

### Step 2 — Get the Public IP

**Command:**
```bash
aws cloudformation describe-stacks \
  --stack-name cc-transactions-lake-stack \
  --region us-east-1 \
  --profile DG4-Developer-065317679010 \
  --query 'Stacks[0].Outputs[?OutputKey==`EC2InstancePublicIP`].OutputValue' \
  --output text
```

**Claude Code prompt:**
```
get the ec2 ip
```

---

### Step 3 — SSH into the Instance

**Command:**
```bash
ssh -i Infra-AWS/cc-transactions-lake-key.pem ubuntu@<EC2_IP>
```

**Verify:**
```bash
# Once inside EC2:
ls /app/source/ | wc -l
# Expected: 15 (CSVs already uploaded from previous session)
```

**Claude Code prompt:**
```
ssh into the instance
```

---

### Step 4 — Run Historical Pipeline (Days 1–6)

**Command (on EC2):**
```bash
cd /app && sudo docker compose run --rm pipeline python -m pipeline.pipeline \
  --mode historical --start-date 2024-01-01 --end-date 2024-01-06
```

**Verify:**
```bash
sudo docker compose run --rm pipeline python -c "
import duckdb; con = duckdb.connect()
print('Watermark:', con.execute(\"SELECT last_processed_date FROM read_parquet('/app/data/pipeline/control.parquet')\").fetchone()[0])
print('Bronze:', con.execute(\"SELECT COUNT(*) FROM read_parquet('/app/data/bronze/transactions/**/*.parquet')\").fetchone()[0])
print('Silver:', con.execute(\"SELECT COUNT(*) FROM read_parquet('/app/data/silver/transactions/**/*.parquet')\").fetchone()[0])
print('Gold daily:', con.execute(\"SELECT COUNT(*) FROM read_parquet('/app/data/gold/daily_summary/data.parquet')\").fetchone()[0])
" 2>&1 | grep -v warning
# Expected: Watermark: 2024-01-06 | Bronze: 30 | Silver: 24 | Gold daily: 6
```

**Claude Code prompt:**
```
run historical 1 to 6 dates
```

---

### Step 5 — Run Incremental Pipeline (Day 7)

**Command (on EC2):**
```bash
cd /app && sudo docker compose run --rm pipeline python -m pipeline.pipeline --mode incremental
```

**Verify:**
```bash
sudo docker compose run --rm pipeline python -c "
import duckdb; con = duckdb.connect()
print('Watermark:', con.execute(\"SELECT last_processed_date FROM read_parquet('/app/data/pipeline/control.parquet')\").fetchone()[0])
print('Gold daily:', con.execute(\"SELECT COUNT(*) FROM read_parquet('/app/data/gold/daily_summary/data.parquet')\").fetchone()[0])
print('Gold weekly:', con.execute(\"SELECT COUNT(*) FROM read_parquet('/app/data/gold/weekly_account_summary/data.parquet')\").fetchone()[0])
" 2>&1 | grep -v warning
# Expected: Watermark: 2024-01-07 | Gold daily: 7 | Gold weekly: 3
```

**Claude Code prompt:**
```
run incremental on 7 date
```

---

### Step 6 — Sync Data to S3

**Command (on EC2):**
```bash
/home/ubuntu/.local/bin/aws s3 sync /app/data/bronze   s3://cc-transaction-databricks-datalake-2026/bronze/
/home/ubuntu/.local/bin/aws s3 sync /app/data/silver   s3://cc-transaction-databricks-datalake-2026/silver/
/home/ubuntu/.local/bin/aws s3 sync /app/data/gold     s3://cc-transaction-databricks-datalake-2026/gold/
/home/ubuntu/.local/bin/aws s3 sync /app/data/pipeline s3://cc-transaction-databricks-datalake-2026/pipeline/
```

**Verify (from local machine):**
```bash
aws s3 ls s3://cc-transaction-databricks-datalake-2026/gold/ \
  --recursive --profile DG4-Developer-065317679010
# Expected: daily_summary/data.parquet + weekly_account_summary/data.parquet
```

**Claude Code prompt:**
```
sync to s3
```

---

### Step 7 — Stop the Instance

**Command:**
```bash
aws ec2 stop-instances \
  --instance-ids i-010a06f920b86bd2a \
  --region us-east-1 \
  --profile DG4-Developer-065317679010
```

**Claude Code prompt:**
```
stop the instance
```

---

## Deploy / Teardown

### Deploy Stack from Scratch

```bash
# 1. Ensure key pair exists
aws ec2 describe-key-pairs \
  --key-names cc-transactions-lake-key \
  --region us-east-1 \
  --profile DG4-Developer-065317679010

# 2. Deploy
cd Infra-AWS
AWS_PROFILE=DG4-Developer-065317679010 bash deploy-cc-lake.sh create

# 3. Wait ~3 min for UserData to finish, then upload source CSVs
scp -i cc-transactions-lake-key.pem source/*.csv ubuntu@<EC2_IP>:/tmp/
ssh -i cc-transactions-lake-key.pem ubuntu@<EC2_IP> "sudo cp /tmp/*.csv /app/source/"
```

**Claude Code prompt:**
```
proceed to deploy the stack
```

### Delete Stack

```bash
aws cloudformation delete-stack \
  --stack-name cc-transactions-lake-stack \
  --region us-east-1 \
  --profile DG4-Developer-065317679010
```

**Claude Code prompt:**
```
delete the stack
```

---

## Reset and Rerun Pipeline

```bash
# Clear all pipeline data on EC2 (SSH in first)
sudo rm -rf /app/data/bronze/* /app/data/silver/* /app/data/gold/* /app/data/pipeline/*

# Then rerun from Step 4
```

**Claude Code prompt:**
```
clear the data and rerun historical 1 to 6 then incremental on 7
```

---

## Quick Verification Checklist

| Check | Command | Expected |
|-------|---------|----------|
| Stack status | `aws cloudformation describe-stacks --stack-name cc-transactions-lake-stack --query 'Stacks[0].StackStatus' --output text --profile DG4-Developer-065317679010 --region us-east-1` | `CREATE_COMPLETE` |
| EC2 state | `aws ec2 describe-instances --instance-ids i-010a06f920b86bd2a --query 'Reservations[0].Instances[0].State.Name' --output text --profile DG4-Developer-065317679010 --region us-east-1` | `running` or `stopped` |
| Watermark | Query `control.parquet` (see Step 5) | `2024-01-07` |
| S3 gold files | `aws s3 ls s3://cc-transaction-databricks-datalake-2026/gold/ --recursive --profile DG4-Developer-065317679010` | 2 parquet files |
| S3 pipeline files | `aws s3 ls s3://cc-transaction-databricks-datalake-2026/pipeline/ --recursive --profile DG4-Developer-065317679010` | 3 parquet files |

---

## Cost Control

| Action | Cost |
|--------|------|
| Instance running | ~$0.0104/hr |
| Instance stopped | ~$0.0004/hr (EBS only) |
| Single full pipeline run | ~$0.002 |

**Always stop after each run.**

---

## Gotchas

| Issue | Fix |
|-------|-----|
| `aws: command not found` on EC2 | Use `/home/ubuntu/.local/bin/aws` |
| Docker commands fail on EC2 | Prefix with `sudo` (`sudo docker compose run ...`) |
| `.env not found` | `sudo cp /app/.env.example /app/.env` |
| EC2 IP changed after restart | Re-fetch IP from CloudFormation outputs (Step 2) |
| Source CSVs missing after fresh deploy | Upload via `scp` (see Deploy section) |
