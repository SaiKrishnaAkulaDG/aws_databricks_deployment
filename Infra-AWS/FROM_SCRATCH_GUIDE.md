# From Scratch to S3-Direct Pipeline — Manual Guide

**Branch:** `feature/s3-direct-writes`  
**Stack:** `cc-transactions-lake-stack` | **Region:** `us-east-1` | **Account:** `065317679010`  
**S3:** `cc-transaction-databricks-datalake-2026`

> End-to-end guide from CloudFormation stack creation → SSH into EC2 → run pipeline → verify S3 outputs.  
> No GitHub Actions. All steps run from your local machine + SSH into EC2.

---

## Prerequisites

### Tools needed locally

```bash
# AWS CLI v2
aws --version

# Verify AWS profile works
aws sts get-caller-identity --profile DG4-Developer-065317679010
# Expected: "Account": "065317679010"

# GitHub CLI (only needed to clone — optional if repo already on EC2)
gh --version
```

---

## Step 1 — Create EC2 Key Pair (one-time, skip if key already exists)

### Claude Code prompt
```
create an ec2 key pair named cc-transactions-lake-key in us-east-1 and save the pem file to Infra-AWS/
```

### Command
```bash
# Run from repo root
aws ec2 create-key-pair \
  --key-name cc-transactions-lake-key \
  --region us-east-1 \
  --profile DG4-Developer-065317679010 \
  --query 'KeyMaterial' \
  --output text > Infra-AWS/cc-transactions-lake-key.pem

# Fix permissions (required for SSH)
chmod 400 Infra-AWS/cc-transactions-lake-key.pem

# Verify
aws ec2 describe-key-pairs \
  --key-names cc-transactions-lake-key \
  --region us-east-1 \
  --profile DG4-Developer-065317679010
```

> **Note:** `Infra-AWS/cc-transactions-lake-key.pem` is gitignored — never commit it.

---

## Step 2 — Deploy CloudFormation Stack

### Claude Code prompt
```
deploy the cloudformation stack using deploy-cc-lake.sh
```

### Command (from `Infra-AWS/` folder)
```bash
cd Infra-AWS
AWS_PROFILE=DG4-Developer-065317679010 bash deploy-cc-lake.sh create
```

This script:
1. Validates prerequisites (AWS CLI, credentials, region, template file)
2. Creates the CloudFormation stack with these resources: EC2 (t3.micro), S3 bucket, IAM role + instance profile, security group, CloudWatch log group
3. Waits for `CREATE_COMPLETE` (~5–10 min)
4. Prints stack outputs (EC2 ID, public IP, S3 paths)

### Stack parameters used by the script

| Parameter | Value |
|-----------|-------|
| `S3BucketName` | `cc-transaction-databricks-datalake-2026` |
| `EC2InstanceType` | `t3.micro` |
| `EBSVolumeSize` | `5` |
| `KeyName` | `cc-transactions-lake-key` |
| `GitHubRepoURL` | `https://github.com/SaiKrishnaAkulaDG/aws_databricks_deployment.git` |

### Check stack status
```bash
aws cloudformation describe-stacks \
  --stack-name cc-transactions-lake-stack \
  --query 'Stacks[0].StackStatus' \
  --output text \
  --region us-east-1 \
  --profile DG4-Developer-065317679010
# Expected: CREATE_COMPLETE
```

---

## Step 3 — Get Stack Outputs

### Claude Code prompt
```
get the cloudformation stack outputs — instance id, public ip, s3 bucket
```

### Commands
```bash
# All outputs in table format
aws cloudformation describe-stacks \
  --stack-name cc-transactions-lake-stack \
  --query 'Stacks[0].Outputs' \
  --output table \
  --region us-east-1 \
  --profile DG4-Developer-065317679010

# EC2 instance ID (save this — you'll need it to start/stop)
aws cloudformation describe-stacks \
  --stack-name cc-transactions-lake-stack \
  --region us-east-1 --profile DG4-Developer-065317679010 \
  --query 'Stacks[0].Outputs[?OutputKey==`EC2InstanceId`].OutputValue' \
  --output text

# EC2 public IP
aws cloudformation describe-stacks \
  --stack-name cc-transactions-lake-stack \
  --region us-east-1 --profile DG4-Developer-065317679010 \
  --query 'Stacks[0].Outputs[?OutputKey==`EC2InstancePublicIP`].OutputValue' \
  --output text
```

> **Note:** EC2 public IP changes every time the instance is started. Always re-fetch before SSH.

---

## Step 4 — Start EC2 Instance

### Claude Code prompt
```
start ec2 instance <INSTANCE_ID> and get the public ip
```

### Commands
```bash
# Start the instance
aws ec2 start-instances \
  --instance-ids <INSTANCE_ID> \
  --region us-east-1 \
  --profile DG4-Developer-065317679010

# Wait until running (~30–60 sec)
aws ec2 wait instance-running \
  --instance-ids <INSTANCE_ID> \
  --region us-east-1 \
  --profile DG4-Developer-065317679010

# Get fresh public IP (changes on every start)
aws cloudformation describe-stacks \
  --stack-name cc-transactions-lake-stack \
  --region us-east-1 --profile DG4-Developer-065317679010 \
  --query 'Stacks[0].Outputs[?OutputKey==`EC2InstancePublicIP`].OutputValue' \
  --output text
```

---

## Step 5 — SSH into EC2 and Prepare Environment

### Claude Code prompt
```
ssh into ec2 at <EC2_IP> and prepare the environment — refresh .env, pull latest code, rebuild docker
```

### SSH in
```bash
# From repo root
ssh -i Infra-AWS/cc-transactions-lake-key.pem ubuntu@<EC2_IP>
```

### On EC2 — run these commands
```bash
# Pull latest code from the S3-direct branch
cd /app
git pull origin feature/s3-direct-writes

# Always overwrite .env from .env.example
# .env only contains: S3_BUCKET + AWS_DEFAULT_REGION (no credentials)
# Credentials come from EC2 instance role via IMDS — no injection needed
sudo cp /app/.env.example /app/.env
cat /app/.env
# Expected:
# S3_BUCKET=cc-transaction-databricks-datalake-2026
# AWS_DEFAULT_REGION=us-east-1

# Clear BuildKit cache before build (prevents exit-code-17 corruption)
sudo docker builder prune -f

# Build Docker image
sudo docker compose build
```

> **Why always overwrite .env:** EC2 is stopped between runs but not terminated — the filesystem persists. Any previous `.env` may contain expired AWS session tokens. Overwriting ensures only non-secret config is present.

> **Credential flow:** `pipeline/s3_utils.py configure_duckdb_s3()` fetches credentials at runtime via `boto3 → EC2 instance role → IMDS` (reachable because `docker-compose.yml` uses `network_mode: host` and the CF template sets `HttpPutResponseHopLimit: 2`). No credentials stored in `.env` or code.

---

## Step 6 — Run Historical Pipeline (Days 1–6)

### Claude Code prompt
```
run historical pipeline from 2024-01-01 to 2024-01-06 on the ec2
```

### Command (on EC2 via SSH)
```bash
cd /app
sudo docker compose run --rm pipeline \
  python -m pipeline.pipeline \
  --mode historical \
  --start-date 2024-01-01 \
  --end-date 2024-01-06
```

### What happens inside the container
1. `configure_duckdb_s3()` fetches credentials from EC2 instance role via boto3 → IMDS
2. Injects credentials into DuckDB via `SET s3_access_key_id / s3_secret_access_key / s3_session_token`
3. Exports credentials to `os.environ` so dbt subprocess inherits them via `env_var()` in `profiles.yml`
4. Bronze loaders write directly to `s3://cc-transaction-databricks-datalake-2026/bronze/`
5. dbt silver/gold models read from and write to S3 via DuckDB httpfs
6. Control plane files (`control.parquet`, `run_log.parquet`) written to `s3://.../pipeline/` via boto3

### Expected output (last lines)
```
Historical pipeline completed successfully.
Watermark advanced to 2024-01-06
```

---

## Step 7 — Run Incremental Pipeline (Day 7)

### Claude Code prompt
```
run incremental pipeline on the ec2
```

### Command (on EC2 via SSH)
```bash
cd /app
sudo docker compose run --rm pipeline \
  python -m pipeline.pipeline \
  --mode incremental
```

Incremental reads `last_processed_date` from `s3://.../pipeline/control.parquet` → processes `watermark + 1 day` = `2024-01-07`.

### Expected output (last lines)
```
Incremental pipeline completed successfully.
Watermark advanced to 2024-01-07
```

---

## Step 8 — Verify S3 Outputs

### Claude Code prompt
```
verify s3 pipeline outputs — query bronze silver gold and control plane counts
```

### Query S3 via DuckDB (from EC2 container)
```bash
# On EC2
sudo docker compose run --rm pipeline python -c "
import duckdb, os
from pipeline.s3_utils import configure_duckdb_s3

bucket = os.environ['S3_BUCKET']
con = duckdb.connect()
configure_duckdb_s3(con)

wm   = con.execute(f\"SELECT last_processed_date FROM read_parquet('s3://{bucket}/pipeline/control.parquet')\").fetchone()[0]
brnz = con.execute(f\"SELECT COUNT(*) FROM read_parquet('s3://{bucket}/bronze/transactions/**/*.parquet')\").fetchone()[0]
silv = con.execute(f\"SELECT COUNT(*) FROM read_parquet('s3://{bucket}/silver/transactions/**/*.parquet')\").fetchone()[0]
gd   = con.execute(f\"SELECT COUNT(*) FROM read_parquet('s3://{bucket}/gold/daily_summary/data.parquet')\").fetchone()[0]
gw   = con.execute(f\"SELECT COUNT(*) FROM read_parquet('s3://{bucket}/gold/weekly_account_summary/data.parquet')\").fetchone()[0]
rl   = con.execute(f\"SELECT COUNT(*) FROM read_parquet('s3://{bucket}/pipeline/run_log.parquet')\").fetchone()[0]

print(f'Watermark:    {wm}')
print(f'Bronze txns:  {brnz}')
print(f'Silver txns:  {silv}')
print(f'Gold daily:   {gd}')
print(f'Gold weekly:  {gw}')
print(f'Run log rows: {rl}')
"
```

### From local machine (quick file-presence check)
```bash
# Gold outputs
aws s3 ls s3://cc-transaction-databricks-datalake-2026/gold/ \
  --recursive --profile DG4-Developer-065317679010

# Control plane files
aws s3 ls s3://cc-transaction-databricks-datalake-2026/pipeline/ \
  --recursive --profile DG4-Developer-065317679010

# Bronze partitions (7 date folders)
aws s3 ls s3://cc-transaction-databricks-datalake-2026/bronze/transactions/ \
  --recursive --profile DG4-Developer-065317679010
```

### Expected values after historical (Jan 1–6) + incremental (Jan 7)

| Metric | Expected |
|--------|----------|
| Watermark | `2024-01-07` |
| Bronze transactions | `35` |
| Silver transactions | `28` |
| Gold daily rows | `7` |
| Gold weekly rows | `3` |
| Run log rows | `≥ 2` |

---

## Step 9 — Stop EC2 (always do this after every run)

### Claude Code prompt
```
stop ec2 instance <INSTANCE_ID>
```

### Command
```bash
aws ec2 stop-instances \
  --instance-ids <INSTANCE_ID> \
  --region us-east-1 \
  --profile DG4-Developer-065317679010
```

> **Cost:** Running = ~$0.0104/hr. Stopped = ~$0.0004/hr (EBS only). Always stop when done.

---

## Step 10 — Reset and Rerun (Same Stack, Wipe S3 Data)

Use when you need to re-run from a clean state without tearing down the stack.

### Claude Code prompt
```
clear the s3 pipeline data — bronze silver gold pipeline folders
```

### Commands
```bash
aws s3 rm s3://cc-transaction-databricks-datalake-2026/bronze/   --recursive --profile DG4-Developer-065317679010
aws s3 rm s3://cc-transaction-databricks-datalake-2026/silver/   --recursive --profile DG4-Developer-065317679010
aws s3 rm s3://cc-transaction-databricks-datalake-2026/gold/     --recursive --profile DG4-Developer-065317679010
aws s3 rm s3://cc-transaction-databricks-datalake-2026/pipeline/ --recursive --profile DG4-Developer-065317679010
```

Then repeat Steps 4–9.

---

## Step 11 — Teardown Stack

### Claude Code prompt
```
delete the cloudformation stack and empty the s3 bucket first
```

### Commands
```bash
# Step 1 — Empty S3 bucket (versioned bucket — must delete all versions and delete markers)
aws s3api list-object-versions \
  --bucket cc-transaction-databricks-datalake-2026 \
  --profile DG4-Developer-065317679010 \
  --query '{Objects: Versions[].{Key:Key,VersionId:VersionId}}' \
  --output json | \
aws s3api delete-objects \
  --bucket cc-transaction-databricks-datalake-2026 \
  --profile DG4-Developer-065317679010 \
  --delete file:///dev/stdin || true

# Also purge delete markers
aws s3api list-object-versions \
  --bucket cc-transaction-databricks-datalake-2026 \
  --profile DG4-Developer-065317679010 \
  --query '{Objects: DeleteMarkers[].{Key:Key,VersionId:VersionId}}' \
  --output json | \
aws s3api delete-objects \
  --bucket cc-transaction-databricks-datalake-2026 \
  --profile DG4-Developer-065317679010 \
  --delete file:///dev/stdin || true

# Step 2 — Delete CloudFormation stack
aws cloudformation delete-stack \
  --stack-name cc-transactions-lake-stack \
  --region us-east-1 \
  --profile DG4-Developer-065317679010

# Step 3 — Wait for deletion
aws cloudformation wait stack-delete-complete \
  --stack-name cc-transactions-lake-stack \
  --region us-east-1 \
  --profile DG4-Developer-065317679010

echo "Stack deleted"
```

> **After teardown:** A new deploy (Step 2) creates a new EC2 instance with a new instance ID. Re-fetch outputs (Step 3) to get the new ID.

---

## Full Sequence — Quick Reference

| Step | Claude Code Prompt | Command Summary |
|------|--------------------|----------------|
| 1 | `create ec2 key pair cc-transactions-lake-key` | `aws ec2 create-key-pair` → save to `Infra-AWS/*.pem` |
| 2 | `deploy the cloudformation stack` | `bash deploy-cc-lake.sh create` |
| 3 | `get the stack outputs` | `aws cloudformation describe-stacks ... --query Outputs` |
| 4 | `start ec2 instance <ID> and get the ip` | `aws ec2 start-instances` + `wait instance-running` |
| 5 | `ssh into ec2, refresh .env, pull code, rebuild docker` | `ssh` → `git pull` → `cp .env.example .env` → `docker build` |
| 6 | `run historical pipeline 2024-01-01 to 2024-01-06 on ec2` | `docker compose run pipeline --mode historical` |
| 7 | `run incremental pipeline on ec2` | `docker compose run pipeline --mode incremental` |
| 8 | `verify s3 pipeline outputs` | DuckDB query or `aws s3 ls` |
| 9 | `stop ec2 instance <ID>` | `aws ec2 stop-instances` |
| 10 | `clear s3 pipeline data and rerun` | `aws s3 rm` all 4 folders → repeat 4–9 |
| 11 | `delete the cloudformation stack` | empty S3 versions → `delete-stack` |

---

## S3 Layout After Successful Run

```
cc-transaction-databricks-datalake-2026/
├─ bronze/
│   ├─ accounts/date=2024-01-01/data.parquet  ...  date=2024-01-07/
│   ├─ transactions/date=2024-01-01/data.parquet  ...  date=2024-01-07/
│   └─ transaction_codes/data.parquet
├─ silver/
│   ├─ accounts/data.parquet
│   ├─ transactions/date=2024-01-01/data.parquet  ...  date=2024-01-07/
│   └─ transaction_codes/data.parquet
├─ gold/
│   ├─ daily_summary/data.parquet          (7 rows — one per date)
│   └─ weekly_account_summary/data.parquet (3 rows — account x week)
└─ pipeline/
    ├─ control.parquet                      (watermark = 2024-01-07)
    ├─ gold_weekly_control.parquet          (computed weeks registry)
    └─ run_log.parquet                      (audit trail, ≥2 rows)
```

---

## Credential Architecture (Why No .env Credentials)

```
CF template bakes in: HttpPutResponseHopLimit: 2
docker-compose.yml:   network_mode: host
        ↓
Container can reach EC2 IMDS at 169.254.169.254
        ↓
boto3.Session().get_credentials()   ←  pipeline/s3_utils.py configure_duckdb_s3()
        ↓                    ↓
DuckDB SET commands      os.environ["AWS_ACCESS_KEY_ID / SECRET / SESSION_TOKEN"]
        ↓                    ↓
DuckDB httpfs S3     dbt subprocess → profiles.yml env_var() → dbt-duckdb sessions
```

**DuckDB 0.10.0 does NOT auto-resolve IMDS** — boto3 must fetch and inject credentials explicitly.  
**os.environ export** is required so the dbt subprocess inherits credentials (dbt runs as a child process).

---

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| SSH `error in libcrypto` | Key file has CRLF (Windows line endings) | Re-create key: `aws ec2 create-key-pair` → `chmod 400` |
| SSH `Permission denied` | Wrong key file or wrong user | Use `ubuntu@<IP>`, not `ec2-user`. Use `-i Infra-AWS/cc-transactions-lake-key.pem` |
| `S3 403 Forbidden` on DuckDB write | IAM instance role not attached or IMDS hop limit wrong | Verify: `aws ec2 describe-instance-attribute --instance-id <ID> --attribute metadataOptions` — `HttpPutResponseHopLimit` must be `2` |
| `S3 400 Bad Request` | Stale expired credentials in `.env` | `sudo cp /app/.env.example /app/.env` — removes all credential vars |
| Docker build exit code 17 | BuildKit cache corruption | `sudo docker builder prune -f` then rebuild |
| `docker: permission denied` | `/app` owned by root on EC2 | All `docker compose` commands must be prefixed with `sudo` |
| `run_log.parquet not found` on first run | File doesn't exist yet — expected | Handled automatically — pipeline creates it on first flush |
| EC2 public IP not reachable | IP changed after restart | Re-fetch from CF outputs (Step 3) |
| Stack status `ROLLBACK_COMPLETE` | Previous deploy failed | Delete manually: `aws cloudformation delete-stack` then re-deploy |

---

**Last Updated:** 2026-05-12
