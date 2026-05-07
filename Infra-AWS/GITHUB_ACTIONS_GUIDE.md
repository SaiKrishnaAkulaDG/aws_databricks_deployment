# GitHub Actions Workflows — Operations Guide

**Last updated:** 2026-05-07 (v2 — full end-to-end validation complete)  
**Repo:** `SaiKrishnaAkulaDG/aws_databricks_deployment`  
**Region:** `us-east-1`

---

## Overview

Three workflows manage the full lifecycle of the pipeline infrastructure:

| Workflow | Trigger | Purpose |
|----------|---------|---------|
| `deploy-infra.yml` | Manual | Create or update the CloudFormation stack |
| `run-pipeline.yml` | Daily 2 AM UTC + Manual | Start EC2, run pipeline, sync S3, stop EC2 |
| `teardown-infra.yml` | Manual (requires typing "DELETE") | Stop EC2, empty S3, delete CF stack |

---

## Required GitHub Secrets

Set at: **GitHub repo → Settings → Secrets and variables → Actions**

| Secret | Value | How to set |
|--------|-------|------------|
| `AWS_DEPLOY_ROLE_ARN` | `arn:aws:iam::065317679010:role/cc-transactions-lake-github-role` | Copy ARN from IAM console |
| `EC2_INSTANCE_ID` | e.g. `i-0c498606197a51825` | Get from CF stack output `EC2InstanceId` |
| `EC2_SSH_KEY` | Contents of `Infra-AWS/cc-transactions-lake-key.pem` | See command below |

### Set EC2_SSH_KEY from local PEM file
```bash
# From repo root — pipes file directly, avoids CRLF issues
gh secret set EC2_SSH_KEY < Infra-AWS/cc-transactions-lake-key.pem
```

> **Warning:** Do NOT copy-paste the PEM content manually into the GitHub UI — Windows line endings (CRLF) will corrupt the key and cause `error in libcrypto` on SSH.

---

## Workflow 1 — Deploy Infrastructure

**File:** `.github/workflows/deploy-infra.yml`  
**Trigger:** Manual (`workflow_dispatch`)

### What it does
1. Validates the CloudFormation template
2. Checks stack status:
   - `DOES_NOT_EXIST` → `create-stack`
   - `CREATE_COMPLETE` / `UPDATE_COMPLETE` → `update-stack`
   - `ROLLBACK_COMPLETE` → auto-deletes and recreates
   - No changes → prints "stack is up to date", skips wait
3. Waits for operation to complete
4. Prints stack outputs (EC2 IP, S3 paths, etc.)

### Trigger via CLI
```bash
gh workflow run deploy-infra.yml --ref main
```

### Trigger with custom parameters
```bash
gh workflow run deploy-infra.yml --ref main \
  -f s3_bucket_name=cc-transaction-databricks-datalake-2026 \
  -f ec2_instance_type=t3.micro \
  -f ebs_volume_size=5
```

### Get stack outputs after deploy
```bash
aws cloudformation describe-stacks \
  --stack-name cc-transactions-lake-stack \
  --query 'Stacks[0].Outputs' \
  --output table \
  --region us-east-1 \
  --profile DG4-Developer-065317679010
```

---

## Workflow 2 — Run Pipeline

**File:** `.github/workflows/run-pipeline.yml`  
**Trigger:** Schedule (daily 2 AM UTC) + Manual

### What it does
1. Starts EC2 instance
2. Waits 60s for SSH to be ready
3. Pulls latest code on EC2 (`git pull origin main`)
4. Creates `/app/.env` from `.env.example` if absent (first run on fresh EC2)
5. Runs pipeline (incremental or historical)
6. Verifies outputs (watermark, counts)
7. Syncs data to S3 (auto-detects `aws` CLI path, installs if missing)
8. Stops EC2 (`if: always()` — runs even on failure)

### Trigger incremental (default)
```bash
gh workflow run run-pipeline.yml --ref main -f mode=incremental
```

### Trigger historical
```bash
gh workflow run run-pipeline.yml --ref main \
  -f mode=historical \
  -f start_date=2024-01-01 \
  -f end_date=2024-01-06
```

### Check run status
```bash
# List recent runs
gh run list --workflow=run-pipeline.yml --limit=5

# Watch a specific run
gh run view <RUN_ID>

# Get logs for failed steps only
gh run view --log-failed --job=<JOB_ID>
```

---

## Workflow 3 — Teardown Infrastructure

**File:** `.github/workflows/teardown-infra.yml`  
**Trigger:** Manual only (requires typing `DELETE` to confirm)

### What it does
1. Checks stack exists
2. Stops EC2 instance if running
3. Purges all S3 object versions and delete markers (handles versioned bucket)
4. Deletes CloudFormation stack
5. Waits for deletion to complete

### Trigger via CLI
```bash
gh workflow run teardown-infra.yml --ref main -f confirm=DELETE
```

> **Note:** This destroys all AWS resources. The S3 data and EC2 instance will be gone. Re-run `deploy-infra` + `run-pipeline` to restore.

---

## IAM Role — cc-transactions-lake-github-role

The GitHub Actions OIDC role used by all workflows.

| Item | Value |
|------|-------|
| Role name | `cc-transactions-lake-github-role` |
| Role ARN | `arn:aws:iam::065317679010:role/cc-transactions-lake-github-role` |
| Policy name | `cc-transactions-lake-github-policy` |
| Policy version | `v6` (current) |
| OIDC trust | `repo:SaiKrishnaAkulaDG/aws_databricks_deployment:*` |

### Permissions granted (v6)

| Service | Actions |
|---------|---------|
| **EC2** | RunInstances, StartInstances, StopInstances, TerminateInstances, Create/DeleteSecurityGroup, AuthorizeSecurityGroupIngress, CreateTags, Describe* |
| **CloudFormation** | CreateStack, UpdateStack, DeleteStack, DescribeStacks, DescribeStackEvents, ValidateTemplate, GetTemplate, ListStackResources |
| **S3** | CRUD + versioning + tagging on `cc-transaction-databricks-datalake-2026` |
| **IAM** | Create/Delete Role, InstanceProfile, AttachPolicy — scoped to `cc-transactions-lake-*` |
| **CloudWatch Logs** | CreateLogGroup, DeleteLogGroup, DescribeLogGroups, PutRetentionPolicy |

### Update policy (if new permissions needed)
```bash
# Get current policy document first
aws iam get-policy-version \
  --policy-arn arn:aws:iam::065317679010:policy/cc-transactions-lake-github-policy \
  --version-id v6 \
  --profile DG4-Developer-065317679010

# Create new version (max 5 versions — delete old ones if needed)
aws iam delete-policy-version \
  --policy-arn arn:aws:iam::065317679010:policy/cc-transactions-lake-github-policy \
  --version-id v3 \
  --profile DG4-Developer-065317679010

aws iam create-policy-version \
  --policy-arn arn:aws:iam::065317679010:policy/cc-transactions-lake-github-policy \
  --policy-document file:///tmp/updated-policy.json \
  --set-as-default \
  --profile DG4-Developer-065317679010
```

---

## Common Operations

### Get EC2 public IP after deploy
```bash
aws cloudformation describe-stacks \
  --stack-name cc-transactions-lake-stack \
  --region us-east-1 \
  --profile DG4-Developer-065317679010 \
  --query 'Stacks[0].Outputs[?OutputKey==`EC2InstancePublicIP`].OutputValue' \
  --output text
```

### SSH into EC2
```bash
ssh -i Infra-AWS/cc-transactions-lake-key.pem ubuntu@<EC2_IP>
```

### Start / Stop EC2 manually
```bash
# Start
aws ec2 start-instances \
  --instance-ids i-0c498606197a51825 \
  --region us-east-1 \
  --profile DG4-Developer-065317679010

# Stop
aws ec2 stop-instances \
  --instance-ids i-0c498606197a51825 \
  --region us-east-1 \
  --profile DG4-Developer-065317679010
```

### Run pipeline manually on EC2
```bash
# Historical
sudo docker compose run --rm pipeline \
  python -m pipeline.pipeline --mode historical \
  --start-date 2024-01-01 --end-date 2024-01-06

# Incremental
sudo docker compose run --rm pipeline \
  python -m pipeline.pipeline --mode incremental
```

### Sync data to S3 manually
```bash
# Find aws CLI path first (varies by instance)
which aws || echo "/home/ubuntu/.local/bin/aws"

aws s3 sync /app/data/bronze   s3://cc-transaction-databricks-datalake-2026/bronze/
aws s3 sync /app/data/silver   s3://cc-transaction-databricks-datalake-2026/silver/
aws s3 sync /app/data/gold     s3://cc-transaction-databricks-datalake-2026/gold/
aws s3 sync /app/data/pipeline s3://cc-transaction-databricks-datalake-2026/pipeline/
```

### Update EC2_INSTANCE_ID secret after redeploy
Every `deploy-infra` run creates a new EC2 instance with a new ID. Update the secret after each deploy:
```bash
# Get new instance ID from CF outputs
NEW_ID=$(aws cloudformation describe-stacks \
  --stack-name cc-transactions-lake-stack \
  --region us-east-1 --profile DG4-Developer-065317679010 \
  --query 'Stacks[0].Outputs[?OutputKey==`EC2InstanceId`].OutputValue' \
  --output text)

echo $NEW_ID | gh secret set EC2_INSTANCE_ID
```

---

## Bugs Fixed (2026-05-07 Session)

| # | Workflow | Problem | Fix |
|---|----------|---------|-----|
| 1 | deploy-infra | `create-stack` failed if stack already existed | Added create-or-update logic with stack status check |
| 2 | deploy-infra | `ROLLBACK_COMPLETE` blocked re-deploy | Auto-delete and recreate on ROLLBACK_COMPLETE |
| 3 | run-pipeline | SSH key `error in libcrypto` | Re-set `EC2_SSH_KEY` secret via `gh secret set < file` — copy-paste from UI adds CRLF |
| 4 | run-pipeline | `EC2_INSTANCE_ID` stale after redeploy | Updated secret to new instance ID; added note in guide |
| 5 | run-pipeline | `.env` missing on fresh EC2 | Added step to copy `.env.example → .env` if absent |
| 6 | run-pipeline | Source CSVs missing on EC2 | Fixed corrupt `.gitignore`, committed all `source/` CSV files |
| 7 | run-pipeline | `aws` CLI not found on new EC2 | Workflow now auto-detects path via `which aws`, installs via pip if missing |
| 8 | run-pipeline | EC2 had stale code | Added `git pull origin main` step before every pipeline run |
| 9 | teardown-infra | `DELETE_FAILED` — S3 versioned objects | Replaced `s3 rm --recursive` with `s3api list-object-versions` + `delete-objects` |
| 10 | All | Missing IAM permissions (6 actions) | Policy updated v1→v6: `UpdateStack`, `TerminateInstances`, `DeleteLogGroup`, `DeleteSecurityGroup`, `TagResource`, `RunInstances`, `CreateTags` |

## Validated Pipeline Results (2026-05-07)

| Run | Mode | Dates | Result |
|-----|------|-------|--------|
| run-pipeline | Historical | 2024-01-01 → 2024-01-06 | ✓ Passed — Bronze/Silver/Gold written, S3 synced |
| run-pipeline | Incremental | 2024-01-07 | ✓ Passed — Watermark advanced to 2024-01-07, S3 synced |
