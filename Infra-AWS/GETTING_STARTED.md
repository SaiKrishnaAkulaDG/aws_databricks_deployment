# Getting Started — CC Transactions Data Lake on AWS

**For new users.** This guide walks you through everything needed to deploy the pipeline to AWS and run it end-to-end using Claude Code.

---

## What This System Does

A batch data pipeline that ingests daily credit card transaction CSVs and produces clean, aggregated analytics data stored in S3. It follows the Medallion architecture:

```
Source CSVs → Bronze (raw) → Silver (cleaned) → Gold (aggregated) → S3
```

Runs on an EC2 t3.micro instance using Docker Compose. Stop the instance after each run to minimise cost (~$0.002 per run).

---

## Part 1 — Prerequisites

Complete every item in this section **before** opening Claude Code.

---

### 1.1 AWS Account & Permissions

You need an AWS account with permissions to:
- Create/delete CloudFormation stacks
- Launch EC2 instances
- Create S3 buckets
- Create IAM roles

Ask your AWS admin if you are unsure. The deploying identity (user or role) needs at minimum:
`cloudformation:*`, `ec2:*`, `s3:*`, `iam:CreateRole`, `iam:CreateInstanceProfile`, `iam:PassRole`

---

### 1.2 AWS CLI

**Install:**
- Windows: https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2-windows.html
- Mac: `brew install awscli`
- Linux: `sudo apt install awscli`

**Configure your profile:**
```bash
aws configure --profile <your-profile-name>
# Enter: AWS Access Key ID, Secret Access Key, region (us-east-1), output (json)
```

**Verify:**
```bash
aws sts get-caller-identity --profile <your-profile-name>
# Expected: returns your Account ID and ARN
```

> If your org uses AWS SSO, run `aws sso login --profile <your-profile-name>` instead.

---

### 1.3 Git

**Install:** https://git-scm.com/downloads

**Verify:**
```bash
git --version
# Expected: git version 2.x.x
```

---

### 1.4 SSH Client

- **Windows 11/10**: Built-in — open PowerShell or Command Prompt and type `ssh`
- **Mac/Linux**: Built-in

**Verify:**
```bash
ssh -V
# Expected: OpenSSH_x.x
```

---

### 1.5 Claude Code

**Install:**
```bash
npm install -g @anthropic/claude-code
```

**Or download:** https://claude.ai/code

**Verify:**
```bash
claude --version
```

---

### 1.6 Clone the Repository

```bash
git clone https://github.com/SaiKrishnaAkulaDG/aws_databricks_deployment.git
cd aws_databricks_deployment
```

---

### 1.7 Create EC2 Key Pair

This is required **before** deploying — CloudFormation will fail without it.

```bash
aws ec2 create-key-pair \
  --key-name cc-transactions-lake-key \
  --region us-east-1 \
  --profile <your-profile-name> \
  --query 'KeyMaterial' \
  --output text > Infra-AWS/cc-transactions-lake-key.pem
```

**Restrict permissions (required for SSH to work):**

Windows (PowerShell):
```powershell
icacls "Infra-AWS\cc-transactions-lake-key.pem" /inheritance:r /grant:r "$($env:USERNAME):(R)"
```

Mac/Linux:
```bash
chmod 600 Infra-AWS/cc-transactions-lake-key.pem
```

**Verify:**
```bash
aws ec2 describe-key-pairs \
  --key-names cc-transactions-lake-key \
  --region us-east-1 \
  --profile <your-profile-name> \
  --query 'KeyPairs[0].KeyName' \
  --output text
# Expected: cc-transactions-lake-key
```

---

### 1.8 Update Profile Name in Deploy Script

Open `Infra-AWS/deploy-cc-lake.sh` — the default AWS profile is `DG4-Developer-065317679010`.

If your profile name is different, set it before running:
```bash
export AWS_PROFILE=<your-profile-name>
```

Or pass it inline:
```bash
AWS_PROFILE=<your-profile-name> bash deploy-cc-lake.sh create
```

---

### Prerequisites Checklist

Before proceeding to Part 2, confirm all of these:

- [ ] AWS CLI installed and `aws sts get-caller-identity` returns your account
- [ ] Git installed and repo cloned
- [ ] SSH client works (`ssh -V`)
- [ ] Claude Code installed
- [ ] Key pair `cc-transactions-lake-key` created in `us-east-1`
- [ ] `Infra-AWS/cc-transactions-lake-key.pem` exists with restricted permissions
- [ ] You know your AWS profile name

---

## Part 2 — Deploy and Run Using Claude Code

Open Claude Code from the repository root:

```bash
cd aws_databricks_deployment
claude
```

Then paste the prompts below in order.

---

### Step 1 — Deploy the Infrastructure

Paste this prompt into Claude Code:

```
Deploy the CloudFormation stack for the credit card transactions data lake to AWS.
Use profile <your-profile-name>, region us-east-1.
Run deploy-cc-lake.sh create from the Infra-AWS/ folder.
Wait for the stack to reach CREATE_COMPLETE and show me the EC2 instance ID and public IP.
```

**What Claude does:** Runs `deploy-cc-lake.sh create`, waits ~10-15 minutes for CloudFormation to provision EC2, S3, and IAM role, then prints the outputs.

**Expected result:** Stack status `CREATE_COMPLETE`, EC2 instance ID and IP printed.

---

### Step 2 — Wait for EC2 Initialization

The EC2 instance runs a startup script (UserData) that installs Docker, clones the repo, and installs Python dependencies. This takes **2-3 minutes** after the stack is created.

Paste this prompt:

```
Wait for EC2 UserData initialization to complete, then SSH into the instance
using Infra-AWS/cc-transactions-lake-key.pem and verify:
- /var/log/user-data.log ends with "initialization completed"
- docker compose version works
- ls /app/ shows the pipeline files
```

---

### Step 3 — Upload Source CSV Files

The source CSVs are not committed to the repo (read-only data). Upload them to EC2:

```
Upload all CSV files from the local source/ folder to /app/source/ on the EC2 instance
using scp with key Infra-AWS/cc-transactions-lake-key.pem.
Verify 15 files are present in /app/source/.
```

---

### Step 4 — Run the Pipeline

```
SSH into the EC2 instance using Infra-AWS/cc-transactions-lake-key.pem.
Run the historical pipeline for 2024-01-01 to 2024-01-06.
Then run incremental mode for day 7.
After both complete, verify:
- Watermark = 2024-01-07
- Bronze transactions = 35
- Silver transactions = 28
- Gold daily rows = 7
- Gold weekly rows = 3
```

---

### Step 5 — Sync to S3

```
Sync all pipeline outputs from EC2 to S3.
Sync bronze, silver, gold, and pipeline folders to s3://cc-transaction-databricks-datalake-2026.
Use /home/ubuntu/.local/bin/aws on the EC2 instance.
Verify the gold/ and pipeline/ folders appear in S3.
```

---

### Step 6 — Stop the Instance

```
Stop the EC2 instance to avoid idle charges.
```

---

## Part 3 — Day-to-Day Operations

For subsequent runs (stack already deployed, instance stopped):

```
Start EC2 instance i-<your-instance-id> in us-east-1 using profile <your-profile-name>.
Get the public IP, SSH in using Infra-AWS/cc-transactions-lake-key.pem.
Clear existing pipeline data, run historical 2024-01-01 to 2024-01-06,
then run incremental for day 7.
Verify watermark = 2024-01-07 and gold daily rows = 7.
Sync all data to s3://cc-transaction-databricks-datalake-2026.
Stop the instance.
```

---

## Part 4 — Teardown

To delete all AWS resources:

```
Delete the CloudFormation stack cc-transactions-lake-stack in us-east-1
using profile <your-profile-name>. Empty the S3 bucket first if needed.
```

> **Warning:** This deletes the EC2 instance, S3 bucket contents, and IAM role permanently.

---

## Reference

| Resource | Value |
|----------|-------|
| Stack name | `cc-transactions-lake-stack` |
| S3 bucket | `cc-transaction-databricks-datalake-2026` |
| EC2 type | t3.micro (2 vCPU, 1GB RAM) |
| Region | `us-east-1` |
| EC2 user | `ubuntu` |
| App directory | `/app` |
| AWS CLI on EC2 | `/home/ubuntu/.local/bin/aws` |
| Key pair name | `cc-transactions-lake-key` |
| Key file | `Infra-AWS/cc-transactions-lake-key.pem` |

| Cost | Amount |
|------|--------|
| Instance running | ~$0.0104/hr |
| Instance stopped | ~$0.0004/hr |
| Single pipeline run | ~$0.002 |
| S3 storage (7 days data) | ~$0.001/month |

---

## Common Issues

| Problem | Fix |
|---------|-----|
| `aws: command not found` on EC2 | Use `/home/ubuntu/.local/bin/aws` |
| `Permission denied` writing to `/app` | Use `sudo docker compose run ...` |
| `.env not found` error | `sudo cp /app/.env.example /app/.env` |
| EC2 IP changed after restart | Re-fetch from CloudFormation outputs |
| Source CSVs missing | Re-upload via `scp` (Step 3) |
| Stack creation fails on S3 bucket | Bucket name already taken — change `S3_BUCKET_NAME` in `deploy-cc-lake.sh` |
| SSH `Permission denied (publickey)` | Check `.pem` file permissions (Step 1.7) |

---

**Last Updated:** 2026-05-06  
**Repo:** https://github.com/SaiKrishnaAkulaDG/aws_databricks_deployment
