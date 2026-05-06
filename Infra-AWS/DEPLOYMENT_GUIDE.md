# Credit Card Transactions Data Lake - AWS Deployment Guide

## Overview

This guide provides step-by-step instructions to deploy the credit card transactions data lake pipeline on AWS using CloudFormation. The pipeline implements a three-layer data architecture:

- **Bronze Layer**: Raw ingested data from source systems
- **Silver Layer**: Cleaned, validated, and deduplicated data
- **Gold Layer**: Aggregated, business-ready analytics data

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    AWS CloudFormation Stack                 │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌──────────────────────────────────────────────────────┐   │
│  │          S3 Data Lake Bucket                         │   │
│  │  ├── bronze/    (Raw layer)                          │   │
│  │  ├── silver/    (Cleaned layer)                      │   │
│  │  └── gold/      (Analytics layer)                    │   │
│  └──────────────────────────────────────────────────────┘   │
│           ▲                                                   │
│           │ (Read/Write)                                     │
│  ┌──────────────────────────────────────────────────────┐   │
│  │     EC2 Instance (t3.small minimum)                  │   │
│  │  ├── Python 3 Runtime                               │   │
│  │  ├── DuckDB (embedded analytics DB)                 │   │
│  │  ├── dbt (data transformation)                      │   │
│  │  └── Pipeline Scripts                               │   │
│  │      └── pipeline.py (--mode historical/incremental) │   │
│  └──────────────────────────────────────────────────────┘   │
│           │                                                   │
│  ┌──────────────────────────────────────────────────────┐   │
│  │     IAM Role (EC2 → S3 Access)                      │   │
│  │  └── S3 Read/Write permissions                      │   │
│  └──────────────────────────────────────────────────────┘   │
│                                                               │
└─────────────────────────────────────────────────────────────┘
```

## Prerequisites

1. **AWS Account**: Active AWS account with appropriate permissions
2. **AWS CLI**: Configured with credentials or SSO profile
3. **EC2 Key Pair**: Must exist in `us-east-1` before deploying
   ```bash
   aws ec2 create-key-pair --key-name cc-transactions-lake-key \
     --region us-east-1 --profile DG4-Developer-065317679010 \
     --query 'KeyMaterial' --output text > cc-transactions-lake-key.pem
   chmod 600 cc-transactions-lake-key.pem
   ```
4. **AWS CloudFormation**: Service enabled in your region

## Step 1: Prepare the CloudFormation Template

The template has been created: `cf-cc-transactions-lake.yaml`

**Key Features:**
- Minimal EC2 instance (t3.small or smaller)
- 20GB gp3 EBS volume
- S3 bucket with three folder structure (bronze/silver/gold)
- IAM role with S3 and CloudWatch permissions
- Security group with SSH access (port 22)

## Step 2: Deploy the Stack

### Option A: Using AWS CLI

```bash
# Set parameters
STACK_NAME="cc-transactions-lake-stack"
S3_BUCKET_NAME="cc-transaction-databricks-datalake-2026"  # Must be globally unique
REGION="us-east-1"

# Create the stack
aws cloudformation create-stack \
  --stack-name $STACK_NAME \
  --template-body file://cf-cc-transactions-lake.yaml \
  --parameters \
    ParameterKey=S3BucketName,ParameterValue=$S3_BUCKET_NAME \
    ParameterKey=EC2InstanceType,ParameterValue=t3.micro \
    ParameterKey=EBSVolumeSize,ParameterValue=5 \
  --capabilities CAPABILITY_NAMED_IAM \
  --region $REGION

# Wait for stack creation (10-15 minutes)
aws cloudformation wait stack-create-complete \
  --stack-name $STACK_NAME \
  --region $REGION

# Get stack outputs
aws cloudformation describe-stacks \
  --stack-name $STACK_NAME \
  --query 'Stacks[0].Outputs' \
  --region $REGION
```

### Option B: Using AWS Console

1. Go to CloudFormation console
2. Click "Create Stack" → "With new resources"
3. Upload the template file: `cf-cc-transactions-lake.yaml`
4. Fill in stack details:
   - Stack name: `cc-transactions-lake-stack`
   - S3 Bucket Name: `cc-transaction-databricks-datalake-2026`
   - EC2 Instance Type: `t3.small`
   - EBS Volume Size: `20`
5. Click "Create Stack"
6. Wait for status "CREATE_COMPLETE" (10-15 minutes)

## Step 3: Set Up the Pipeline on EC2

Once the stack is created, connect to the EC2 instance:

```bash
# Get instance IP
INSTANCE_IP=$(aws cloudformation describe-stacks \
  --stack-name $STACK_NAME \
  --region $REGION \
  --query 'Stacks[0].Outputs[?OutputKey==`EC2InstancePublicIP`].OutputValue' \
  --output text)

# SSH into the instance
ssh -i cc-transactions-lake-key.pem ubuntu@$INSTANCE_IP
```

### On the EC2 Instance:

```bash
# Navigate to app directory
cd /app

# Clone or upload the codebase
# Option 1: Clone from GitHub (recommended)
git clone https://github.com/SaiKrishnaAkulaDG/aws_databricks_deployment.git .

# Option 2: From S3 (if you uploaded the code there)
# aws s3 cp s3://your-code-bucket/credit-card-transactions-lake.zip . && \
#   unzip credit-card-transactions-lake.zip

# Install dependencies (if not done by user data)
python3 -m pip install -r requirements.txt

# Verify Docker Compose
docker compose version

# Verify installations
python3 -c "import duckdb; print('DuckDB OK')"
python3 -c "import dbt; print('dbt OK')"
aws --version
```

## Step 4: Prepare Source Data

### Create sample source CSV files in S3:

```bash
# Create local source data files with proper structure
mkdir -p /app/source

# Upload to S3 for syncing
aws s3 sync /app/source s3://cc-transaction-databricks-datalake-2026/source/
```

**Expected source file structure:**
```
source/
├── accounts_2024-01-01.csv
├── accounts_2024-01-02.csv
├── ...
├── transactions_2024-01-01.csv
├── transactions_2024-01-02.csv
├── ...
└── transaction_codes.csv
```

## Step 5: Run the Pipeline

### Option A: Full Historical Pipeline

```bash
# Run for date range 2024-01-01 to 2024-01-06 (Docker Compose — recommended)
docker compose run --rm pipeline python -m pipeline.pipeline \
  --mode historical \
  --start-date 2024-01-01 \
  --end-date 2024-01-06
```

> **Directories are created automatically** — no `mkdir` needed before running.

**Expected Output:**
- Bronze layer: `/app/data/bronze/` (partitioned by date)
- Silver layer: `/app/data/silver/` (cleaned data, including date-partitioned transactions)
- Gold layer: `/app/data/gold/` (aggregated summaries)
- Watermark advances to `--end-date`

### Option B: Incremental Pipeline

```bash
# Auto-detects next date from watermark and processes it
docker compose run --rm pipeline python -m pipeline.pipeline --mode incremental
```

### Option C: Using the Shell Script

```bash
# Execute the run_pipeline.sh script
/app/run_pipeline.sh
```

This script:
1. Syncs source data from S3
2. Runs the pipeline
3. Syncs results back to S3

## Step 6: Verify Pipeline Execution

### Check Local Files

```bash
# Verify directory structure
ls -la /app/data/bronze/
ls -la /app/data/silver/
ls -la /app/data/gold/

# Check file sizes
du -sh /app/data/bronze/* /app/data/silver/* /app/data/gold/*

# View parquet file structure
python3 << 'EOF'
import duckdb

con = duckdb.connect()

# Check bronze layer
print("BRONZE LAYER:")
for table in con.execute("SELECT * FROM read_dir('/app/data/bronze/')").fetchall():
    print(f"  {table}")

# Check silver layer
print("\nSILVER LAYER:")
for table in con.execute("SELECT * FROM read_dir('/app/data/silver/')").fetchall():
    print(f"  {table}")

# Check gold layer
print("\nGOLD LAYER:")
for table in con.execute("SELECT * FROM read_dir('/app/data/gold/')").fetchall():
    print(f"  {table}")
EOF
```

### Check S3 Sync

```bash
# Sync data/ outputs to S3
aws s3 sync /app/data/bronze   s3://cc-transaction-databricks-datalake-2026/bronze/
aws s3 sync /app/data/silver   s3://cc-transaction-databricks-datalake-2026/silver/
aws s3 sync /app/data/gold     s3://cc-transaction-databricks-datalake-2026/gold/
aws s3 sync /app/data/pipeline s3://cc-transaction-databricks-datalake-2026/pipeline/

# List S3 contents
aws s3 ls s3://cc-transaction-databricks-datalake-2026/bronze/   --recursive
aws s3 ls s3://cc-transaction-databricks-datalake-2026/silver/   --recursive
aws s3 ls s3://cc-transaction-databricks-datalake-2026/gold/     --recursive
aws s3 ls s3://cc-transaction-databricks-datalake-2026/pipeline/ --recursive

# Verify file counts
aws s3 ls s3://cc-transaction-databricks-datalake-2026/bronze/   --recursive | wc -l
aws s3 ls s3://cc-transaction-databricks-datalake-2026/silver/   --recursive | wc -l
aws s3 ls s3://cc-transaction-databricks-datalake-2026/gold/     --recursive | wc -l
aws s3 ls s3://cc-transaction-databricks-datalake-2026/pipeline/ --recursive | wc -l
```

### Validate Data Quality

```bash
# Run validation against the pipeline outputs
python3 << 'EOF'
import duckdb
import json

con = duckdb.connect()

# Query gold layer summaries
gold_summary = con.execute("""
  SELECT COUNT(*) as record_count, 
         COUNT(DISTINCT date) as unique_dates
  FROM read_parquet('/app/data/gold/daily_summary/data.parquet')
""").fetchall()

print("Gold Layer Daily Summary:")
print(f"  Records: {gold_summary[0][0]}")
print(f"  Date Range: {gold_summary[0][1]} days")

# Query silver layer
silver_stats = con.execute("""
  SELECT 'transactions' as layer, COUNT(*) as count
  FROM read_parquet('/app/data/silver/transactions/**/*.parquet')
  UNION ALL
  SELECT 'accounts' as layer, COUNT(*) as count
  FROM read_parquet('/app/data/silver/accounts/**/*.parquet')
""").fetchall()

print("\nSilver Layer Statistics:")
for row in silver_stats:
    print(f"  {row[0]}: {row[1]} records")
EOF
```

### View CloudWatch Logs

```bash
# Get recent logs
aws logs tail /aws/ec2/cc-transactions-lake --follow
```

## Step 7: Monitor and Maintain

### Stop Instance After Each Run (Cost Control)

```bash
# Stop the instance when pipeline is done — pay EBS-only rate (~$0.01/day) instead of compute
INSTANCE_ID=$(aws cloudformation describe-stacks \
  --stack-name $STACK_NAME \
  --query 'Stacks[0].Outputs[?OutputKey==`EC2InstanceId`].OutputValue' \
  --output text)
aws ec2 stop-instances --instance-ids $INSTANCE_ID --region $REGION

# Start it again before the next run
aws ec2 start-instances --instance-ids $INSTANCE_ID --region $REGION
# Wait ~60s for instance to be ready, then SSH in
```

### Set Up Scheduled Pipeline Execution

```bash
# Add cron job on EC2 to run pipeline daily at 2 AM and stop instance after
crontab -e

# Run pipeline then stop instance to minimise cost
0 2 * * * docker compose -f /app/docker-compose.yml run --rm pipeline \
  python -m pipeline.pipeline --mode incremental \
  >> /var/log/pipeline-exec.log 2>&1 && \
  aws ec2 stop-instances \
  --instance-ids $(curl -s http://169.254.169.254/latest/meta-data/instance-id) \
  --region us-east-1
```

### Monitor S3 Storage

```bash
# Calculate total size
aws s3 ls s3://cc-transaction-databricks-datalake-2026 --recursive \
  --summarize \
  --human-readable | grep "Total Size"
```

### Access Data with DuckDB

```bash
# On EC2, query the data directly
python3 << 'EOF'
import duckdb

con = duckdb.connect()

# Query bronze raw data
result = con.execute("""
  SELECT COUNT(*) as transactions
  FROM read_parquet('/app/data/bronze/transactions/**/*.parquet')
""").fetchall()

print(f"Total transactions in bronze: {result[0][0]}")

# Query gold layer
result = con.execute("""
  SELECT transaction_date, total_transactions, total_signed_amount
  FROM read_parquet('/app/data/gold/daily_summary/data.parquet')
  ORDER BY transaction_date DESC
  LIMIT 5
""").fetchall()

print("\nLatest Gold Layer Records:")
for row in result:
    print(f"  Date: {row[0]}, Count: {row[1]}, Amount: {row[2]}")
EOF
```

## Step 8: Clean Up (When Needed)

### Delete the CloudFormation Stack

```bash
# Delete stack (removes EC2, S3 bucket contents, IAM role)
aws cloudformation delete-stack \
  --stack-name $STACK_NAME \
  --region $REGION

# Wait for deletion
aws cloudformation wait stack-delete-complete \
  --stack-name $STACK_NAME \
  --region $REGION
```

**Note:** S3 buckets with data cannot be deleted. To force deletion:

```bash
# Empty S3 bucket
aws s3 rm s3://cc-transaction-databricks-datalake-2026 --recursive

# Then delete stack
aws cloudformation delete-stack --stack-name $STACK_NAME
```

## Troubleshooting

### Issue: Pipeline fails with S3 access errors

**Solution:**
```bash
# Verify IAM role permissions
aws iam get-role-policy \
  --role-name cc-transactions-lake-ec2-role \
  --policy-name S3AccessPolicy

# Test S3 access from EC2
aws s3 ls s3://cc-transaction-databricks-datalake-2026/
```

### Issue: Disk space issues

**Solution:**
```bash
# Check disk usage
df -h

# Clean up temporary files
rm -rf /app/dbt/target/*
rm -rf /app/dbt/logs/*

# If needed, increase EBS volume (requires downtime)
# Create new CF stack with larger EBSVolumeSize parameter
```

### Issue: Pipeline execution times out

**Solution:**
```bash
# Run with verbose logging
docker compose run --rm pipeline python -m pipeline.pipeline \
  --mode historical \
  --start-date 2024-01-01 \
  --end-date 2024-01-06 \
  2>&1 | tee /var/log/pipeline-verbose.log

# Check if data source files exist
ls -la /app/source/
```

## Performance Notes

- **t3.micro instance**: Suitable for 1-7 days of data processing (1GB RAM, ~400MB peak usage)
- **Processing time**: ~10-15 minutes for 7 days of sample data
- **Storage**: 5GB EBS sufficient for pipeline code + 7-day sample data (~50MB used)
- **Cost**: ~$0.002 per pipeline run; ~$0.01/day when stopped (EBS only)
- **⚠ Stop instance after each run** — do not leave it running idle
- **For larger datasets**: Upgrade EBS to 10GB; upgrade to t3.small only if memory exhaustion occurs

## Security Best Practices

1. **SSH Key Management**
   ```bash
   chmod 600 cc-transactions-lake-key.pem
   ```

2. **S3 Encryption**
   - The template enables S3 versioning
   - Consider enabling server-side encryption (SSE-S3 or SSE-KMS)

3. **IAM Least Privilege**
   - The EC2 role has minimal required permissions
   - No public internet access to S3 bucket (blocked by default)

4. **Network Security**
   - SSH access restricted to port 22; consider restricting CidrIp to your IP in production

## Outputs Reference

After stack creation, you'll have these outputs:

| Output | Description | Value |
|--------|-------------|-------|
| S3BucketName | Data lake bucket name | cc-transaction-databricks-datalake-2026 |
| BronzePath | S3 location for raw data | s3://cc-transaction-databricks-datalake-2026/bronze/ |
| SilverPath | S3 location for cleaned data | s3://cc-transaction-databricks-datalake-2026/silver/ |
| GoldPath | S3 location for analytics | s3://cc-transaction-databricks-datalake-2026/gold/ |
| EC2InstanceId | EC2 instance identifier | i-xxxxx |
| EC2InstancePublicIP | Public IP (informational) | xxx.xxx.xxx.xxx |
| EC2RoleArn | IAM role for permissions | arn:aws:iam::xxxxx:role/xxxxx |

## Next Steps

1. **Schedule Automation**: Set up Lambda + EventBridge for daily pipeline execution
2. **Add Monitoring**: Configure CloudWatch alarms for pipeline failures
3. **Data Analysis**: Query gold layer with tools like Athena or QuickSight
4. **Version Control**: Track pipeline changes with git
5. **Documentation**: Maintain runbooks for operational procedures

## Support

For issues or questions:
1. Check CloudFormation stack events for deployment errors
2. Review EC2 user data logs: `/var/log/user-data.log`
3. Check pipeline execution logs: `/var/log/pipeline-exec.log`
4. Review CloudWatch logs: `/aws/ec2/cc-transactions-lake`
