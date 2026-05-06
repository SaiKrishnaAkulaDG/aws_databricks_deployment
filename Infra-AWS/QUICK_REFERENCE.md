# Credit Card Transactions Data Lake - Quick Reference

## 🚀 Quick Start (5 minutes)

### 1. Deploy Infrastructure

```bash
# Make script executable
chmod +x deploy-cc-lake.sh

# Create stack (automatic setup)
./deploy-cc-lake.sh create

# Or using AWS CLI directly
aws cloudformation create-stack \
  --stack-name cc-transactions-lake-stack \
  --template-body file://cf-cc-transactions-lake.yaml \
  --parameters ParameterKey=S3BucketName,ParameterValue=cc-transactions-lake-2026 \
  --capabilities CAPABILITY_NAMED_IAM
```

### 2. Connect to EC2

```bash
# Get IP from CloudFormation outputs
IP=$(aws cloudformation describe-stacks \
  --stack-name cc-transactions-lake-stack \
  --query 'Stacks[0].Outputs[?OutputKey==`EC2InstancePublicIP`].OutputValue' \
  --output text)

# SSH into instance
ssh -i cc-transactions-lake-key.pem ubuntu@$IP
```

### 3. Deploy Code (On EC2)

```bash
cd /app

# Clone from GitHub
git clone https://github.com/SaiKrishnaAkulaDG/aws_databricks_deployment.git .

# Install dependencies
pip3 install -r requirements.txt
```

### 4. Run Pipeline

```bash
# Historical pipeline (Docker Compose — recommended)
docker compose run --rm pipeline python -m pipeline.pipeline \
  --mode historical --start-date 2024-01-01 --end-date 2024-01-06

# Incremental pipeline (auto-detects next date from watermark)
docker compose run --rm pipeline python -m pipeline.pipeline --mode incremental
```

> **No manual directory setup needed** — the pipeline creates all silver/gold directories automatically.

### 5. Verify Results

```bash
# Set your S3 bucket name (from CloudFormation outputs)
S3_BUCKET_NAME="cc-transactions-lake-2026"

# Check S3
aws s3 ls s3://$S3_BUCKET_NAME/gold/ --recursive

# Or query locally
python3 -c "
import duckdb
con = duckdb.connect()
result = con.execute('SELECT COUNT(*) FROM read_parquet(\"/app/data/gold/daily_summary/data.parquet\")').fetchall()
print(f'Gold layer records: {result[0][0]}')
"
```

---

## 📊 Architecture Summary

```
Source Data (CSV)
       ↓
    Bronze Layer (Raw Parquet)
       ↓
    Silver Layer (Cleaned Parquet)
       ↓
    Gold Layer (Aggregated Parquet)
       ↓
    S3 Bucket Storage
```

**Layers:**
- **Bronze**: `/app/data/bronze/` - Raw ingested data
- **Silver**: `/app/data/silver/` - Cleaned, deduplicated data
- **Gold**: `/app/data/gold/` - Business-ready aggregates

---

## 🛠️ File Structure

```
/app/
├── source/                    # CSV source files (read-only)
├── data/                      # All pipeline outputs (synced to S3)
│   ├── bronze/                # Raw layer
│   │   ├── accounts/
│   │   ├── transactions/
│   │   └── transaction_codes/
│   ├── silver/                # Cleaned layer
│   │   ├── accounts/
│   │   ├── transactions/
│   │   ├── transaction_codes/
│   │   └── quarantine/
│   ├── gold/                  # Analytics layer
│   │   ├── daily_summary/
│   │   └── weekly_account_summary/
│   └── pipeline/              # Control plane
│       ├── control.parquet
│       ├── gold_weekly_control.parquet
│       └── run_log.parquet
├── pipeline/                  # Python pipeline code
│   ├── pipeline.py            # Main orchestrator (--mode historical/incremental)
│   ├── bronze_accounts.py
│   ├── bronze_transactions.py
│   ├── bronze_transaction_codes.py
│   ├── dbt_runner.py
│   ├── run_log.py
│   └── control_plane.py
└── dbt/                       # dbt models
    └── models/
        ├── silver/
        └── gold/
```

---

## 📋 Pipeline Execution

### Full Historical Pipeline

```bash
docker compose run --rm pipeline python -m pipeline.pipeline \
  --mode historical --start-date 2024-01-01 --end-date 2024-01-06
```

**Flow:**
1. Load Bronze layer (3 entity types)
2. Auto-create all required silver/gold directories
3. Promote silver_transaction_codes (once)
4. Promote Silver layer per date (accounts → transactions → quarantine)
5. Build Gold layer (aggregations)
6. Validate and advance watermark

### Incremental Pipeline

```bash
docker compose run --rm pipeline python -m pipeline.pipeline --mode incremental
```

**Flow:**
1. Check watermark (last processed date)
2. Identify new dates
3. Run Bronze → Silver → Gold for new dates
4. Update watermark

### Validate Execution

```bash
# Check control/watermark
python3 -c "
import duckdb
con = duckdb.connect()
control = con.execute('SELECT * FROM read_parquet(\"/app/data/pipeline/control.parquet\")').fetchall()
print('Last Processed Date:', control[0] if control else 'None')
"

# Check run log
python3 -c "
import duckdb
con = duckdb.connect()
logs = con.execute('SELECT * FROM read_parquet(\"/app/data/pipeline/run_log.parquet\") ORDER BY run_timestamp DESC LIMIT 5').fetchall()
for log in logs:
    print(log)
"
```

---

## 📤 Data Sync to S3

### Manual Sync

```bash
# Sync all data/ outputs to S3
aws s3 sync /app/data/bronze   s3://cc-transactions-lake-2026/bronze/
aws s3 sync /app/data/silver   s3://cc-transactions-lake-2026/silver/
aws s3 sync /app/data/gold     s3://cc-transactions-lake-2026/gold/
aws s3 sync /app/data/pipeline s3://cc-transactions-lake-2026/pipeline/
```

### Automatic (Via Script)

The `/app/run_pipeline.sh` script automatically:
1. Syncs source data from S3
2. Runs pipeline
3. Syncs results back to S3

---

## 🔍 Query Examples

### Using DuckDB (Local)

```python
import duckdb

con = duckdb.connect()

# Count records by layer
bronze = con.execute("""
  SELECT 'bronze' as layer, COUNT(*) as count
  FROM read_parquet('/app/data/bronze/transactions/**/*.parquet')
""").fetchall()

silver = con.execute("""
  SELECT 'silver' as layer, COUNT(*) as count  
  FROM read_parquet('/app/data/silver/transactions/**/*.parquet')
""").fetchall()

gold = con.execute("""
  SELECT 'gold' as layer, COUNT(*) as count
  FROM read_parquet('/app/data/gold/**/*.parquet')
""").fetchall()

for layer in [bronze, silver, gold]:
    print(f"{layer[0][0]}: {layer[0][1]:,} records")
```

### Using SQL Files

```bash
# Query gold layer
python3 << 'EOF'
import duckdb

con = duckdb.connect()

# Daily summary
result = con.execute("""
  SELECT 
    transaction_date,
    total_transactions,
    instore_transactions,
    online_transactions,
    total_signed_amount
  FROM read_parquet('/app/data/gold/daily_summary/data.parquet')
  ORDER BY transaction_date DESC
  LIMIT 5
""")

print(result.description)
for row in result:
    print(row)
EOF
```

---

## 📊 Monitoring & Health Checks

### Stack Status

```bash
# Check stack status
aws cloudformation describe-stacks \
  --stack-name cc-transactions-lake-stack \
  --query 'Stacks[0].[StackStatus,StackStatusReason]' \
  --output text

# Get stack events (last 10)
aws cloudformation describe-stack-events \
  --stack-name cc-transactions-lake-stack \
  --query 'StackEvents[0:10].[Timestamp,LogicalResourceId,ResourceStatus]' \
  --output table
```

### EC2 Instance Health

```bash
# Get instance status
aws ec2 describe-instance-status \
  --instance-ids i-xxxxx \
  --query 'InstanceStatuses[0].[InstanceStatus.Status,SystemStatus.Status]' \
  --output text

# Check disk space
ssh ubuntu@<IP> 'df -h'

# Check memory
ssh ubuntu@<IP> 'free -h'
```

### S3 Storage Usage

```bash
# Calculate total size
aws s3 ls s3://cc-transactions-lake-2026 --recursive --summarize --human-readable

# Count objects by layer
aws s3 ls s3://cc-transactions-lake-2026/bronze/ --recursive | wc -l
aws s3 ls s3://cc-transactions-lake-2026/silver/ --recursive | wc -l
aws s3 ls s3://cc-transactions-lake-2026/gold/ --recursive | wc -l
```

---

## 🆘 Troubleshooting

### Pipeline Fails

```bash
# Check logs on EC2
tail -f /var/log/pipeline-exec.log

# Run with verbose output
docker compose run --rm pipeline python -m pipeline.pipeline \
  --mode historical --start-date 2024-01-01 --end-date 2024-01-06 \
  2>&1 | tee /tmp/debug.log

# Check for missing files
ls -la /app/source/
```

### S3 Access Issues

```bash
# Verify IAM role
aws iam get-role-policy \
  --role-name cc-transactions-lake-ec2-role \
  --policy-name S3AccessPolicy

# Test from EC2
ssh ubuntu@<IP> 'aws s3 ls'
```

### Disk Space Full

```bash
# Check usage
df -h

# Clean temporary files
rm -rf /app/dbt/target/*
rm -rf /app/dbt/logs/*
rm -rf /tmp/*
```

### Out of Memory

```bash
# Check memory
free -h

# Reduce batch size in pipeline config or upgrade instance
# Modify: EC2_INSTANCE_TYPE parameter
```

---

## 🧹 Cleanup

### Delete Stack (Entire Infrastructure)

```bash
# Using script
./deploy-cc-lake.sh delete

# Or manual
aws s3 rm s3://cc-transactions-lake-2026 --recursive
aws cloudformation delete-stack --stack-name cc-transactions-lake-stack
```

### Clear S3 Data Only

```bash
# Keep infrastructure, clear data
aws s3 rm s3://cc-transactions-lake-2026/bronze   --recursive
aws s3 rm s3://cc-transactions-lake-2026/silver   --recursive
aws s3 rm s3://cc-transactions-lake-2026/gold     --recursive
aws s3 rm s3://cc-transactions-lake-2026/pipeline --recursive
```

### Clear EC2 Local Data

```bash
ssh -i cc-transactions-lake-key.pem ubuntu@<IP> << 'EOF'
rm -rf /app/data/bronze/* /app/data/silver/* /app/data/gold/*
rm -f /app/data/pipeline/control.parquet /app/data/pipeline/run_log.parquet
rm -rf /app/dbt/target/*
EOF
```

---

## 📚 Configuration Parameters

| Parameter | Default | Minimal | Recommended |
|-----------|---------|---------|-------------|
| Instance Type | t3.small | t3.micro | t3.small |
| EBS Volume | 20GB | 20GB | 30GB |
| S3 Bucket | cc-transactions-lake-2026 | - | global unique |
| Region | us-east-1 | any | your closest |

---

## ✅ Validation Checklist

After deployment, verify:

- [ ] CloudFormation stack status = CREATE_COMPLETE
- [ ] EC2 instance is running
- [ ] Can SSH into instance
- [ ] S3 bucket exists with bronze/, silver/, gold/ folders
- [ ] Python3 and pip installed on EC2
- [ ] DuckDB, pandas, boto3 libraries installed
- [ ] dbt installed and configured
- [ ] Docker Compose installed (`docker compose version` succeeds)
- [ ] Source CSV files available
- [ ] Pipeline executes without errors
- [ ] Output files created in bronze/, silver/, gold/
- [ ] Data synced to S3

---

## 📞 Support Commands

```bash
# Get all stack outputs
aws cloudformation describe-stacks \
  --stack-name cc-transactions-lake-stack \
  --query 'Stacks[0].Outputs' \
  --output table

# Get EC2 details
aws ec2 describe-instances \
  --filters "Name=tag:Name,Values=cc-transactions-lake-pipeline" \
  --query 'Reservations[0].Instances[0].[PublicIpAddress,PrivateIpAddress,State.Name]' \
  --output text

# Get IAM role details
aws iam get-role --role-name cc-transactions-lake-ec2-role

# Get CloudWatch logs
aws logs tail /aws/ec2/cc-transactions-lake --follow
```

---

**Last Updated:** April 28, 2026  
**Version:** 2.0 (Docker Compose, automatic directories, fixed ec2-user → ubuntu, fixed bucket names)  
**Status:** Production Ready
