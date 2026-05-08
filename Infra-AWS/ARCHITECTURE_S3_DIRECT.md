# AWS Deployment Architecture — S3-Direct Pipeline

**Branch:** `feature/s3-direct-writes`  
**Stack:** `cc-transactions-lake-stack` | **Region:** `us-east-1` | **Account:** `065317679010`  
**Last Updated:** 2026-05-08 (Session 4 — IMDS credential migration complete)

> This document covers the **infrastructure and deployment architecture** for the S3-direct-writes pipeline.  
> For the pipeline data architecture (Medallion layers, design decisions, invariants) see `docs/ARCHITECTURE.md`.

---

## 1. System Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│  GitHub Actions                                                          │
│                                                                          │
│  run-pipeline.yml                                                        │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │ 1. Start EC2                                                      │   │
│  │ 2. SSH → git pull                                                 │   │
│  │ 3. SSH → overwrite .env from .env.example                        │   │
│  │ 4. SSH → docker builder prune + docker compose build             │   │
│  │ 5. SSH → docker compose run pipeline (historical or incremental) │   │
│  │ 6. SSH → verify outputs (query S3 via DuckDB)                    │   │
│  │ 7. Stop EC2 (always, even on failure)                            │   │
│  └──────────────────────────────────────────────────────────────────┘   │
│         │ OIDC                                                           │
│         ▼                                                                │
│  IAM Role: cc-transactions-lake-github-role                             │
└─────────────────────────────────────────────────────────────────────────┘
         │ SSH + AWS API
         ▼
┌────────────────────────────────────────────────────────────────────────┐
│  EC2 — t3.micro (i-018df9bc857748709)                                  │
│  Ubuntu, us-east-1a, IAM instance role attached                        │
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │  Docker Container (network_mode: host)                           │   │
│  │                                                                   │   │
│  │  pipeline.py ──► bronze_*.py ──► dbt_runner.py ──► dbt models   │   │
│  │       │                │                               │          │   │
│  │       │         DuckDB httpfs                   DuckDB httpfs     │   │
│  │       │                │                               │          │   │
│  │  s3_utils.py (configure_duckdb_s3 + os.environ export)           │   │
│  │       │                                                           │   │
│  │  boto3 ◄── EC2 IMDS (169.254.169.254) ◄── IAM instance role     │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│           │ All writes go directly to S3 (no local data/ layer)        │
└───────────────────────────────────────────────────────────────────────┘
         │ S3 PutObject / GetObject (boto3 + DuckDB httpfs)
         ▼
┌────────────────────────────────────────────────────────────────────────┐
│  S3 — cc-transaction-databricks-datalake-2026                          │
│                                                                         │
│  bronze/                silver/               gold/                     │
│  ├─ accounts/           ├─ accounts/          ├─ daily_summary/         │
│  │   └─ date=*/         ├─ transactions/      └─ weekly_account_summary/│
│  ├─ transactions/       │   └─ date=*/                                  │
│  │   └─ date=*/         └─ transaction_codes/ pipeline/                 │
│  └─ transaction_codes/                        ├─ control.parquet        │
│                                               ├─ gold_weekly_control.   │
│                                               │   parquet               │
│                                               └─ run_log.parquet        │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Infrastructure Components

### EC2 Instance

| Attribute | Value |
|-----------|-------|
| Instance ID | `i-018df9bc857748709` |
| Type | `t3.micro` (2 vCPU, 1 GB RAM) |
| OS | Ubuntu 22.04 LTS |
| Region / AZ | `us-east-1a` |
| IAM instance role | `cc-transactions-lake-role` (S3 CRUD on bucket) |
| IMDS | IMDSv2 required; `HttpPutResponseHopLimit: 2` |
| EBS | 5 GB gp3 (OS + Docker layers + `/app/source/` CSVs only) |
| State at rest | **Stopped** — started by GitHub Actions, stopped after each run |

**Why hop limit 2:** Docker bridge networking adds one extra network hop between the container and IMDS at `169.254.169.254`. The default limit of 1 drops the IMDS request after one hop. Limit 2 allows IMDS to be reached from inside any Docker container — both bridge and host network modes.

**Why `network_mode: host`:** The container shares the EC2 host's network stack directly. `169.254.169.254` is a link-local address that requires host-network proximity. While hop limit 2 solves the bridge-hop problem, host mode eliminates the extra hop entirely and is the more reliable configuration.

### S3 Bucket

| Attribute | Value |
|-----------|-------|
| Bucket | `cc-transaction-databricks-datalake-2026` |
| Region | `us-east-1` |
| Access | EC2 instance role (no public access) |
| Versioning | Enabled (required for teardown workflow) |
| Writes | Atomic `PutObject` — `s3:PutObject` is atomic at object level |

### CloudFormation Stack

| Resource | Logical ID | Purpose |
|----------|-----------|---------|
| EC2 instance | `EC2Instance` | Pipeline execution host |
| IAM role | `EC2Role` | S3 read/write for pipeline |
| IAM instance profile | `EC2InstanceProfile` | Binds role to EC2 |
| Security group | `EC2SecurityGroup` | SSH (22) inbound from GitHub Actions IPs |
| S3 bucket | `DataLakeBucket` | All pipeline outputs |
| CloudWatch log group | `PipelineLogGroup` | Container logs |

Stack name: `cc-transactions-lake-stack`

### IAM Roles

**EC2 instance role — `cc-transactions-lake-role`**
- `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject`, `s3:ListBucket` on `cc-transaction-databricks-datalake-2026`
- No other permissions — principle of least privilege

**GitHub Actions OIDC role — `cc-transactions-lake-github-role`**
- EC2 start/stop/describe
- CloudFormation CRUD
- S3 CRUD + versioning on the bucket
- IAM create/delete scoped to `cc-transactions-lake-*`
- OIDC trust: `repo:SaiKrishnaAkulaDG/aws_databricks_deployment:*`

---

## 3. Credential Architecture

The credential chain resolves at runtime — no AWS keys are stored in `.env`, GitHub Secrets, or code.

```
EC2 IAM Instance Role (attached to i-018df9bc857748709)
        │
        │  IMDSv2 token request + metadata fetch
        │  (reachable via network_mode: host + hop limit 2)
        ▼
boto3.Session().get_credentials()  ←  pipeline/s3_utils.py: configure_duckdb_s3()
        │
        ├──► DuckDB SET commands
        │    SET s3_access_key_id = '...'
        │    SET s3_secret_access_key = '...'
        │    SET s3_session_token = '...'
        │    SET s3_region = 'us-east-1'
        │         │
        │         └──► DuckDB httpfs S3 reads/writes
        │              (bronze COPY TO, silver/gold reads, verify queries)
        │
        └──► os.environ["AWS_ACCESS_KEY_ID"]
             os.environ["AWS_SECRET_ACCESS_KEY"]
             os.environ["AWS_SESSION_TOKEN"]
                  │
                  └──► dbt subprocess inherits os.environ
                       dbt/profiles.yml: env_var('AWS_ACCESS_KEY_ID', '')
                                         env_var('AWS_SECRET_ACCESS_KEY', '')
                                         env_var('AWS_SESSION_TOKEN', '')
                              │
                              └──► dbt-duckdb DuckDB sessions
                                   (silver/gold model reads/writes via httpfs)
```

**Why boto3 injects explicitly into DuckDB:** DuckDB 0.10.0 `httpfs` does not auto-resolve the EC2 IMDS credential chain. Removing explicit injection causes HTTP 403 on all DuckDB S3 operations. boto3 correctly resolves from IMDS; the credentials are then passed to DuckDB via `SET` commands.

**Why `os.environ` export:** dbt runs as a Python subprocess (`subprocess.Popen`). Credentials set on a DuckDB connection object are not visible to subprocess environments. Exporting to `os.environ` before launching dbt ensures dbt's own DuckDB sessions (created by dbt-duckdb adapter internally) inherit the credentials via `env_var()` in `dbt/profiles.yml`.

**`.env` contents (runtime only):**
```
S3_BUCKET=cc-transaction-databricks-datalake-2026
AWS_DEFAULT_REGION=us-east-1
```
No AWS credential variables. Always overwritten from `.env.example` at the start of each GitHub Actions run to prevent stale credential files from prior runs.

---

## 4. Container Architecture

```
docker-compose.yml
│
└─ service: pipeline
   │
   ├─ build: Dockerfile (Python 3.11, DuckDB 0.10.0, dbt-core 1.7.9, boto3, pyarrow)
   ├─ network_mode: host          ← shares EC2 host network; IMDS reachable at 169.254.169.254
   ├─ env_file: .env              ← S3_BUCKET, AWS_DEFAULT_REGION (no credentials)
   ├─ environment:
   │   ├─ S3_BUCKET
   │   └─ AWS_DEFAULT_REGION
   └─ volumes:
       ├─ ./source:/app/source:ro     ← source CSVs (read-only, INV-01)
       ├─ ./dbt:/app/dbt              ← dbt project + profiles
       └─ ./data:/app/data:rw         ← dbt needs /app/data/lake.duckdb (ephemeral catalog)
```

**What stays local (ephemeral):** `lake.duckdb` — dbt's working in-memory DuckDB catalog. Recreated on every run. Not durable state.

**What goes to S3 (durable):** All bronze, silver, gold, and pipeline control parquet files. Written directly during the run via DuckDB `COPY TO 's3://...'` (bulk data) and `boto3.put_object` via `atomic_parquet_put` (control plane files).

**Docker builder prune:** `docker builder prune -f` runs before every `docker compose build`. Prevents BuildKit snapshot corruption that accumulates on long-running EC2 instances where builds are frequently cancelled and restarted (manifests as exit code 17).

---

## 5. S3 Storage Layout

```
cc-transaction-databricks-datalake-2026/
│
├─ bronze/
│   ├─ accounts/
│   │   └─ date=2024-01-01/data.parquet   (partitioned by date)
│   │   └─ date=2024-01-02/data.parquet
│   │   └─ ...
│   ├─ transactions/
│   │   └─ date=2024-01-01/data.parquet
│   │   └─ ...
│   └─ transaction_codes/
│       └─ data.parquet                    (static, loaded once)
│
├─ silver/
│   ├─ accounts/
│   │   └─ data.parquet                    (full upserted snapshot — not partitioned)
│   ├─ transactions/
│   │   └─ date=2024-01-01/data.parquet   (partitioned by date)
│   │   └─ ...
│   └─ transaction_codes/
│       └─ data.parquet                    (static)
│
├─ gold/
│   ├─ daily_summary/
│   │   └─ data.parquet                    (all dates, single file — dbt table materialisation)
│   └─ weekly_account_summary/
│       └─ data.parquet                    (all computed weeks, single file)
│
└─ pipeline/
    ├─ control.parquet                      (watermark — single row)
    ├─ gold_weekly_control.parquet          (computed weeks registry)
    └─ run_log.parquet                      (append-only audit trail)
```

**Atomicity:** S3 `PutObject` is atomic at the object level. Bronze and silver/gold bulk files are written directly by DuckDB `COPY TO 's3://...'`. Control-plane files (`control.parquet`, `gold_weekly_control.parquet`, `run_log.parquet`) are serialized to bytes in memory by pyarrow then uploaded in a single `boto3.put_object` call — preserving INV-40.

**Idempotency:** Before writing any bronze partition, `s3_key_exists(bucket, key)` checks for the object using `boto3.head_object`. If it exists, the load is skipped. Same semantics as the former `os.path.exists` check — different transport.

---

## 6. GitHub Actions Workflow — run-pipeline.yml

```
Trigger: schedule (daily 2 AM UTC) or workflow_dispatch (manual)

Inputs:
  mode:       incremental | historical  (default: incremental)
  start_date: YYYY-MM-DD               (historical only)
  end_date:   YYYY-MM-DD               (historical only)

Steps:
  1. configure-aws-credentials         ← OIDC → cc-transactions-lake-github-role
  2. start-ec2                         ← aws ec2 start-instances
  3. wait-for-ssh (60s)
  4. pull-code                         ← ssh: git pull origin feature/s3-direct-writes
  5. ensure-env                        ← ssh: sudo cp /app/.env.example /app/.env  [always overwrites]
  6. rebuild-docker                    ← ssh: docker builder prune -f + docker compose build
  7. run-pipeline                      ← ssh: docker compose run --rm pipeline python -m pipeline.pipeline
  8. verify-outputs                    ← ssh: docker compose run --rm pipeline python -c "query S3..."
  9. stop-ec2 (if: always())          ← aws ec2 stop-instances  [runs even on failure]
```

**Step 5 always overwrites:** Previously conditional (`if [ ! -f .env ]`). On long-running instances, `.env` could hold expired session tokens. Always overwriting ensures `.env` contains only non-secret config and never stale credentials.

**No "Sync data to S3" step:** Removed entirely. Data is written directly to S3 during step 7.

---

## 7. Data Flow (End-to-End)

```
source/accounts_YYYY-MM-DD.csv        (read-only, on EC2 /app/source/)
source/transactions_YYYY-MM-DD.csv
source/transaction_codes.csv
         │
         │  DuckDB COPY FROM (local CSV read inside container)
         ▼
bronze/accounts/date=*/data.parquet    ─── S3 PutObject (DuckDB COPY TO 's3://...')
bronze/transactions/date=*/data.parquet
bronze/transaction_codes/data.parquet
         │
         │  dbt run --select tag:silver (reads from S3 via DuckDB httpfs)
         ▼
silver/accounts/data.parquet           ─── S3 PutObject (dbt post_hook COPY TO 's3://...')
silver/transactions/date=*/data.parquet
silver/transaction_codes/data.parquet
         │
         │  dbt run --select tag:gold (reads silver from S3)
         ▼
gold/daily_summary/data.parquet        ─── S3 PutObject (dbt post_hook COPY TO 's3://...')
gold/weekly_account_summary/data.parquet
         │
         │  pipeline.py control-plane writes (boto3 atomic_parquet_put)
         ▼
pipeline/control.parquet               ─── S3 PutObject (boto3 put_object)
pipeline/gold_weekly_control.parquet
pipeline/run_log.parquet
```

**Intra-date ordering (INV-24):**  
Accounts Bronze → Accounts Silver → Transactions Bronze → Transactions Silver → Gold Daily → Gold Weekly (if week boundary)

---

## 8. Key Design Decisions (Deployment Layer)

### D1 — S3-Direct Writes (No Local Data Volume)

**Decision:** All pipeline outputs write directly to S3 via DuckDB `httpfs` and `boto3`. The local EBS volume holds only the OS, Docker layers, and source CSVs.

**Why:** Eliminates the post-run `aws s3 sync` step — data is always current on S3 the moment it is written. Removes the EBS volume as a failure point. EC2 instances are treated as ephemeral — the durable state is entirely on S3.

**Trade-off:** Every read and write involves a network round-trip to S3. For the current data volume (7 days, ~35 transactions) this is negligible.

---

### D2 — boto3 Credential Injection into DuckDB (Not Auto-Resolution)

**Decision:** `configure_duckdb_s3()` explicitly fetches credentials from boto3 and injects them into DuckDB via `SET` commands. DuckDB is not left to resolve credentials itself.

**Why:** DuckDB 0.10.0 `httpfs` does not auto-resolve the EC2 IMDS credential chain. boto3 correctly resolves from the full credential chain (env vars → instance profile → IMDS), making it the reliable resolution mechanism. Credentials are also exported to `os.environ` so dbt subprocesses inherit them via `env_var()` in `profiles.yml`.

---

### D3 — IMDSv2 Hop Limit 2 in CloudFormation Template

**Decision:** `MetadataOptions: HttpPutResponseHopLimit: 2` is baked into the EC2 resource in `cf-cc-transactions-lake.yaml`.

**Why:** Docker bridge networking adds one extra hop between the container and IMDS. Default hop limit of 1 drops the request. Hop limit 2 is the minimum required. Baking it into the CF template means any future redeploy inherits the correct setting without a manual `modify-instance-metadata-options` step.

---

### D4 — Always Overwrite `.env` from `.env.example`

**Decision:** The "Ensure .env" step unconditionally runs `sudo cp /app/.env.example /app/.env` on every pipeline execution.

**Why:** EC2 instances are stopped but not terminated between runs — the filesystem persists. A previous run's `.env` may contain expired AWS session tokens. Always overwriting ensures `.env` contains only non-secret configuration and never stale credentials.

---

### D5 — `docker builder prune -f` Before Every Build

**Decision:** `sudo docker builder prune -f` runs before every `docker compose build`.

**Why:** Long-lived EC2 instances accumulate corrupted BuildKit layer cache after multiple cancel-restart cycles (manifests as exit code 17). Pruning before every build prevents this with certainty at the cost of ~5 seconds.

---

### D6 — Atomic `PutObject` for Control-Plane Files

**Decision:** Control-plane files are written via `atomic_parquet_put(bucket, key, df)` — serialises to bytes in memory using pyarrow, then calls `boto3.put_object` in a single call.

**Why:** S3 does not support atomic rename. `PutObject` is atomic at the object level. Serializing to bytes in memory before upload achieves the same atomicity guarantee as the original `write to .tmp + os.replace()` pattern. Preserves INV-40.

---

## 9. Cost Profile

| Resource | Running cost | Stopped cost |
|----------|-------------|-------------|
| EC2 t3.micro | $0.0104/hr | $0.0004/hr (EBS only) |
| S3 (7 days parquet data) | ~$0.001/month | — |
| Single full pipeline run (historical + incremental) | ~$0.002 | — |

**Rule:** Stop the instance after every run. The GitHub Actions workflow stops EC2 in the final step with `if: always()`.

---

## 10. Deployment Commands

### Deploy (fresh stack)
```bash
gh workflow run deploy-infra.yml --ref feature/s3-direct-writes
```

### Run pipeline via GitHub Actions
```bash
# Historical (Jan 1–6)
gh workflow run run-pipeline.yml --ref feature/s3-direct-writes \
  -f mode=historical -f start_date=2024-01-01 -f end_date=2024-01-06

# Incremental (picks up from watermark)
gh workflow run run-pipeline.yml --ref feature/s3-direct-writes -f mode=incremental
```

### After teardown + redeploy — update instance ID secret
```bash
NEW_ID=$(aws cloudformation describe-stacks \
  --stack-name cc-transactions-lake-stack \
  --region us-east-1 --profile DG4-Developer-065317679010 \
  --query 'Stacks[0].Outputs[?OutputKey==`EC2InstanceId`].OutputValue' \
  --output text)
echo $NEW_ID | gh secret set EC2_INSTANCE_ID
```

### Teardown
```bash
gh workflow run teardown-infra.yml --ref feature/s3-direct-writes -f confirm=DELETE
```

---

## 11. Validated Runs

| Session | Mode | Dates | GitHub Actions Run | Result |
|---------|------|-------|--------------------|--------|
| S3 (2026-05-07) | Historical | 2024-01-01 → 2024-01-06 | `25492219411` | ✅ Passed |
| S3 (2026-05-07) | Incremental | 2024-01-07 | `25492672505` | ✅ Passed |
| S4 (2026-05-08) | Historical | 2024-01-01 → 2024-01-06 | `25540595793` | ✅ Passed |
| S4 (2026-05-08) | Incremental | 2024-01-07 | `25540903789` | ✅ Passed |

Watermark after all runs: `2024-01-07`  
S3 outputs confirmed: `bronze/`, `silver/`, `gold/`, `pipeline/`
