# Credit Card Data Lake — Deploy Using IAM Role (No Access Keys)

**Version**: 1.0  
**Date**: May 6, 2026  
**Status**: Ready to Apply  
**Contact**: saikrishna.akula@datagrokr.co

---

## What This Guide Does

The deployment script `deploy-cc-lake.sh` uses the AWS CLI, which by default reads static access keys
from `~/.aws/credentials` (`aws configure`). This guide replaces those static keys with an IAM role.

After following this guide:
- No `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` stored locally
- AWS CLI assumes a role and gets short-lived credentials automatically
- `deploy-cc-lake.sh` and all `aws` CLI commands run identically — no script changes needed
- The EC2 instance already uses its own IAM instance role for S3 access — that is unchanged

---

## Two Ways to Use a Role (Pick One)

| Option | When to Use |
|--------|-------------|
| **Option A — Profile assumption** (recommended) | Running deploy from your local laptop/desktop |
| **Option B — EC2 instance profile** | Running deploy from another EC2 instance (jump box) |

---

## Quick Reference — Values Used Throughout

| Item | Value |
|------|-------|
| Deployer role name | `cc-lake-deployer-role` |
| AWS profile name | `cc-lake-deployer` |
| Region | `us-east-1` |
| Stack name | `cc-transactions-lake-stack` |
| S3 bucket | `cc-transactions-lake-2026` |

---

## STEP 1 — Find Your AWS Account ID

```bash
aws sts get-caller-identity --query Account --output text
# Expected: 123456789012  (your 12-digit account ID)

# Save it for use in every ARN below
export AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
echo "Account ID: $AWS_ACCOUNT_ID"
```

---

## STEP 2 — Create the Deployer IAM Role

This role carries the permissions needed to run `deploy-cc-lake.sh` (CloudFormation, EC2, S3, IAM).

### 2a. Write the trust policy

**Option A (local machine)** — the role can be assumed by your IAM user:

```bash
# Get your current IAM user ARN
IAM_USER_ARN=$(aws sts get-caller-identity --query Arn --output text)
echo "Your IAM user ARN: $IAM_USER_ARN"

cat > /tmp/deployer-trust-policy.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "${IAM_USER_ARN}"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF
```

**Option B (EC2 jump box)** — the role can be assumed by an EC2 instance:

```bash
cat > /tmp/deployer-trust-policy.json << 'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "ec2.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF
```

### 2b. Create the role

```bash
aws iam create-role \
  --role-name cc-lake-deployer-role \
  --assume-role-policy-document file:///tmp/deployer-trust-policy.json \
  --description "Deployer role for cc-transactions-lake — used by deploy-cc-lake.sh"

# Verify
aws iam get-role \
  --role-name cc-lake-deployer-role \
  --query 'Role.Arn' --output text
# Expected: arn:aws:iam::<ACCOUNT_ID>:role/cc-lake-deployer-role
```

---

## STEP 3 — Attach Permissions to the Role

The `deploy-cc-lake.sh` script calls CloudFormation, EC2, S3, and IAM — this policy covers all of them.

```bash
# 3a. Write the permissions policy
cat > /tmp/deployer-permissions.json << 'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "CloudFormationFull",
      "Effect": "Allow",
      "Action": [
        "cloudformation:CreateStack",
        "cloudformation:DeleteStack",
        "cloudformation:DescribeStacks",
        "cloudformation:DescribeStackEvents",
        "cloudformation:DescribeStackResources",
        "cloudformation:GetTemplate",
        "cloudformation:ValidateTemplate",
        "cloudformation:ListStackResources",
        "cloudformation:UpdateStack"
      ],
      "Resource": "arn:aws:cloudformation:us-east-1:*:stack/cc-transactions-lake-stack/*"
    },
    {
      "Sid": "EC2Deploy",
      "Effect": "Allow",
      "Action": [
        "ec2:DescribeInstances",
        "ec2:DescribeInstanceStatus",
        "ec2:DescribeKeyPairs",
        "ec2:DescribeSecurityGroups",
        "ec2:DescribeSubnets",
        "ec2:DescribeVpcs",
        "ec2:DescribeRegions",
        "ec2:DescribeAvailabilityZones",
        "ec2:CreateSecurityGroup",
        "ec2:DeleteSecurityGroup",
        "ec2:AuthorizeSecurityGroupIngress",
        "ec2:RunInstances",
        "ec2:TerminateInstances",
        "ec2:CreateTags"
      ],
      "Resource": "*"
    },
    {
      "Sid": "S3DataLake",
      "Effect": "Allow",
      "Action": [
        "s3:CreateBucket",
        "s3:DeleteBucket",
        "s3:ListBucket",
        "s3:ListBucketVersions",
        "s3:GetBucketLocation",
        "s3:GetBucketVersioning",
        "s3:PutBucketVersioning",
        "s3:PutBucketPublicAccessBlock",
        "s3:GetBucketPublicAccessBlock",
        "s3:DeleteObject",
        "s3:DeleteObjectVersion",
        "s3:GetObject",
        "s3:PutObject"
      ],
      "Resource": [
        "arn:aws:s3:::cc-transactions-lake-2026",
        "arn:aws:s3:::cc-transactions-lake-2026/*"
      ]
    },
    {
      "Sid": "IAMForCloudFormation",
      "Effect": "Allow",
      "Action": [
        "iam:CreateRole",
        "iam:DeleteRole",
        "iam:GetRole",
        "iam:PassRole",
        "iam:AttachRolePolicy",
        "iam:DetachRolePolicy",
        "iam:PutRolePolicy",
        "iam:DeleteRolePolicy",
        "iam:GetRolePolicy",
        "iam:CreateInstanceProfile",
        "iam:DeleteInstanceProfile",
        "iam:AddRoleToInstanceProfile",
        "iam:RemoveRoleFromInstanceProfile",
        "iam:GetInstanceProfile"
      ],
      "Resource": [
        "arn:aws:iam::*:role/cc-transactions-lake-*",
        "arn:aws:iam::*:instance-profile/cc-transactions-lake-*"
      ]
    },
    {
      "Sid": "CloudWatchLogs",
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:DeleteLogGroup",
        "logs:DescribeLogGroups",
        "logs:PutRetentionPolicy"
      ],
      "Resource": "arn:aws:logs:us-east-1:*:log-group:/aws/ec2/cc-transactions-lake*"
    }
  ]
}
EOF

# 3b. Create the policy in AWS
POLICY_ARN=$(aws iam create-policy \
  --policy-name cc-lake-deployer-policy \
  --policy-document file:///tmp/deployer-permissions.json \
  --query 'Policy.Arn' --output text)
echo "Policy ARN: $POLICY_ARN"

# 3c. Attach to the role
aws iam attach-role-policy \
  --role-name cc-lake-deployer-role \
  --policy-arn $POLICY_ARN

# 3d. Verify
aws iam list-attached-role-policies \
  --role-name cc-lake-deployer-role \
  --query 'AttachedPolicies[*].PolicyName' --output text
# Expected: cc-lake-deployer-policy
```

---

## STEP 4 — Configure Your Local Machine to Use the Role

### Option A — Local Machine (profile assumption)

This configures your local AWS CLI to assume `cc-lake-deployer-role` automatically when you use the `cc-lake-deployer` profile.

```bash
# 4a. Get the role ARN
ROLE_ARN=$(aws iam get-role \
  --role-name cc-lake-deployer-role \
  --query 'Role.Arn' --output text)
echo "Role ARN: $ROLE_ARN"

# 4b. Add the profile to ~/.aws/config
# (Assumes your existing [default] profile has sts:AssumeRole permission)
cat >> ~/.aws/config << EOF

[profile cc-lake-deployer]
role_arn = ${ROLE_ARN}
source_profile = default
region = us-east-1
EOF

# 4c. Verify the profile works — this should show the role, not your IAM user
AWS_PROFILE=cc-lake-deployer aws sts get-caller-identity
# Expected:
# {
#   "UserId": "AROAXXXXXXXXXX:botocore-session-...",
#   "Account": "123456789012",
#   "Arn": "arn:aws:sts::123456789012:assumed-role/cc-lake-deployer-role/botocore-session-..."
# }
```

### Option B — EC2 Jump Box (instance profile)

If you're running the deploy from another EC2 instance, attach the role as an instance profile.

```bash
# 4b-1. Create an instance profile and attach the role
aws iam create-instance-profile \
  --instance-profile-name cc-lake-deployer-profile

aws iam add-role-to-instance-profile \
  --instance-profile-name cc-lake-deployer-profile \
  --role-name cc-lake-deployer-role

# 4b-2. Attach the instance profile to your jump box EC2
aws ec2 associate-iam-instance-profile \
  --instance-id <YOUR_JUMP_BOX_INSTANCE_ID> \
  --iam-instance-profile Name=cc-lake-deployer-profile

# 4b-3. Verify on the EC2 instance (SSH in and run)
aws sts get-caller-identity
# Expected: Arn contains "assumed-role/cc-lake-deployer-role"
```

---

## STEP 5 — Run deploy-cc-lake.sh Using the Role

No changes to the script are required. The AWS CLI reads the role credentials automatically.

### Option A — Local machine

```bash
# Navigate to Infra folder
cd Infra-AWS/

# Set the profile for this terminal session
export AWS_PROFILE=cc-lake-deployer

# Verify you are now running as the role
aws sts get-caller-identity
# Expected: Arn contains "assumed-role/cc-lake-deployer-role"

# Deploy the stack — same command as before
./deploy-cc-lake.sh create

# Check status
./deploy-cc-lake.sh status

# Delete stack (when needed)
./deploy-cc-lake.sh delete
```

### Option B — EC2 jump box

```bash
# On the EC2 instance — no profile needed, role is picked up automatically via IMDS
cd /path/to/Infra

# Verify role is active
aws sts get-caller-identity
# Expected: Arn contains "assumed-role/cc-lake-deployer-role"

# Deploy
./deploy-cc-lake.sh create
```

---

## STEP 6 — Verify the Deployment Worked

The `deploy-cc-lake.sh` script already validates credentials in `check_prerequisites()` using
`aws sts get-caller-identity`. If the role assumption succeeded, that check passes automatically.

```bash
# Confirm the stack was created with the role
AWS_PROFILE=cc-lake-deployer aws cloudformation describe-stacks \
  --stack-name cc-transactions-lake-stack \
  --query 'Stacks[0].StackStatus' \
  --output text
# Expected: CREATE_COMPLETE

# Confirm the EC2 instance is running
AWS_PROFILE=cc-lake-deployer aws cloudformation describe-stacks \
  --stack-name cc-transactions-lake-stack \
  --query 'Stacks[0].Outputs[?OutputKey==`EC2InstancePublicIP`].OutputValue' \
  --output text
# Expected: public IP address of the EC2 instance
```

---

## What the EC2 Instance Uses (Already a Role — No Change Needed)

The EC2 instance itself has always used an IAM instance role (`cc-transactions-lake-ec2-role`) defined
in the CloudFormation template. This role gives the instance access to S3 and CloudWatch — no access
keys are ever stored on the instance.

```
Your machine (deployer)          EC2 instance (pipeline)
────────────────────────         ────────────────────────
cc-lake-deployer-role            cc-transactions-lake-ec2-role
  cloudformation:*                 s3:GetObject / PutObject / ListBucket
  ec2:Describe* / Run*             logs:PutLogEvents
  s3:Delete* (for stack delete)
  iam:* (scoped to cc-lake-*)
```

---

## Troubleshooting

| Error | Cause | Fix |
|---|---|---|
| `Not authorized to assume role` | Your IAM user doesn't have `sts:AssumeRole` on this role | Add `sts:AssumeRole` permission to your IAM user for `cc-lake-deployer-role` |
| `credentials not configured or invalid` (deploy-cc-lake.sh line 65) | Wrong profile or profile not exported | Run `export AWS_PROFILE=cc-lake-deployer` before the script |
| `AccessDenied` on cloudformation:CreateStack | Permission missing from policy | Re-check STEP 3; add missing action and update policy |
| `Cannot exceed quota for RolesPerAccount` | AWS account hit IAM role limit | Delete unused roles or request a quota increase |
| Role credentials expire mid-deploy | Default session duration is 1 hour | Extend: `aws iam update-role --role-name cc-lake-deployer-role --max-session-duration 7200` |

---

## Summary — Before vs. After

| | Before (Static Keys) | After (IAM Role) |
|---|---|---|
| Credentials stored locally | `~/.aws/credentials` (permanent keys) | None — role token fetched at runtime |
| Key rotation needed | Yes — periodic manual rotation | No — tokens auto-expire |
| If laptop is stolen | Keys are compromised until revoked | No credentials to steal |
| Deploy command | `./deploy-cc-lake.sh create` | `AWS_PROFILE=cc-lake-deployer ./deploy-cc-lake.sh create` |
| Script changes needed | — | None |

---

**Last Updated**: May 6, 2026  
**Applies To**: `Infra/deploy-cc-lake.sh`, all manual `aws` CLI deployment commands
