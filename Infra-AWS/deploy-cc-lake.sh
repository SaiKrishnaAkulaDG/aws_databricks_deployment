#!/bin/bash

################################################################################
# Credit Card Transactions Data Lake - CloudFormation Deployment Script
# Purpose: Automate CloudFormation stack creation and EC2 setup
# Usage: ./deploy-cc-lake.sh [create|delete|status]
################################################################################

set -e

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
STACK_NAME="${STACK_NAME:-cc-transactions-lake-stack}"
S3_BUCKET_NAME="${S3_BUCKET_NAME:-cc-transactions-lake-2026}"
EC2_INSTANCE_TYPE="${EC2_INSTANCE_TYPE:-t3.micro}"
EBS_VOLUME_SIZE="${EBS_VOLUME_SIZE:-5}"
GITHUB_REPO_URL="${GITHUB_REPO_URL:-https://github.com/SaiKrishnaAkulaDG/aws_databricks_deployment.git}"
REGION="${AWS_REGION:-us-east-1}"
TEMPLATE_FILE="cf-cc-transactions-lake.yaml"
LOG_FILE="deployment-$(date +%Y%m%d-%H%M%S).log"

################################################################################
# Utility Functions
################################################################################

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1" | tee -a "$LOG_FILE"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1" | tee -a "$LOG_FILE"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1" | tee -a "$LOG_FILE"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1" | tee -a "$LOG_FILE"
}

check_prerequisites() {
    log_info "Checking prerequisites..."
    
    # Check AWS CLI
    if ! command -v aws &> /dev/null; then
        log_error "AWS CLI is not installed. Please install it first."
        exit 1
    fi
    log_success "AWS CLI found: $(aws --version)"
    
    # Check CloudFormation template
    if [[ ! -f "$TEMPLATE_FILE" ]]; then
        log_error "CloudFormation template not found: $TEMPLATE_FILE"
        exit 1
    fi
    log_success "Template file found"
    
    # Check AWS credentials
    if ! aws sts get-caller-identity &> /dev/null; then
        log_error "AWS credentials not configured or invalid"
        exit 1
    fi
    ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
    log_success "AWS credentials valid (Account: $ACCOUNT_ID)"
    
    # Check region
    if ! aws ec2 describe-regions --region-names "$REGION" &> /dev/null; then
        log_error "Invalid AWS region: $REGION"
        exit 1
    fi
    log_success "AWS region valid: $REGION"
    
    # Validate S3 bucket name format
    if ! [[ "$S3_BUCKET_NAME" =~ ^[a-z0-9]([a-z0-9\-]{1,61}[a-z0-9])?$ ]]; then
        log_error "Invalid S3 bucket name format: $S3_BUCKET_NAME"
        log_error "Bucket name must be lowercase, 3-63 chars, contain only letters, numbers, and hyphens"
        exit 1
    fi
    log_success "S3 bucket name format valid: $S3_BUCKET_NAME"

    # Check S3 bucket name availability
    bucket_check=$(aws s3api head-bucket --bucket "$S3_BUCKET_NAME" --region "$REGION" 2>&1)
    bucket_exit=$?
    if [ $bucket_exit -eq 0 ]; then
        log_warn "S3 bucket already exists: $S3_BUCKET_NAME (CloudFormation will reuse it)"
    elif echo "$bucket_check" | grep -qi "404\|NoSuchBucket\|does not exist"; then
        log_success "S3 bucket name available (will be created by CloudFormation)"
    else
        log_warn "Cannot verify S3 bucket availability — proceeding (CloudFormation will handle it)"
    fi
}

create_stack() {
    log_info "Creating CloudFormation stack: $STACK_NAME"
    
    # Check if stack already exists
    if aws cloudformation describe-stacks \
        --stack-name "$STACK_NAME" \
        --region "$REGION" &> /dev/null; then
        log_warn "Stack already exists: $STACK_NAME"
        read -p "Do you want to update it? (y/N) " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            log_info "Skipping stack creation"
            return
        fi
    fi
    
    log_info "Submitting template to CloudFormation..."
    
    aws cloudformation create-stack \
        --stack-name "$STACK_NAME" \
        --template-body "file://$TEMPLATE_FILE" \
        --parameters \
            "ParameterKey=S3BucketName,ParameterValue=$S3_BUCKET_NAME" \
            "ParameterKey=EC2InstanceType,ParameterValue=$EC2_INSTANCE_TYPE" \
            "ParameterKey=EBSVolumeSize,ParameterValue=$EBS_VOLUME_SIZE" \
            "ParameterKey=GitHubRepoURL,ParameterValue=$GITHUB_REPO_URL" \
        --capabilities CAPABILITY_NAMED_IAM \
        --region "$REGION" 2>&1 | tee -a "$LOG_FILE"
    
    log_success "Stack creation initiated"
    log_info "Waiting for stack creation to complete (this may take 10-15 minutes)..."
    
    # Show progress
    aws cloudformation wait stack-create-complete \
        --stack-name "$STACK_NAME" \
        --region "$REGION" 2>&1 | tee -a "$LOG_FILE"
    
    log_success "Stack creation completed!"
    
    # Display outputs
    display_stack_outputs
}

display_stack_outputs() {
    log_info "Retrieving stack outputs..."
    
    local outputs=$(aws cloudformation describe-stacks \
        --stack-name "$STACK_NAME" \
        --query 'Stacks[0].Outputs' \
        --region "$REGION" \
        --output json)
    
    if [[ -z "$outputs" ]] || [[ "$outputs" == "[]" ]]; then
        log_warn "No outputs found for stack"
        return
    fi
    
    echo -e "\n${GREEN}=== Stack Outputs ===${NC}" | tee -a "$LOG_FILE"
    echo "$outputs" | python3 -m json.tool | tee -a "$LOG_FILE"
    
    # Extract key values
    INSTANCE_ID=$(echo "$outputs" | \
        python3 -c "import sys, json; outputs = json.load(sys.stdin); print(next((o['OutputValue'] for o in outputs if o['OutputKey'] == 'EC2InstanceId'), 'N/A'))")
    
    INSTANCE_IP=$(echo "$outputs" | \
        python3 -c "import sys, json; outputs = json.load(sys.stdin); print(next((o['OutputValue'] for o in outputs if o['OutputKey'] == 'EC2InstancePublicIP'), 'N/A'))")
    
    S3_BUCKET=$(echo "$outputs" | \
        python3 -c "import sys, json; outputs = json.load(sys.stdin); print(next((o['OutputValue'] for o in outputs if o['OutputKey'] == 'S3BucketName'), 'N/A'))")
    
    echo -e "\n${BLUE}Quick Reference:${NC}" | tee -a "$LOG_FILE"
    echo "  Instance ID: $INSTANCE_ID" | tee -a "$LOG_FILE"
    echo "  Instance IP: $INSTANCE_IP" | tee -a "$LOG_FILE"
    echo "  S3 Bucket: $S3_BUCKET" | tee -a "$LOG_FILE"
    
    # Provide SSH connection command
    if [[ "$INSTANCE_IP" != "N/A" ]]; then
        echo -e "\n${YELLOW}To connect via SSH:${NC}" | tee -a "$LOG_FILE"
        echo "  ssh -i <your-key-pair.pem> ec2-user@$INSTANCE_IP" | tee -a "$LOG_FILE"
    fi
}

get_stack_status() {
    log_info "Checking stack status..."
    
    local status=$(aws cloudformation describe-stacks \
        --stack-name "$STACK_NAME" \
        --query 'Stacks[0].StackStatus' \
        --region "$REGION" \
        --output text 2>/dev/null || echo "DOES_NOT_EXIST")
    
    echo -e "\nStack Status: ${BLUE}$status${NC}" | tee -a "$LOG_FILE"
    
    if [[ "$status" == "CREATE_COMPLETE" ]] || [[ "$status" == "UPDATE_COMPLETE" ]]; then
        log_success "Stack is operational"
        display_stack_outputs
    elif [[ "$status" == "CREATE_IN_PROGRESS" ]] || [[ "$status" == "UPDATE_IN_PROGRESS" ]]; then
        log_warn "Stack is currently being updated"
    elif [[ "$status" == "DOES_NOT_EXIST" ]]; then
        log_warn "Stack does not exist"
    else
        log_error "Stack status: $status"
    fi
}

delete_stack() {
    log_warn "This will delete the stack and all associated resources"
    read -p "Are you sure you want to delete stack '$STACK_NAME'? (y/N) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        log_info "Deletion cancelled"
        return
    fi
    
    log_info "Attempting to empty S3 bucket first..."
    
    if aws s3 ls "s3://$S3_BUCKET_NAME" &> /dev/null 2>&1; then
        log_info "Emptying S3 bucket: $S3_BUCKET_NAME"
        aws s3 rm "s3://$S3_BUCKET_NAME" --recursive --region "$REGION" 2>&1 | tee -a "$LOG_FILE" || true
    fi
    
    log_info "Deleting CloudFormation stack..."
    
    aws cloudformation delete-stack \
        --stack-name "$STACK_NAME" \
        --region "$REGION" 2>&1 | tee -a "$LOG_FILE"
    
    log_success "Stack deletion initiated"
    log_info "Waiting for stack deletion to complete..."
    
    aws cloudformation wait stack-delete-complete \
        --stack-name "$STACK_NAME" \
        --region "$REGION" 2>&1 | tee -a "$LOG_FILE"
    
    log_success "Stack deletion completed!"
}

setup_ec2_connection() {
    log_info "Preparing EC2 connection details..."
    
    local outputs=$(aws cloudformation describe-stacks \
        --stack-name "$STACK_NAME" \
        --query 'Stacks[0].Outputs' \
        --region "$REGION" \
        --output json)
    
    INSTANCE_IP=$(echo "$outputs" | \
        python3 -c "import sys, json; outputs = json.load(sys.stdin); print(next((o['OutputValue'] for o in outputs if o['OutputKey'] == 'EC2InstancePublicIP'), ''))")
    
    if [[ -z "$INSTANCE_IP" ]]; then
        log_error "Could not retrieve instance IP"
        return 1
    fi
    
    log_success "EC2 Instance IP: $INSTANCE_IP"
    
    # Create setup script
    cat > setup-ec2.sh << 'SETUP_EOF'
#!/bin/bash
echo "Initializing EC2 instance for pipeline..."
cd /app
echo "Current directory: $(pwd)"
echo "Listing contents: $(ls -la)"
echo "EC2 setup completed"
SETUP_EOF
    
    chmod +x setup-ec2.sh
    
    echo -e "\n${YELLOW}Next steps:${NC}" | tee -a "$LOG_FILE"
    echo "  1. Wait 2-3 minutes for EC2 user data to complete" | tee -a "$LOG_FILE"
    echo "  2. Connect via Session Manager (local CLI):" | tee -a "$LOG_FILE"
    echo "     aws ssm start-session --target <instance-id> --region $REGION --profile <your-profile>" | tee -a "$LOG_FILE"
    echo "  3. Or via AWS Console: EC2 → Instances → Connect → Session Manager" | tee -a "$LOG_FILE"
    echo "  4. Run: docker compose run --rm pipeline python -m pipeline.pipeline --mode historical --start-date 2024-01-01 --end-date 2024-01-07" | tee -a "$LOG_FILE"
    echo "  5. After pipeline finishes, STOP the instance to avoid idle charges:" | tee -a "$LOG_FILE"
    echo "     aws ec2 stop-instances --instance-ids \$INSTANCE_ID --region $REGION" | tee -a "$LOG_FILE"
}

print_usage() {
    cat << EOF
Usage: $0 [COMMAND] [OPTIONS]

Commands:
  create      Create the CloudFormation stack and EC2 instance
  delete      Delete the CloudFormation stack and resources
  status      Check stack status and retrieve outputs
  help        Display this help message

Environment Variables:
  STACK_NAME           (default: cc-transactions-lake-stack)
  S3_BUCKET_NAME       (default: cc-transactions-lake-2026)
  EC2_INSTANCE_TYPE    (default: t3.micro)
  EBS_VOLUME_SIZE      (default: 5)
  GITHUB_REPO_URL      (default: https://github.com/SaiKrishnaAkulaDG/aws_databricks_deployment.git)
  AWS_REGION           (default: us-east-1)

Examples:
  ./deploy-cc-lake.sh create
  AWS_REGION=us-west-2 ./deploy-cc-lake.sh create
  S3_BUCKET_NAME=my-bucket ./deploy-cc-lake.sh create
  ./deploy-cc-lake.sh status
  ./deploy-cc-lake.sh delete

Configuration will be logged to: $LOG_FILE
EOF
}

################################################################################
# Main Script
################################################################################

main() {
    local command="${1:-help}"
    
    # Initialize log file
    echo "=== Deployment Log ===" > "$LOG_FILE"
    echo "Started: $(date)" >> "$LOG_FILE"
    echo "Command: $command" >> "$LOG_FILE"
    echo "" >> "$LOG_FILE"
    
    echo -e "${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${BLUE}║  Credit Card Transactions Data Lake - CloudFormation Deploy${NC} ║"
    echo -e "${BLUE}╚════════════════════════════════════════════════════════════╝${NC}"
    echo ""
    
    log_info "Configuration:"
    log_info "  Stack Name: $STACK_NAME"
    log_info "  S3 Bucket: $S3_BUCKET_NAME"
    log_info "  Instance Type: $EC2_INSTANCE_TYPE"
    log_info "  Volume Size: ${EBS_VOLUME_SIZE}GB"
    log_info "  Region: $REGION"
    echo ""
    
    case "$command" in
        create)
            check_prerequisites
            create_stack
            setup_ec2_connection
            ;;
        delete)
            delete_stack
            ;;
        status)
            check_prerequisites
            get_stack_status
            ;;
        help)
            print_usage
            ;;
        *)
            log_error "Unknown command: $command"
            print_usage
            exit 1
            ;;
    esac
    
    echo ""
    log_success "Operation completed. Full log saved to: $LOG_FILE"
}

# Run main function
main "$@"
