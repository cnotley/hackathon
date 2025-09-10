#!/bin/bash

# Invoice Auditing File Ingestion Module - Deployment Script
# This script automates the deployment of the serverless file ingestion system

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
STACK_NAME="InvoiceIngestionStack"
PYTHON_VERSION="3.11"
CDK_VERSION="2.100.0"

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Function to check Python version
check_python_version() {
    if command_exists python3; then
        PYTHON_VER=$(python3 --version 2>&1 | cut -d' ' -f2 | cut -d'.' -f1,2)
        if [ "$(printf '%s\n' "$PYTHON_VERSION" "$PYTHON_VER" | sort -V | head -n1)" = "$PYTHON_VERSION" ]; then
            print_success "Python $PYTHON_VER is compatible (required: $PYTHON_VERSION+)"
            return 0
        else
            print_error "Python $PYTHON_VER is not compatible (required: $PYTHON_VERSION+)"
            return 1
        fi
    else
        print_error "Python 3 is not installed"
        return 1
    fi
}

# Function to check Node.js and CDK
check_nodejs_cdk() {
    if ! command_exists node; then
        print_error "Node.js is not installed. Please install Node.js 18.x or later."
        return 1
    fi
    
    NODE_VER=$(node --version | cut -d'v' -f2 | cut -d'.' -f1)
    if [ "$NODE_VER" -lt 18 ]; then
        print_error "Node.js version $NODE_VER is not supported. Please install Node.js 18.x or later."
        return 1
    fi
    
    print_success "Node.js $(node --version) is compatible"
    
    if ! command_exists cdk; then
        print_warning "AWS CDK is not installed. Installing CDK $CDK_VERSION..."
        npm install -g aws-cdk@$CDK_VERSION
    else
        CDK_VER=$(cdk --version | cut -d' ' -f1)
        print_success "AWS CDK $CDK_VER is installed"
    fi
}

# Function to check AWS CLI and credentials
check_aws_cli() {
    if ! command_exists aws; then
        print_error "AWS CLI is not installed. Please install AWS CLI v2."
        return 1
    fi
    
    AWS_VER=$(aws --version 2>&1 | cut -d' ' -f1 | cut -d'/' -f2 | cut -d'.' -f1)
    if [ "$AWS_VER" -lt 2 ]; then
        print_warning "AWS CLI v1 detected. Consider upgrading to v2 for better performance."
    fi
    
    # Check AWS credentials
    if ! aws sts get-caller-identity >/dev/null 2>&1; then
        print_error "AWS credentials are not configured. Please run 'aws configure'."
        return 1
    fi
    
    ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
    REGION=$(aws configure get region)
    
    print_success "AWS CLI configured for account $ACCOUNT_ID in region $REGION"
}

# Function to setup Python virtual environment
setup_python_env() {
    print_status "Setting up Python virtual environment..."
    
    if [ ! -d ".venv" ]; then
        python3 -m venv .venv
        print_success "Created virtual environment"
    else
        print_status "Virtual environment already exists"
    fi
    
    # Activate virtual environment
    source .venv/bin/activate
    
    # Upgrade pip
    pip install --upgrade pip
    
    # Install dependencies
    print_status "Installing Python dependencies..."
    pip install -r requirements.txt
    
    print_success "Python environment setup complete"
}

# Function to run tests
run_tests() {
    print_status "Running unit tests..."
    
    # Activate virtual environment
    source .venv/bin/activate
    
    # Run tests
    if python -m pytest tests/ -v; then
        print_success "All tests passed"
    else
        print_error "Some tests failed. Please fix the issues before deploying."
        return 1
    fi
}

# Function to bootstrap CDK
bootstrap_cdk() {
    print_status "Checking CDK bootstrap status..."
    
    ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
    REGION=$(aws configure get region)
    
    # Check if CDK is already bootstrapped
    if aws cloudformation describe-stacks --stack-name CDKToolkit --region $REGION >/dev/null 2>&1; then
        print_success "CDK is already bootstrapped in $REGION"
    else
        print_status "Bootstrapping CDK in $REGION..."
        cdk bootstrap aws://$ACCOUNT_ID/$REGION
        print_success "CDK bootstrap complete"
    fi
}

# Function to synthesize CDK
synthesize_cdk() {
    print_status "Synthesizing CDK template..."
    
    # Activate virtual environment
    source .venv/bin/activate
    
    if cdk synth; then
        print_success "CDK synthesis successful"
    else
        print_error "CDK synthesis failed"
        return 1
    fi
}

# Function to deploy stack
deploy_stack() {
    print_status "Deploying $STACK_NAME..."
    
    # Activate virtual environment
    source .venv/bin/activate
    
    # Deploy with approval
    if cdk deploy --require-approval never; then
        print_success "Deployment successful!"
        
        # Get stack outputs
        print_status "Stack outputs:"
        aws cloudformation describe-stacks \
            --stack-name $STACK_NAME \
            --query 'Stacks[0].Outputs[*].[OutputKey,OutputValue,Description]' \
            --output table
    else
        print_error "Deployment failed"
        return 1
    fi
}

# Function to verify deployment
verify_deployment() {
    print_status "Verifying deployment..."
    
    # Check S3 bucket
    BUCKET_NAME=$(aws cloudformation describe-stacks \
        --stack-name $STACK_NAME \
        --query 'Stacks[0].Outputs[?OutputKey==`BucketName`].OutputValue' \
        --output text)
    
    if aws s3 ls "s3://$BUCKET_NAME" >/dev/null 2>&1; then
        print_success "S3 bucket $BUCKET_NAME is accessible"
    else
        print_error "S3 bucket $BUCKET_NAME is not accessible"
        return 1
    fi
    
    # Check Lambda function
    if aws lambda get-function --function-name ingestion-lambda >/dev/null 2>&1; then
        print_success "Lambda function ingestion-lambda is deployed"
    else
        print_error "Lambda function ingestion-lambda is not found"
        return 1
    fi
    
    # Check Step Functions state machine
    if aws stepfunctions list-state-machines --query 'stateMachines[?name==`invoice-audit-workflow`]' --output text | grep -q "invoice-audit-workflow"; then
        print_success "Step Functions state machine invoice-audit-workflow is deployed"
    else
        print_error "Step Functions state machine invoice-audit-workflow is not found"
        return 1
    fi
    
    print_success "All resources verified successfully!"
}

# Function to run post-deployment test
post_deployment_test() {
    print_status "Running post-deployment test..."
    
    BUCKET_NAME=$(aws cloudformation describe-stacks \
        --stack-name $STACK_NAME \
        --query 'Stacks[0].Outputs[?OutputKey==`BucketName`].OutputValue' \
        --output text)
    
    # Create a test file
    echo "Test invoice content for deployment verification" > test-deployment.pdf
    
    # Upload test file
    if aws s3 cp test-deployment.pdf "s3://$BUCKET_NAME/test/"; then
        print_success "Test file uploaded successfully"
        
        # Wait a moment for processing
        sleep 5
        
        # Check CloudWatch logs for Lambda execution
        LOG_GROUP="/aws/lambda/ingestion-lambda"
        if aws logs describe-log-streams --log-group-name $LOG_GROUP --order-by LastEventTime --descending --max-items 1 >/dev/null 2>&1; then
            print_success "Lambda function executed successfully"
        else
            print_warning "Could not verify Lambda execution in logs"
        fi
        
        # Clean up test file
        rm -f test-deployment.pdf
        aws s3 rm "s3://$BUCKET_NAME/test/test-deployment.pdf" >/dev/null 2>&1 || true
        
    else
        print_error "Failed to upload test file"
        return 1
    fi
}

# Function to display usage
usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --skip-tests     Skip running unit tests"
    echo "  --skip-verify    Skip post-deployment verification"
    echo "  --help           Show this help message"
    echo ""
    echo "This script will:"
    echo "  1. Check prerequisites (Python, Node.js, AWS CLI)"
    echo "  2. Setup Python virtual environment"
    echo "  3. Run unit tests (unless --skip-tests)"
    echo "  4. Bootstrap CDK if needed"
    echo "  5. Synthesize and deploy CDK stack"
    echo "  6. Verify deployment (unless --skip-verify)"
    echo "  7. Run post-deployment test"
}

# Main deployment function
main() {
    local skip_tests=false
    local skip_verify=false
    
    # Parse command line arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            --skip-tests)
                skip_tests=true
                shift
                ;;
            --skip-verify)
                skip_verify=true
                shift
                ;;
            --help)
                usage
                exit 0
                ;;
            *)
                print_error "Unknown option: $1"
                usage
                exit 1
                ;;
        esac
    done
    
    print_status "Starting deployment of Invoice Auditing File Ingestion Module"
    print_status "=================================================="
    
    # Check prerequisites
    print_status "Checking prerequisites..."
    check_python_version || exit 1
    check_nodejs_cdk || exit 1
    check_aws_cli || exit 1
    
    # Setup Python environment
    setup_python_env || exit 1
    
    # Run tests
    if [ "$skip_tests" = false ]; then
        run_tests || exit 1
    else
        print_warning "Skipping tests as requested"
    fi
    
    # Bootstrap CDK
    bootstrap_cdk || exit 1
    
    # Synthesize CDK
    synthesize_cdk || exit 1
    
    # Deploy stack
    deploy_stack || exit 1
    
    # Verify deployment
    if [ "$skip_verify" = false ]; then
        verify_deployment || exit 1
        post_deployment_test || exit 1
    else
        print_warning "Skipping verification as requested"
    fi
    
    print_success "=================================================="
    print_success "Deployment completed successfully!"
    print_success "=================================================="
    
    echo ""
    print_status "Next steps:"
    echo "  1. Upload invoice files to the S3 bucket"
    echo "  2. Monitor CloudWatch logs for processing status"
    echo "  3. Check Step Functions executions for workflow status"
    echo ""
    print_status "For more information, see docs/deployment.md"
}

# Run main function with all arguments
main "$@"
