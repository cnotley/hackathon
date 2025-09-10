"""
Enhanced MSA Invoice Auditing System - Streamlit User Interface

This module provides an enhanced web-based user interface for the MSA Invoice Auditing System,
with AWS credentials validation, file size checks, progress indicators, authentication,
and real-time updates for Step Functions monitoring.
"""

import streamlit as st
import boto3
import json
import time
import gc
import pandas as pd
from datetime import datetime
from typing import Dict, Any, Optional, List
import io
import base64
import hashlib
import threading
from botocore.exceptions import ClientError, NoCredentialsError, BotoCoreError
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
MAX_FILE_SIZE_MB = 5  # Maximum file size in MB
SUPPORTED_FILE_TYPES = ['pdf']
POLL_INTERVAL_SECONDS = 5  # Polling interval for Step Functions status

# Configure Streamlit page
st.set_page_config(
    page_title="MSA Invoice Auditing System",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

class AuthenticationError(Exception):
    """Custom exception for authentication failures."""
    pass


class FileValidationError(Exception):
    """Custom exception for file validation failures."""
    pass


class AWSCredentialsValidator:
    """Validates AWS credentials and permissions."""
    
    @staticmethod
    def validate_credentials() -> Dict[str, Any]:
        """Validate AWS credentials and return validation status."""
        try:
            # Test STS to verify credentials
            sts_client = boto3.client('sts')
            identity = sts_client.get_caller_identity()
            
            # Test basic S3 permissions
            s3_client = boto3.client('s3')
            s3_client.list_buckets()
            
            # Test Step Functions permissions
            sf_client = boto3.client('stepfunctions')
            sf_client.list_state_machines(maxResults=1)
            
            return {
                'valid': True,
                'account_id': identity.get('Account'),
                'user_arn': identity.get('Arn'),
                'user_id': identity.get('UserId'),
                'message': 'AWS credentials validated successfully'
            }
            
        except NoCredentialsError:
            return {
                'valid': False,
                'error': 'No AWS credentials found. Please configure your credentials.',
                'message': 'Missing AWS credentials'
            }
        except ClientError as e:
            error_code = e.response['Error']['Code']
            return {
                'valid': False,
                'error': f'AWS permission error: {error_code}',
                'message': f'Insufficient permissions: {error_code}'
            }
        except Exception as e:
            return {
                'valid': False,
                'error': f'Unexpected error: {str(e)}',
                'message': 'Credential validation failed'
            }


class FileValidator:
    """Validates uploaded files for size and type."""
    
    @staticmethod
    def validate_file(uploaded_file) -> Dict[str, Any]:
        """Validate uploaded file and return validation status."""
        if uploaded_file is None:
            return {
                'valid': False,
                'error': 'No file provided',
                'message': 'Please select a file to upload'
            }
        
        # Check file size
        file_size_mb = len(uploaded_file.getvalue()) / (1024 * 1024)
        if file_size_mb > MAX_FILE_SIZE_MB:
            return {
                'valid': False,
                'error': f'File size ({file_size_mb:.1f}MB) exceeds maximum allowed size ({MAX_FILE_SIZE_MB}MB)',
                'message': f'File too large: {file_size_mb:.1f}MB > {MAX_FILE_SIZE_MB}MB limit'
            }
        
        # Check file type
        file_extension = uploaded_file.name.split('.')[-1].lower()
        if file_extension not in SUPPORTED_FILE_TYPES:
            return {
                'valid': False,
                'error': f'Unsupported file type: .{file_extension}',
                'message': 'Only PDF files are supported'
            }
        
        return {
            'valid': True,
            'size_mb': file_size_mb,
            'file_type': file_extension,
            'message': f'File validated successfully ({file_size_mb:.1f}MB, .{file_extension})'
        }


class AuthenticationManager:
    """Manages user authentication and session state."""
    
    @staticmethod
    def initialize_session():
        """Initialize session state variables."""
        if 'authenticated' not in st.session_state:
            st.session_state.authenticated = False
        if 'user_info' not in st.session_state:
            st.session_state.user_info = {}
        if 'aws_credentials_valid' not in st.session_state:
            st.session_state.aws_credentials_valid = False
        if 'execution_arn' not in st.session_state:
            st.session_state.execution_arn = None
        if 'uploaded_file_key' not in st.session_state:
            st.session_state.uploaded_file_key = None
        if 'bedrock_session_id' not in st.session_state:
            st.session_state.bedrock_session_id = f"session-{int(time.time())}"
        if 'auto_refresh' not in st.session_state:
            st.session_state.auto_refresh = False
    
    @staticmethod
    def authenticate_user(username: str, password: str) -> bool:
        """Simple authentication (in production, use proper auth service)."""
        # For demo purposes - in production, integrate with proper auth service
        valid_users = {
            'admin': 'admin123',
            'auditor': 'audit123',
            'demo': 'demo123'
        }
        
        if username in valid_users and valid_users[username] == password:
            st.session_state.authenticated = True
            st.session_state.user_info = {
                'username': username,
                'login_time': datetime.now().isoformat(),
                'session_id': hashlib.md5(f"{username}{time.time()}".encode()).hexdigest()
            }
            return True
        return False
    
    @staticmethod
    def logout():
        """Clear session state and logout user."""
        st.session_state.authenticated = False
        st.session_state.user_info = {}
        st.session_state.aws_credentials_valid = False
        st.session_state.execution_arn = None
        st.session_state.uploaded_file_key = None
        st.session_state.auto_refresh = False
        st.rerun()
    
    @staticmethod
    def show_login_form():
        """Display login form."""
        st.markdown("## 🔐 Login Required")
        st.markdown("Please authenticate to access the MSA Invoice Auditing System")
        
        with st.form("login_form"):
            st.markdown("**Demo Credentials:**")
            st.markdown("- Username: `demo` | Password: `demo123`")
            st.markdown("- Username: `auditor` | Password: `audit123`")
            st.markdown("- Username: `admin` | Password: `admin123`")
            
            username = st.text_input("Username", placeholder="Enter your username")
            password = st.text_input("Password", type="password", placeholder="Enter your password")
            
            if st.form_submit_button("🚀 Login", type="primary"):
                if AuthenticationManager.authenticate_user(username, password):
                    st.success("✅ Login successful! Redirecting...")
                    time.sleep(1)
                    st.rerun()
                else:
                    st.error("❌ Invalid credentials. Please try again.")


class ProgressIndicator:
    """Manages progress indicators and real-time updates."""
    
    @staticmethod
    def show_execution_progress(execution_arn: str, stepfunctions_client) -> Dict[str, Any]:
        """Show real-time execution progress."""
        try:
            status = stepfunctions_client.describe_execution(executionArn=execution_arn)
            execution_status = status.get('status', 'UNKNOWN')
            
            # Create progress visualization
            progress_col1, progress_col2 = st.columns([3, 1])
            
            with progress_col1:
                if execution_status == 'RUNNING':
                    st.info("⏳ **Analysis in Progress**")
                    progress_bar = st.progress(0.5)
                    st.caption("Processing your invoice through the audit pipeline...")
                    
                elif execution_status == 'SUCCEEDED':
                    st.success("✅ **Analysis Complete**")
                    progress_bar = st.progress(1.0)
                    st.caption("All audit steps completed successfully!")
                    
                elif execution_status == 'FAILED':
                    st.error("❌ **Analysis Failed**")
                    progress_bar = st.progress(0.0)
                    st.caption("Error occurred during processing. Check details below.")
                    
                else:
                    st.warning(f"📋 **Status: {execution_status}**")
                    progress_bar = st.progress(0.3)
            
            with progress_col2:
                # Auto-refresh toggle
                auto_refresh = st.checkbox("🔄 Auto-refresh", value=st.session_state.auto_refresh)
                st.session_state.auto_refresh = auto_refresh
                
                if auto_refresh and execution_status in ['RUNNING', 'STARTED']:
                    time.sleep(POLL_INTERVAL_SECONDS)
                    st.rerun()
            
            return status
            
        except Exception as e:
            st.error(f"Error checking execution status: {str(e)}")
            return {}


class MSAInvoiceAuditor:
    """Enhanced main class for handling MSA invoice auditing operations."""
    
    def __init__(self):
        """Initialize the auditor with AWS clients and validation."""
        # Validate AWS credentials first
        self.credentials_status = AWSCredentialsValidator.validate_credentials()
        
        if not self.credentials_status['valid']:
            st.error(f"❌ {self.credentials_status['error']}")
            st.info("💡 **Setup Instructions:**")
            st.markdown("""
            1. Configure AWS credentials using one of these methods:
               - AWS CLI: `aws configure`
               - Environment variables: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`
               - IAM roles (for EC2/Lambda)
               - Streamlit secrets: `.streamlit/secrets.toml`
            
            2. Ensure your credentials have permissions for:
               - S3 (read/write buckets)
               - Step Functions (start/describe executions)
               - Bedrock (invoke agents)
            """)
            st.stop()
        
        try:
            # Initialize AWS clients after validation
            self.s3_client = boto3.client('s3')
            self.stepfunctions_client = boto3.client('stepfunctions')
            self.bedrock_agent_client = boto3.client('bedrock-agent-runtime')
            
            # Configuration from environment or Streamlit secrets
            self.ingestion_bucket = st.secrets.get("INGESTION_BUCKET", "msa-invoice-ingestion-bucket")
            self.reports_bucket = st.secrets.get("REPORTS_BUCKET", "msa-invoice-reports-bucket")
            # Ensure this ARN points to the full AI pipeline, not just ingestion
            self.step_function_arn = st.secrets.get("STEP_FUNCTION_ARN", "")
            self.bedrock_agent_id = st.secrets.get("BEDROCK_AGENT_ID", "")
            self.bedrock_agent_alias_id = st.secrets.get("BEDROCK_AGENT_ALIAS_ID", "TSTALIASID")
            
            # Mark credentials as valid in session state
            st.session_state.aws_credentials_valid = True
            
        except Exception as e:
            st.error(f"Error initializing AWS clients: {str(e)}")
            st.session_state.aws_credentials_valid = False
            st.stop()
    
    def validate_and_upload_file(self, uploaded_file) -> Optional[str]:
        """Validate file and upload to S3 with enhanced error handling."""
        try:
            # Validate file first
            validation_result = FileValidator.validate_file(uploaded_file)
            
            if not validation_result['valid']:
                st.error(f"❌ File Validation Failed: {validation_result['error']}")
                st.info(f"💡 {validation_result['message']}")
                return None
            
            # Show file validation success
            st.success(f"✅ File Validation Passed: {validation_result['message']}")
            
            # Upload to S3 with progress
            with st.spinner("📤 Uploading file to S3..."):
                s3_key = self.upload_file_to_s3(uploaded_file.getvalue(), uploaded_file.name)
                
                if s3_key:
                    st.success(f"✅ Upload Complete: `{s3_key}`")
                    return s3_key
                else:
                    st.error("❌ Upload failed. Please try again.")
                    return None
                    
        except Exception as e:
            st.error(f"❌ Upload Error: {str(e)}")
            logger.error(f"File upload error: {str(e)}")
            return None
    
    def upload_file_to_s3(self, file_content: bytes, filename: str) -> str:
        """Upload file to S3 ingestion bucket."""
        try:
            # Generate unique key with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            s3_key = f"uploads/{timestamp}_{filename}"
            
            self.s3_client.put_object(
                Bucket=self.ingestion_bucket,
                Key=s3_key,
                Body=file_content,
                ContentType=self._get_content_type(filename)
            )
            
            return s3_key
        except ClientError as e:
            st.error(f"Error uploading file to S3: {str(e)}")
            return None
    
    def _get_content_type(self, filename: str) -> str:
        """Get content type based on file extension."""
        if filename.lower().endswith('.pdf'):
            return 'application/pdf'
        return 'application/octet-stream'
    
    def start_step_function_execution(self, s3_key: str, query: str = "") -> str:
        """Start Step Functions execution for invoice processing."""
        try:
            execution_input = {
                "bucket": self.ingestion_bucket,
                "key": s3_key,
                "query": query,
                "timestamp": datetime.now().isoformat()
            }
            
            response = self.stepfunctions_client.start_execution(
                stateMachineArn=self.step_function_arn,
                name=f"audit-{int(time.time())}",
                input=json.dumps(execution_input)
            )
            
            return response['executionArn']
        except ClientError as e:
            st.error(f"Error starting Step Functions execution: {str(e)}")
            return None
    
    def get_execution_status(self, execution_arn: str) -> Dict[str, Any]:
        """Get Step Functions execution status."""
        try:
            response = self.stepfunctions_client.describe_execution(
                executionArn=execution_arn
            )
            return response
        except ClientError as e:
            st.error(f"Error getting execution status: {str(e)}")
            return {}
    
    def invoke_bedrock_agent(self, query: str, session_id: str = None) -> Dict[str, Any]:
        """Invoke Bedrock Agent for direct queries."""
        try:
            if not session_id:
                session_id = f"session-{int(time.time())}"
            
            response = self.bedrock_agent_client.invoke_agent(
                agentId=self.bedrock_agent_id,
                agentAliasId=self.bedrock_agent_alias_id,
                sessionId=session_id,
                inputText=query
            )
            
            # Process streaming response
            result_text = ""
            for event in response['completion']:
                if 'chunk' in event:
                    chunk = event['chunk']
                    if 'bytes' in chunk:
                        result_text += chunk['bytes'].decode('utf-8')
            
            return {
                "response": result_text,
                "session_id": session_id
            }
        except ClientError as e:
            st.error(f"Error invoking Bedrock Agent: {str(e)}")
            return {}
    
    def list_reports(self, s3_key_prefix: str = None) -> List[Dict[str, Any]]:
        """List available reports from S3."""
        try:
            prefix = f"reports/{s3_key_prefix.replace('uploads/', '').split('_', 1)[1].split('.')[0]}" if s3_key_prefix else "reports/"
            
            response = self.s3_client.list_objects_v2(
                Bucket=self.reports_bucket,
                Prefix=prefix
            )
            
            reports = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    reports.append({
                        'key': obj['Key'],
                        'size': obj['Size'],
                        'last_modified': obj['LastModified'],
                        'type': self._get_report_type(obj['Key'])
                    })
            
            return sorted(reports, key=lambda x: x['last_modified'], reverse=True)
        except ClientError as e:
            st.error(f"Error listing reports: {str(e)}")
            return []
    
    def _get_report_type(self, key: str) -> str:
        """Determine report type from S3 key."""
        if key.endswith('.xlsx'):
            return 'Excel Report'
        elif key.endswith('.pdf'):
            return 'PDF Report'
        elif key.endswith('.md'):
            return 'Markdown Report'
        else:
            return 'Unknown'
    
    def download_report(self, s3_key: str) -> bytes:
        """Download report from S3."""
        try:
            response = self.s3_client.get_object(
                Bucket=self.reports_bucket,
                Key=s3_key
            )
            return response['Body'].read()
        except ClientError as e:
            st.error(f"Error downloading report: {str(e)}")
            return None
    
    def list_pending_approvals(self) -> List[Dict[str, Any]]:
        """List pending HITL approvals from S3."""
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.ingestion_bucket,
                Prefix="approvals/"
            )
            
            approvals = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    try:
                        approval_obj = self.s3_client.get_object(
                            Bucket=self.ingestion_bucket,
                            Key=obj['Key']
                        )
                        approval_data = json.loads(approval_obj['Body'].read())
                        if approval_data.get('status') == 'pending_approval':
                            approvals.append(approval_data)
                    except Exception as e:
                        logger.warning(f"Failed to load approval {obj['Key']}: {e}")
                        continue
            
            return sorted(approvals, key=lambda x: x.get('timestamp', ''), reverse=True)
        except ClientError as e:
            st.error(f"Error listing pending approvals: {str(e)}")
            return []

    def get_hitl_flags(self, execution_arn: str) -> Dict[str, Any]:
        flags: Dict[str, Any] = {}
        try:
            status = self.stepfunctions_client.describe_execution(executionArn=execution_arn)
            output = status.get('output')
            if output:
                payload = json.loads(output)
                flags = payload.get('discrepancy_analysis') or payload.get('comparison', {}).get('discrepancy_analysis', {})
        except ClientError as e:
            logger.error(f"Error fetching execution details: {e}")
        if flags:
            return flags
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.ingestion_bucket,
                Prefix="approvals/"
            )
            if 'Contents' in response:
                for obj in response['Contents']:
                    approval_obj = self.s3_client.get_object(Bucket=self.ingestion_bucket, Key=obj['Key'])
                    data = json.loads(approval_obj['Body'].read())
                    if data.get('status') == 'pending_approval':
                        return data.get('discrepancy_summary', {})
        except ClientError as e:
            logger.error(f"Error reading approval flags from S3: {e}")
        return {}

    def submit_hitl_decision(self, session_id: str, approved: bool, task_token: Optional[str] = None, comments: str = "") -> None:
        if task_token:
            try:
                if approved:
                    self.stepfunctions_client.send_task_success(
                        taskToken=task_token,
                        output=json.dumps({'decision': 'approved', 'comments': comments})
                    )
                else:
                    self.stepfunctions_client.send_task_failure(
                        taskToken=task_token,
                        error='Rejected',
                        cause=comments or 'User rejected discrepancies'
                    )
            except ClientError as e:
                logger.error(f"Failed to signal Step Functions: {e}")
        else:
            payload = {
                'action': 'hitl_approval',
                'session_id': session_id,
                'approved': approved,
                'comments': comments
            }
            lambda_client = boto3.client('lambda')
            try:
                lambda_client.invoke(
                    FunctionName=os.getenv('AGENT_LAMBDA_NAME', 'agent-lambda'),
                    InvocationType='Event',
                    Payload=json.dumps(payload)
                )
            except ClientError as e:
                logger.error(f"Failed to submit HITL decision via Lambda fallback: {e}")

def main():
    """Enhanced main Streamlit application with authentication and validation."""
    
    # Initialize session state
    AuthenticationManager.initialize_session()
    
    # Check if user is authenticated
    if not st.session_state.authenticated:
        AuthenticationManager.show_login_form()
        return
    
    # User is authenticated - show the main application
    user_info = st.session_state.user_info
    
    # App header with user info
    header_col1, header_col2 = st.columns([3, 1])
    
    with header_col1:
        st.title("🏢 MSA Invoice Auditing System")
        st.markdown("**Upload invoices and analyze them against Master Services Agreement (MSA) standards**")
    
    with header_col2:
        st.markdown(f"**👤 Welcome, {user_info.get('username', 'User')}**")
        if st.button("🚪 Logout", type="secondary"):
            AuthenticationManager.logout()
    
    # Initialize the auditor (with enhanced validation)
    try:
        auditor = MSAInvoiceAuditor()
    except Exception as e:
        st.error(f"Failed to initialize system: {str(e)}")
        return
    
    # Enhanced sidebar with system status
    with st.sidebar:
        st.header("⚙️ System Configuration")
        
        # AWS Credentials Status
        if st.session_state.aws_credentials_valid:
            st.success("✅ AWS Credentials Valid")
            if auditor.credentials_status.get('account_id'):
                st.caption(f"Account: {auditor.credentials_status['account_id']}")
        else:
            st.error("❌ AWS Credentials Invalid")
        
        # Display current configuration
        st.info(f"""
        **Current Settings:**
        - Ingestion Bucket: `{auditor.ingestion_bucket}`
        - Reports Bucket: `{auditor.reports_bucket}`
        - Step Function: `{"✅ Configured" if auditor.step_function_arn else "❌ Missing"}`
        - Bedrock Agent: `{"✅ Configured" if auditor.bedrock_agent_id else "❌ Missing"}`
        """)
        
        # File upload limits
        st.markdown("**Upload Limits:**")
        st.markdown(f"- Max file size: {MAX_FILE_SIZE_MB}MB")
        st.markdown(f"- Supported types: {', '.join(SUPPORTED_FILE_TYPES)}")
        
        # Session info
        st.markdown("---")
        st.markdown("**Session Info:**")
        st.caption(f"User: {user_info.get('username')}")
        st.caption(f"Login: {user_info.get('login_time', 'Unknown')}")
        st.caption(f"Session: {user_info.get('session_id', 'Unknown')[:8]}...")
    
    # Main content area
    col1, col2 = st.columns([1, 1])
    
    with col1:
        st.header("📤 Upload Invoice")
        
        # File uploader with enhanced validation
        uploaded_file = st.file_uploader(
            "Choose an invoice file",
            type=SUPPORTED_FILE_TYPES,
            help=f"Upload PDF files up to {MAX_FILE_SIZE_MB}MB"
        )

        non_pdf_selected = False
        validation_result = None
        # Show file info if file is selected
        if uploaded_file is not None:
            if not uploaded_file.name.lower().endswith('.pdf'):
                st.error("Only PDF files are allowed. Please upload a PDF.")
                non_pdf_selected = True
            else:
                validation_result = FileValidator.validate_file(uploaded_file)
                if validation_result['valid']:
                    st.info(f"📄 **{uploaded_file.name}** ({validation_result['size_mb']:.1f}MB, .{validation_result['file_type']})")
                else:
                    st.error(f"❌ {validation_result['error']}")
        
        # Query input
        query = st.text_input(
            "Analysis Query (Optional)",
            placeholder="e.g., Compare to MSA, Check for overcharges, Validate labor rates",
            help="Provide specific instructions for the analysis"
        )
        
        start_time = time.time()
        # Upload and process button with enhanced validation
        upload_disabled = (
            uploaded_file is None or
            non_pdf_selected or
            (validation_result is not None and not validation_result['valid'])
        )
        
        if st.button("🚀 Upload & Start Analysis", type="primary", disabled=upload_disabled):
            s3_key = auditor.validate_and_upload_file(uploaded_file)
            
            if s3_key:
                st.session_state.uploaded_file_key = s3_key
                
                # Start Step Functions execution
                with st.spinner("🚀 Starting analysis workflow..."):
                    execution_arn = auditor.start_step_function_execution(s3_key, query)
                    
                    if execution_arn:
                        st.success(f"✅ Analysis Started Successfully!")
                        st.session_state.execution_arn = execution_arn
                        st.info(f"**Execution ID:** `{execution_arn.split(':')[-1]}`")
                        
                        # Enable auto-refresh for real-time updates
                        st.session_state.auto_refresh = True
                        duration = time.time() - start_time
                        logger.info(f"Metrics: upload_duration={duration:.2f}s")
                        gc.collect()
                    else:
                        st.error("❌ Failed to start analysis workflow")
        
        # Direct Bedrock Agent query
        st.header("🤖 Direct Agent Query")
        
        agent_query = st.text_area(
            "Ask the MSA Agent",
            placeholder="e.g., What are the standard labor rates for RS work? How do I identify overcharges?",
            help="Query the Bedrock Agent directly for MSA-related questions"
        )
        
        if st.button("💬 Ask Agent"):
            if agent_query:
                with st.spinner("🤖 Querying Bedrock Agent..."):
                    result = auditor.invoke_bedrock_agent(agent_query, st.session_state.bedrock_session_id)
                    
                    if result:
                        st.success("✅ **Agent Response:**")
                        st.markdown(result.get('response', 'No response received'))
                    else:
                        st.error("❌ Failed to get agent response")
            else:
                st.warning("💡 Please enter a query first")
    
    with col2:
        st.header("📊 Analysis Results")
        
        # Enhanced execution status monitoring with progress indicators
        if st.session_state.execution_arn:
            status = ProgressIndicator.show_execution_progress(
                st.session_state.execution_arn, 
                auditor.stepfunctions_client
            )
            
            # Show detailed execution information
            if status:
                execution_status = status.get('status', 'UNKNOWN')
                
                with st.expander("📋 Execution Details", expanded=execution_status == 'FAILED'):
                    detail_col1, detail_col2 = st.columns(2)
                    
                    with detail_col1:
                        st.markdown(f"**Status:** {execution_status}")
                        if status.get('startDate'):
                            st.markdown(f"**Started:** {status['startDate'].strftime('%Y-%m-%d %H:%M:%S')}")
                        if status.get('stopDate'):
                            st.markdown(f"**Ended:** {status['stopDate'].strftime('%Y-%m-%d %H:%M:%S')}")
                    
                    with detail_col2:
                        if status.get('input'):
                            st.markdown("**Input:**")
                            input_data = json.loads(status['input'])
                            st.json({
                                'bucket': input_data.get('bucket', 'N/A'),
                                'key': input_data.get('key', 'N/A')[:50] + '...' if len(input_data.get('key', '')) > 50 else input_data.get('key', 'N/A'),
                                'query': input_data.get('query', 'None')
                            })
                        
                        if execution_status == 'FAILED' and 'error' in status:
                            st.error(f"**Error:** {status['error']}")

                # HITL: Approve/Resume with Task Token (if available)
                st.subheader("🧑‍⚖️ Human-in-the-Loop (HITL)")
                st.caption("If the workflow paused for approval, paste the task token below to approve and resume.")
                
                # Check for pending approvals
                pending_approvals = auditor.list_pending_approvals()
                if pending_approvals:
                    st.warning(f"⚠️ {len(pending_approvals)} approval(s) pending")
                    for approval in pending_approvals:
                        with st.expander(f"Approval Required: {approval['approval_id'][:8]}..."):
                            st.json(approval['discrepancy_summary'])
                
                with st.form("hitl_approval_form"):
                    task_token = st.text_input("Step Functions Task Token", placeholder="Paste task token from approval task")
                    approve = st.form_submit_button("✅ Approve and Resume")
                    if approve and task_token:
                        try:
                            auditor.stepfunctions_client.send_task_success(
                                taskToken=task_token,
                                output=json.dumps({"approved": True})
                            )
                            st.success("Approval sent. Workflow will resume.")
                            st.rerun()
                        except ClientError as e:
                            st.error(f"Failed to send approval: {e}")
        
        else:
            st.info("📋 Upload a file and start analysis to see execution progress here")
        
        # Enhanced reports section
        st.header("📋 Generated Reports")
        
        if st.session_state.uploaded_file_key:
            reports = auditor.list_reports(st.session_state.uploaded_file_key)
            report_start = time.time()
            
            if reports:
                st.success(f"✅ Found {len(reports)} report(s)")
                logger.info(f"Metrics: report_listing_duration={time.time()-report_start:.2f}s, reports={len(reports)}")
                gc.collect()
                
                # Group reports by type for better organization
                report_types = {}
                for report in reports:
                    report_type = report['type']
                    if report_type not in report_types:
                        report_types[report_type] = []
                    report_types[report_type].append(report)
                
                for report_type, type_reports in report_types.items():
                    st.subheader(f"📊 {report_type}s ({len(type_reports)})")
                    
                    for report in type_reports:
                        with st.expander(f"📄 {report['key'].split('/')[-1]}"):
                            info_col, download_col = st.columns([2, 1])
                            
                            with info_col:
                                st.markdown(f"**Size:** {report['size']:,} bytes")
                                st.markdown(f"**Modified:** {report['last_modified'].strftime('%Y-%m-%d %H:%M:%S')}")
                                st.markdown(f"**Type:** {report['type']}")
                            
                            with download_col:
                                if st.button(f"📥 Download", key=f"download_{report['key']}", type="secondary"):
                                    report_data = auditor.download_report(report['key'])
                                    
                                    if report_data:
                                        # Create download with proper MIME type
                                        b64 = base64.b64encode(report_data).decode()
                                        filename = report['key'].split('/')[-1]
                                        
                                        mime_types = {
                                            'Excel Report': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                                            'PDF Report': 'application/pdf',
                                            'Markdown Report': 'text/markdown'
                                        }
                                        mime_type = mime_types.get(report['type'], 'application/octet-stream')
                                        
                                        href = f'<a href="data:{mime_type};base64,{b64}" download="{filename}">📥 Click to download {filename}</a>'
                                        st.markdown(href, unsafe_allow_html=True)
                                        st.success("✅ Download link generated!")
                
                # Enhanced analysis flags section
                st.header("🚩 Analysis Flags")
                st.caption("View discrepancies by category. Filters apply to the CSV exported by the report Lambda.")
                # Allow user to select a CSV from the reports list
                csv_reports = [r for r in reports if str(r['key']).endswith('.csv')]
                if csv_reports:
                    csv_select = st.selectbox("Select CSV report", [r['key'] for r in csv_reports])
                    if st.button("Load Discrepancies", type="secondary"):
                        try:
                            csv_obj = auditor.s3_client.get_object(Bucket=auditor.reports_bucket, Key=csv_select)
                            df = pd.read_csv(csv_obj['Body'])
                            cat = st.selectbox("Filter by type", options=["all","rate_variances","overtime_violations","anomalies","duplicates"], index=0)
                            if cat != "all" and 'type' in df.columns:
                                df_filtered = df[df['type'] == cat]
                            else:
                                df_filtered = df
                            st.dataframe(df_filtered, use_container_width=True)
                        except Exception as e:
                            st.error(f"Failed to load CSV: {e}")
                else:
                    st.info("No CSV report found yet. Generate a report to view discrepancies.")
            
            else:
                st.info("📋 No reports generated yet. Analysis may still be in progress.")
        else:
            st.info("📤 Upload and analyze a file to see reports here")
    
    # Enhanced footer with system information
    st.markdown("---")
    footer_col1, footer_col2, footer_col3 = st.columns(3)
    
    with footer_col1:
        st.markdown("**System Status:**")
        st.markdown(f"🟢 AWS: {'Connected' if st.session_state.aws_credentials_valid else 'Disconnected'}")
    
    with footer_col2:
        st.markdown("**Quick Stats:**")
        st.markdown(f"📁 Active Session: {user_info.get('session_id', 'Unknown')[:8]}...")
    
    with footer_col3:
        st.markdown("**Help:**")
        st.markdown("[📖 Documentation](https://docs.aws.amazon.com/) | [🔧 Support](mailto:support@company.com)")
    
    st.markdown("""
    <div style='text-align: center; color: #666; margin-top: 20px;'>
        <p><strong>Enhanced MSA Invoice Auditing System</strong> | Powered by AWS Bedrock, Step Functions, and Streamlit | v2.0.0</p>
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()
