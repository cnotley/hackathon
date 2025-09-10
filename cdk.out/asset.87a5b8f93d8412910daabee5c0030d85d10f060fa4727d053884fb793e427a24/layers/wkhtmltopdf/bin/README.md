# wkhtmltopdf Binary Directory

This directory should contain the wkhtmltopdf binary compiled for Amazon Linux 2.

## Setup Instructions

1. Download the wkhtmltopdf binary for Amazon Linux 2:
   ```bash
   wget https://github.com/wkhtmltopdf/packaging/releases/download/0.12.6-1/wkhtmltox-0.12.6-1.amazonlinux2.x86_64.rpm
   ```

2. Extract the binary:
   ```bash
   rpm2cpio wkhtmltox-0.12.6-1.amazonlinux2.x86_64.rpm | cpio -idmv
   cp usr/local/bin/wkhtmltopdf ./bin/
   ```

3. Make it executable:
   ```bash
   chmod +x bin/wkhtmltopdf
   ```

4. The CDK deployment will automatically package this as a Lambda layer.

## Usage in Lambda

The binary will be available at `/opt/bin/wkhtmltopdf` in the Lambda runtime environment.
