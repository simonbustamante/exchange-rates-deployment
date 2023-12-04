# AWS Data Processing and Automation Setup for exchange-rate-dim

## Overview
This README outlines the setup and configuration for an AWS-based data processing and automation system, focusing on extracting, transforming, and loading (ETL) exchange rate data. It utilizes AWS Glue, S3, and Step Functions to create a robust and scalable solution.

## Configuration Details

### AWS Terraform Setup
- **Profile and Region Configuration**: 
  - AWS Profile: `<ACCOUNT-ID>_AWSAdministratorAccess`
  - AWS Region: `us-east-1`

- **S3 Buckets and Prefixes**:
  - Script Bucket: `s3-hq-raw-prd-intec`
  - Data Bucket: `s3-hq-std-prd-finan`
  - Prefix for installation script: `app_glu_hq_finan_hq_zeus_exchange_rates_prd_001`
  - Prefix for data crawl: `xchng_rts`

- **AWS Glue Settings**:
  - Glue Job Role ARN: `arn:aws:iam::<ACCOUNT-ID>:role/svc-role-data-mic-development-integrations`
  - Database Target: `hq-std-prd-finan-link`
  - Glue Version: `4.0`
  - Python Version: `3`

- **Step Function Configuration**:
  - Name: `stp-fnc-hq-finan-zeus-exchange-prd`
  - Definition Path: `zeus_exchange_rates_step_function_prd.json`

### AWS Glue Script
- **Python Script for Glue Job**:
  - Ingests and processes exchange rate data.
  - Handles manual values for specific budget scenarios.
  - Filters, transforms, and loads data into the designated S3 bucket.

### Step Function Definition
- **AWS Step Function**:
  - Orchestrates the ETL process.
  - Manages tasks like starting the Glue job and the crawler.
  - Incorporates decision-making logic for crawler state management.

### Usage
1. **Install Script**: The provided Terraform script (`null_resource`) handles the deployment of the Python script to the specified S3 bucket.
2. **Glue Job Configuration**: The `aws_glue_job` resource sets up the Glue job with the necessary configurations.
3. **Crawler Setup**: The `aws_glue_crawler` resource is configured to crawl the specified S3 data path.
4. **Step Function Setup**: The `aws_sfn_state_machine` resource creates the state machine based on the provided JSON definition.

### Additional Notes
- Ensure that the AWS IAM roles and permissions are correctly set for Glue and Step Functions.
- Verify the S3 bucket paths and prefixes for consistency with your AWS environment.
- Update the Python script and Terraform configurations as needed for specific use cases or AWS environment changes.

## Conclusion
This setup provides a comprehensive guide for configuring an AWS-based system to process and manage exchange rate data, leveraging the power of AWS Glue, S3, and Step Functions. Ensure all configurations are reviewed and adapted to your specific AWS environment and data processing needs.
