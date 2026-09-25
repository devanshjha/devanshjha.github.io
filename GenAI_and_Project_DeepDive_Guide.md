# Comprehensive Project Deep-Dive & GenAI Technical Guide
## Enterprise Data Platform Modernization (Prudential Financial) & Gen AI Integration

**Author**: Devansh Jha  
**Role**: Data Engineer | Programmer Analyst (Cognizant Technology Solutions)  
**Client**: Prudential Financial (Fortune 500 Insurance)  

---

## Table of Contents
1. [End-to-End System Architecture & Data Flow Sequence](#1-end-to-end-system-architecture--data-flow-sequence)
2. [S3 Bucket Layout & AWS Transfer Family Integration](#2-s3-bucket-layout--aws-transfer-family-integration)
3. [JSON Configuration & PySpark Dynamic Ingestion Engine](#3-json-configuration--pyspark-dynamic-ingestion-engine)
4. [Step Functions Orchestration & 150-Record Batch API Processing](#4-step-functions-orchestration--150-record-batch-api-processing)
5. [Database Architecture, IBM Db2 Stored Procedures & Flyway CI/CD](#5-database-architecture-ibm-db2-stored-procedures--flyway-cicd)
6. [Gen AI Integration: AWS Bedrock Log Observability & Schema Auto-Parser](#6-gen-ai-integration-aws-bedrock-log-observability--schema-auto-parser)
7. [PySpark Performance Tuning & Optimization Deep-Dive](#7-pyspark-performance-tuning--optimization-deep-dive)
8. [Advanced Technical Interview Q&A & Cross-Examination Scenarios](#8-advanced-technical-interview-qa--cross-examination-scenarios)

---

## 1. End-to-End System Architecture & Data Flow Sequence

```
 ┌─────────────────────────┐
 │ FMH On-Prem Network Path│
 └────────────┬────────────┘
              │ Daily Scheduled SFTP (AWS Transfer Family)
              ▼
 ┌─────────────────────────┐
 │ S3 Inbound Directory    │  s3://prudential-data-lake/<vendor>/inbound/
 └────────────┬────────────┘
              │ Triggered Glue Ingestion Job (moves inbound -> source)
              ▼
 ┌─────────────────────────┐
 │ PySpark Stage Load Job  │  Parses JSON config, validates schemas, loads Stage Tables
 └────────────┬────────────┘
              │
              ▼
 ┌────────────────────────────────────────────────────────────────────────┐
 │ AWS Step Functions State Machine Orchestrator                           │
 │                                                                        │
 │  ┌──────────────────────────────────────────────────────────────────┐  │
 │  │ 1. Batch Generation Glue Job (Groups 150 records -> JSON in S3)  │  │
 │  └───────────────────────────────┬──────────────────────────────────┘  │
 │                                  │                                     │
 │  ┌───────────────────────────────▼──────────────────────────────────┐  │
 │  │ 2. AWS Lambda Claims REST API Invoker                            │  │
 │  │    (Sends 150-record JSON payload, gets Control No, SSN, Claim ID)│  │
 │  │    (Persists Partial Success / Error Responses to S3 Response dir)│  │
 │  └───────────────────────────────┬──────────────────────────────────┘  │
 │                                  │                                     │
 │  ┌───────────────────────────────▼──────────────────────────────────┐  │
 │  │ 3. PySpark Claim Creation & Db2 Stored Procedure Glue Job        │  │
 │  │    (Executes Db2 Stored Procedures via JDBC + Updates Aurora DB) │  │
 │  │    (Moves Stage records -> History tables)                       │  │
 │  └──────────────────────────────────────────────────────────────────┘  │
 └────────────────────────────┬───────────────────────────────────────────┘
                              │
             ┌────────────────┴────────────────┐
             ▼                                 ▼
┌───────────────────────────┐    ┌───────────────────────────┐
│ AWS Bedrock AI Diagnostics│    │ Flyway + GitHub Actions   │
│ (Parses failure logs &    │    │ (Automated DDL deployment │
│  sends SNS alerts)        │    │  to Dev & QA DBs)         │
└───────────────────────────┘    └───────────────────────────┘
```

---

## 2. S3 Bucket Layout & AWS Transfer Family Integration

### File Transfer Setup (FMH Team Integration)
- The **File Management Handler (FMH)** team operates on-premise scheduled batch jobs.
- We configured **AWS Transfer Family for SFTP** mapped directly to Amazon S3 endpoints.
- FMH connects using provided SFTP credentials/SSH keys and deposits daily vendor feeds into the S3 `inbound` directory.

### S3 Directory Standard Structure
```
s3://prudential-data-lake-prod/
├── configs/
│   └── vendor_a_claims_config.json
├── vendor_a/
│   ├── inbound/     <-- FMH deposits files here (CSV, XLSX, ZIP, DAT)
│   ├── source/      <-- Moved here upon PySpark job initialization
│   ├── batch_req/   <-- 150-record JSON request payloads for Lambda API
│   ├── batch_resp/  <-- Lambda writes JSON API responses here
│   ├── archive/     <-- Successfully processed source files
│   └── error/       <-- Failed files for manual inspection
```

---

## 3. JSON Configuration & PySpark Dynamic Ingestion Engine

### Example JSON Config (`configs/vendor_a_claims_config.json`)
```json
{
  "vendor_id": "VENDOR_A",
  "process_type": "disability_claims",
  "source_file_pattern": "claims_*.csv",
  "delimiter": ",",
  "has_header": true,
  "stage_table": "STG_DISABILITY_CLAIMS",
  "columns": [
    {"source_name": "policy_no", "target_name": "POLICY_NO", "type": "string"},
    {"source_name": "ssn_num", "target_name": "SSN", "type": "string"},
    {"source_name": "inc_dt", "target_name": "INCURRAL_DATE", "type": "date", "format": "yyyy-MM-dd"},
    {"source_name": "claim_amt", "target_name": "CLAIM_AMOUNT", "type": "double"}
  ]
}
```

### PySpark Dynamic Reader Code Concept
```python
import sys, json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

spark = SparkSession.builder.appName("DynamicIngestionEngine").getOrCreate()

# Read JSON Config from S3
config_path = sys.argv[1] # e.g. s3://prudential-data-lake-prod/configs/vendor_a_claims_config.json
config_data = json.loads(spark.read.text(config_path).first()[0])

# Move file from inbound to source
inbound_path = f"s3://prudential-data-lake-prod/{config_data['vendor_id']}/inbound/{config_data['source_file_pattern']}"
source_path = f"s3://prudential-data-lake-prod/{config_data['vendor_id']}/source/"

# Read raw file dynamically using config delimiter
df_raw = spark.read.option("header", config_data["has_header"]) \
                   .option("delimiter", config_data["delimiter"]) \
                   .csv(inbound_path)

# Apply Dynamic Schema Mapping & Transformations
for col_info in config_data["columns"]:
    if col_info["type"] == "date":
        df_raw = df_raw.withColumn(col_info["target_name"], to_date(col(col_info["source_name"]), col_info["format"]))
    else:
        df_raw = df_raw.withColumn(col_info["target_name"], col(col_info["source_name"]).cast(col_info["type"]))

# Write to Staging Table in Db2 / Aurora
df_raw.write.format("jdbc") \
      .option("url", jdbc_url) \
      .option("dbtable", config_data["stage_table"]) \
      .option("user", db_user) \
      .option("password", db_pass) \
      .mode("append").save()
```

---

## 4. Step Functions Orchestration & 150-Record Batch API Processing

### Batch Generation & REST API Flow
1. **Glue Batch Generation Job**: Reads records from `STG_DISABILITY_CLAIMS`, partitions records into chunks of **150 records per file**, converts them into a formatted JSON array, and writes to `s3://.../batch_req/batch_001.json`.
2. **AWS Lambda Execution**:
   - Triggered by S3 event notification on `batch_req/` creation.
   - Fetches API Auth Token from **AWS Secrets Manager**.
   - Invokes Claims REST API endpoint per batch payload.
3. **Partial-Success Handling**:
   - If 145 claims return HTTP 200 OK with assigned `Control Number` & `Claim ID`, and 5 fail (e.g. invalid SSN format), Lambda generates a response JSON:
   ```json
   {
     "batch_id": "batch_001",
     "successful_claims": [
       {"ssn": "XXX-XX-1234", "control_number": "CTRL-9921", "claim_id": "CLM-8812"}
     ],
     "failed_claims": [
       {"ssn": "INVALID_SSN", "error_code": "ERR_INVALID_FORMAT", "message": "SSN must be 9 digits"}
     ]
   }
   ```
   - Writes response to `s3://.../batch_resp/batch_001_resp.json`.
4. **PySpark Creation & Db2 Stored Procedure Job**:
   - Reads response JSONs from `batch_resp/`.
   - Inserts successful claims into main database claim tables using IBM Db2 Stored Procedures.
   - Moves stage records to `history` tables to ensure **idempotency** (preventing double processing if rerun).

---

## 5. Database Architecture, IBM Db2 Stored Procedures & Flyway CI/CD

### IBM Db2 Stored Procedure Execution from PySpark
```python
# Calling Db2 Stored Procedure from PySpark via JDBC Connection
import java.sql.DriverManager

connection = DriverManager.getConnection(jdbc_url, db_user, db_pass)
callable_statement = connection.prepareCall("{call SP_PROCESS_CLAIM_CREATION(?, ?, ?)}")
callable_statement.setString(1, batch_id)
callable_statement.registerOutParameter(2, java.sql.Types.INTEGER) # Success Count
callable_statement.registerOutParameter(3, java.sql.Types.VARCHAR) # Status Msg
callable_statement.execute()

success_count = callable_statement.getInt(2)
connection.close()
```

### GitHub Actions + Flyway Database Deployment Pipeline
- **Flyway Repository Structure**:
  ```
  sql/
  ├── V1.1__create_disability_stage_table.sql
  ├── V1.2__create_aurora_claims_table.sql
  └── V1.3__update_sp_process_claim_creation.sql
  ```
- **Workflow Mechanics**:
  - Developers write versioned migration SQL (`V1.x__...sql`).
  - Committing to `feature` branch triggers GitHub Actions to run `flyway migrate` against the **Dev AWS Aurora / Db2 instance**.
  - Merging to `sandbox` branch triggers GitHub Actions to execute `flyway migrate` against the **QA Environment**.

---

## 6. Gen AI Integration: AWS Bedrock Log Observability & Schema Auto-Parser

### 1. AI Pipeline Failure Diagnostic Utility

#### Architecture
```
CloudWatch Log Group (Glue Job Failure)
   │
   ▼ EventBridge Rule (State: FAILED)
AWS Lambda Function (Python 3.11)
   │ 1. Fetches raw stack trace
   │ 2. Calls AWS Bedrock (Anthropic Claude 3 Sonnet)
   ▼
AWS Bedrock API (Generates Root-Cause Summary)
   │
   ▼
Amazon SNS Topic -> Emails & Slack Notification to Engineering Team
```

#### Lambda Bedrock Invocation Code
```python
import boto3, json

bedrock = boto3.client(service_name='bedrock-runtime', region_name='us-east-1')
sns = boto3.client('sns')

def lambda_handler(event, context):
    log_data = extract_log_stream(event) # Extract last 100 lines of failure log
    
    prompt = f"""
    You are a Data Engineering Incident Manager. Analyze the following PySpark Glue job traceback:
    ---
    {log_data}
    ---
    Provide:
    1. Root Cause Analysis (1 sentence)
    2. Exact Error Location or S3 Path/Column
    3. Recommended Fix
    """
    
    body = json.dumps({
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 300,
        "messages": [{"role": "user", "content": prompt}]
    })
    
    response = bedrock.invoke_model(modelId='anthropic.claude-3-sonnet-20240229-v1:0', body=body)
    response_body = json.loads(response.get('body').read())
    analysis = response_body['content'][0]['text']
    
    # Send via SNS
    sns.publish(
        TopicArn='arn:aws:sns:us-east-1:123456789012:PipelineAlerts',
        Subject='🚨 Production Pipeline Failure Diagnosis',
        Message=analysis
    )
```

---

### 2. LLM-Assisted Dynamic Schema Parsing Utility

#### Scenario & Value
When a new vendor feed lands with 80+ columns, manually writing the JSON mapping config takes 2-3 hours. Bedrock reads the header row and target database schema, generating a formatted JSON mapping config in 5 seconds.

#### Prompt Engineering Strategy
```text
SYSTEM PROMPT: You are an expert Data Architect. Given a raw CSV header row and a target SQL DDL schema, generate a valid JSON mapping config matching this format:
{"columns": [{"source_name": "...", "target_name": "...", "type": "..."}]}

RAW HEADER: policy_number, insured_ssn, date_of_loss, total_claim_amount
TARGET DDL: 
CREATE TABLE CLAIMS (POLICY_NO VARCHAR(50), SSN VARCHAR(11), INCURRAL_DATE DATE, CLAIM_AMOUNT DOUBLE)
```

---

## 7. PySpark Performance Tuning & Optimization Deep-Dive

| Optimization Technique | Before Optimization | After Optimization | Business Impact |
|---|---|---|---|
| **Broadcast Join** | Standard Shuffle Join on Vendor Lookup table | `broadcast(df_lookup)` | Eliminated network shuffle across 50 nodes |
| **S3 Write Coalesce** | Default 200 small output files per run | `df.coalesce(4).write.parquet()` | Reduced S3 API metadata calls & eliminated Small File Problem |
| **Partition Pruning** | Unpartitioned S3 storage scans | Partitioned by `year=YYYY/month=MM/day=DD` | Reduced Glue read data scanning volume by 85% |
| **Overall Runtime** | **30 Minutes** | **5–6 Minutes** | **~80% Runtime Reduction & DPU Cost Saving** |

---

## 8. Advanced Technical Interview Q&A & Cross-Examination Scenarios

### Section A: Ingestion & Dynamic Framework
**Q1: Why did you externalize schema mapping to JSON configs instead of hardcoding in PySpark?**  
*Answer*: In our modernization project for Prudential, we received file feeds from dozens of third-party insurance vendors. Hardcoding transformations meant modifying, testing, and deploying PySpark code for every new vendor or schema change. By externalizing mappings to JSON configs stored in S3, onboarding a new vendor feed only requires adding a JSON file — requiring **zero Python code modifications or redeployments**.

**Q2: How do you handle file arrival and trigger Glue jobs?**  
*Answer*: The FMH (File Management Handler) team runs scheduled SFTP tasks pushing files into `s3://<bucket>/<vendor>/inbound/` via AWS Transfer Family. CloudWatch EventBridge rules or scheduled AutoSys jobs detect file arrival, trigger parameterized Glue jobs passing `vendor_id` and `process_type`, and move files from `inbound` to `source` for processing.

---

### Section B: Step Functions, Batching & Lambda REST API
**Q3: Why did you batch records into groups of 150 before calling the Claims API?**  
*Answer*: The downstream Claims REST API could not accept massive 100,000-record bulk payloads in a single HTTP request due to payload size limits and timeout constraints. Invoking the API per individual record (100,000 separate HTTP calls) caused massive network latency and rate-limiting. Batching records into 150 per payload struck the optimal balance between throughput, memory usage, and API response speed.

**Q4: How do you handle partial successes in API responses?**  
*Answer*: Lambda receives the API response for each 150-record batch. It inspects status codes per claim. Valid claims (returning `Control Number` and `Claim ID`) are written to a `successful_claims` JSON array in S3, while invalid records (with validation error codes) are written to a `failed_claims` JSON array. The downstream Glue job processes valid claims into Db2/Aurora and routes failed claims to an error table for operational review — ensuring valid claims are not held back by individual record errors.

---

### Section C: Database & Flyway CI/CD
**Q5: How do you invoke IBM Db2 Stored Procedures from AWS Glue?**  
*Answer*: We execute reworked Db2 stored procedures directly from PySpark scripts using JDBC connections (`java.sql.DriverManager` and `CallableStatement`). Database credentials are retrieved at runtime from AWS Secrets Manager to ensure zero hardcoded secrets.

**Q6: How does Flyway + GitHub Actions manage database deployments across Dev and QA?**  
*Answer*: DDL scripts are version-controlled in a Flyway repository (`V1.1__...sql`). When code is merged into `feature` branch, GitHub Actions runs Flyway migrations against the **Dev** database. When merged into `sandbox` branch, GitHub Actions executes Flyway migrations against **QA**. This guarantees schema changes are automated, versioned, and identical across environments.

---

### Section D: Gen AI & Observability
**Q7: How does your AWS Bedrock AI diagnostic utility work when a Glue job fails?**  
*Answer*: When a Glue job fails, EventBridge triggers a Lambda function that extracts the tail of the failure log from CloudWatch. Lambda formats a prompt with the error log and calls AWS Bedrock (Claude 3 model). Bedrock analyzes PySpark tracebacks (e.g. schema mismatches, null pointers, JDBC timeouts) and returns a concise 3-bullet summary: 1. Root Cause, 2. Affected S3 Path/Table, 3. Recommended Remediation. Lambda sends this directly to engineers via SNS, cutting incident triage time from 30 minutes to under 1 minute.

---

*Document compiled for Devansh Jha — Cognizant Technology Solutions | Prudential Modernization Project*
