# Devansh Jha — Data Engineering & GenAI Project Implementation Guide

This document provides a comprehensive technical walkthrough of the projects, architectures, implementations, and Gen AI use cases featured on your resume. Use this guide to prepare for technical interviews, system design discussions, and recruiter screening calls.

---

## Table of Contents
1. [Core Project Overview & Prudential Client Context](#1-core-project-overview--prudential-client-context)
2. [Enterprise ETL Modernization Pipeline (Informatica → AWS)](#2-enterprise-etl-modernization-pipeline-informatica--aws)
3. [AI-Powered Log Diagnostics & Schema Automation Suite](#3-ai-powered-log-diagnostics--schema-automation-suite)
4. [Data Validation & Reconciliation Framework](#4-data-validation--reconciliation-framework)
5. [Database Operations: AWS Aurora & RDS via GitHub Actions](#5-database-operations-aws-aurora--rds-via-github-actions)
6. [PySpark Performance Optimization Deep Dive](#6-pyspark-performance-optimization-deep-dive)
7. [Comprehensive Interview Preparation Q&A](#7-comprehensive-interview-preparation-qa)

---

## 1. Core Project Overview & Prudential Client Context

### Client Context
- **Client**: Prudential Financial (Fortune 500 Insurance & Financial Services Leader).
- **Program**: Legacy Informatica PowerCenter to AWS Cloud Data Platform Modernization.
- **Problem Statement**: Legacy Informatica ETL jobs running on-premise were costly, rigid to scale, slow to onboard new multi-vendor insurance feeds (CSV, XLSX, ZIP, DAT), and required manual intervention for error handling and database updates.
- **Solution**: Built cloud-native, serverless, parameter-driven PySpark ETL pipelines using AWS Glue, AWS Step Functions, AWS Lambda, AWS Secrets Manager, Amazon S3, AWS Bedrock, IBM Db2, and AWS Aurora/RDS.

---

## 2. Enterprise ETL Modernization Pipeline (Informatica → AWS)

### Architecture Flow
```
[Inbound S3 Bucket]
    │ (Vendor File Arrival: CSV, XLSX, ZIP, DAT)
    ▼
[AWS Glue Ingestion Job] (Config-driven JSON Schema Extraction)
    │
    ▼
[AWS Step Functions Workflow]
    ├─► Batch Generator (Groups 150 staged records into JSON payloads)
    │
    ├─► AWS Lambda API Layer (Invokes Claims REST API per batch)
    │      └─► Partial-Success Handling & Response Persistence in S3
    │
    ├─► AWS Glue Claim Creation Job (Executes Db2 Stored Procedures)
    │
    └─► History Load & SNS 4-Stage Notification System
```

### Key Technical Details
- **Config-Driven Ingestion**: Schema mappings, delimiters, headers, and target table definitions are stored in external JSON configs on S3. When a new vendor onboarded, only the JSON config was updated — **zero Python/Spark code changes**.
- **Batching & REST API Integration**: Glue reads stage records, groups them into batches of 150 records, converts them into JSON payloads, and invokes the Claims API via Lambda.
- **Partial-Success Semantics**: If 140 claims pass API validation and 10 fail, the 140 successful claims are committed to Db2/Aurora while the 10 failed records are routed to an error table with failure reason codes for reprocessing.
- **Stored Procedure Integration**: IBM Db2 stored procedures were reworked to execute directly from PySpark Glue scripts via JDBC connection pooled with credentials secured in AWS Secrets Manager.

---

## 3. AI-Powered Log Diagnostics & Schema Automation Suite

### Implementation 1: AI Pipeline Failure Diagnostic Utility (AWS Bedrock + Lambda + SNS)

#### Use Case & Problem
When a PySpark Glue job or Step Function fails in production, the CloudWatch error log often contains 500+ lines of raw Java/Python stack tracebacks (e.g., `Py4JJavaError`, `AnalysisException`, `NullPointerException`, `ConnectionTimeout`). Triaging this manually takes 20-30 minutes.

#### How It Was Built
1. **Trigger**: AWS CloudWatch Alarms / EventBridge detects a Glue job state change to `FAILED`.
2. **Lambda Handler**: A Python Lambda function extracts the last 100 lines of the CloudWatch Log Stream.
3. **AWS Bedrock Invocation**: Lambda calls **AWS Bedrock API (Claude 3 Model)** using `boto3` with a specialized system prompt:
   - *Prompt*: *"You are an expert Data Engineering Observability assistant. Analyze this PySpark/Glue error log. Extract: 1. Root Cause, 2. Affected S3 Path or Table, 3. Recommended Remediation Steps."*
4. **SNS Alerting**: Bedrock returns a concise 3-bullet-point summary which Lambda formats and sends to the Engineering Slack/Email via Amazon SNS.

#### Real-World Example Output
> 🚨 **Pipeline Failure Alert [Job: claim_stage_load]**
> - **Root Cause**: `SchemaMismatchException` — Column `incurral_date` expected `YYYY-MM-DD`, but received `MM/DD/YYYY` in line 4,120.
> - **Affected Source**: `s3://prudential-vendor-inbound/vendor_b/2026-09-24/claims.csv`
> - **Recommended Action**: Update JSON mapping config for Vendor B or request vendor re-format.

---

### Implementation 2: LLM-Assisted Dynamic Schema Mapping Utility (AWS Bedrock + Python)

#### Use Case & Problem
Vendor feeds frequently alter column names or add new optional fields (e.g., vendor changes `Policy_Num` to `policy_number`). Hardcoded schemas fail.

#### How It Was Built
1. **Header Extraction**: When a new or updated vendor file lands in S3, a lightweight Lambda reads the first 10 rows.
2. **Bedrock Schema Resolution**: Passes the raw header/rows to AWS Bedrock to map incoming headers to the target Db2/Aurora database schema attributes.
3. **JSON Config Generation**: Auto-generates candidate JSON mapping configs:
   ```json
   {
     "source_column": "policy_number",
     "target_column": "POLICY_NO",
     "data_type": "STRING",
     "transformation": "TRIM(UPPER(policy_number))"
   }
   ```
4. **Human-in-the-Loop Review**: Engineer approves the generated JSON config via a simple Git pull request into the deployment repository.

---

## 4. Data Validation & Reconciliation Framework

### Implementation
- **Source vs. Target Row Count Reconciliation**: Asserts `count(source) == count(target)` post-transform.
- **Schema & Type Integrity**: Ensures data types match expectations before database write.
- **Null & Duplicate Checks**: Scans primary key combinations (e.g., `claim_id` + `incurral_date`) for duplicates or unexpected nulls.
- **Checksum / Hash Validation**: Computes MD5/SHA256 hashes of record contents to confirm end-to-end data integrity across S3, PySpark, and Aurora/Db2.

---

## 5. Database Operations: AWS Aurora & RDS via GitHub Actions

### Implementation
- **Infrastructure as Code & CI/CD**: Database tables, indexes, and schema migrations for AWS Aurora (PostgreSQL/MySQL) and RDS were codified into DDL scripts version-controlled in GitHub.
- **Automated Workflow**: When code was merged to `dev` or `qa` branches, a **GitHub Actions** workflow executed:
  1. Validated SQL syntax and migration scripts.
  2. Connected securely to AWS Dev/QA VPC using IAM role credentials.
  3. Executed schema DDL changes automatedly against Dev and QA Aurora/RDS clusters without manual console access.

---

## 6. PySpark Performance Optimization Deep Dive

### Problem
Initial PySpark Glue jobs suffered from execution times exceeding 30 minutes due to **data skew**, **shuffling overhead**, and inefficient **S3 read/write partitioning**.

### Optimizations Applied
1. **Partition Pruning & Repartitioning**: Replaced default `repartition()` with `coalesce()` for writing files and partitioned S3 outputs by `year=YYYY/month=MM/day=DD` to enable partition pruning on reads.
2. **Broadcast Joins**: Used `broadcast()` joins when joining large claim fact datasets with small dimension/lookup tables (e.g., vendor code lookups), eliminating expensive Spark shuffle stages.
3. **Tuning DPU & Memory Allocation**: Configured Glue Worker Types (`G.1X` / `G.2X`) and tuned `spark.sql.shuffle.partitions` to match the data volume.
4. **Result**: Reduced job execution runtime from **~30 minutes down to 5–6 minutes** (~80% reduction) and significantly lowered AWS Glue DPU billing costs.

---

## 7. Comprehensive Interview Preparation Q&A

### Q1: How did you use AWS Bedrock in your data engineering project?
**Answer**: *"We leveraged AWS Bedrock for two key operational enhancements in our modernization project. First, we built an AI-powered pipeline failure diagnostic tool using Lambda and Bedrock (Claude 3). When a Glue or Step Function job failed, Lambda fetched the CloudWatch error traceback and passed it to Bedrock to extract the root cause, affected S3 file path, and recommended fix into a plain-English alert sent via SNS. Second, we built an LLM-assisted schema mapping utility that parsed new or modified vendor file headers in S3 and auto-generated candidate JSON transformation configs, drastically reducing vendor onboarding time."*

### Q2: How did your Step Functions & Glue Claims API batching work?
**Answer**: *"The Step Function orchestrated a multi-stage Glue architecture. Glue ingested raw S3 files into staging tables. Then, a batch generation step grouped staged records into batches of 150 and converted them to JSON API payloads. Lambda invoked the Claims REST API for each batch, persisted JSON responses to S3, and handled partial successes — committing valid claims to Db2/Aurora while storing invalid records with error codes for correction."*

### Q3: How did you handle CI/CD and Database Schema Deployments?
**Answer**: *"We owned CI/CD delivery using GitHub Actions and Jenkins. For AWS Aurora and RDS database schemas, we version-controlled all DDL scripts in GitHub. Merging to Dev or QA triggered GitHub Actions workflows that securely connected to the AWS VPC and executed table DDL updates across Dev and QA environments automatically, avoiding manual AWS console edits."*

### Q4: How did you optimize PySpark job performance from 30 minutes to 5 minutes?
**Answer**: *"We analyzed Spark UI DAGs and identified major shuffle bottlenecks. We implemented broadcast joins for smaller lookup tables, optimized S3 write partitioning, tuned `spark.sql.shuffle.partitions`, and switched from `repartition` to `coalesce` before writing outputs. This cut runtime by ~80% from 30 minutes down to 5-6 minutes."*

---
*Created for Devansh Jha — Data Engineer | Prudential Financial Modernization Program*
