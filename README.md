# Data Reliability & Reconciliation Framework

## Overview

The Data Reliability & Reconciliation Framework is a reusable framework designed to validate source data, perform data quality checks, reconcile source and target data, identify detailed data mismatches, determine overall data reliability, and maintain auditability.

This project enhances the previous Data Quality POC into a structured and customer-demo-ready framework.

## Objective

The objective is to provide a reusable approach for identifying data quality and reconciliation issues and determining whether data can be trusted for downstream consumption.

## Framework Flow

Source / API
    ↓
01. Ingestion
    ↓
02. Data Quality
    ↓
03. Reconciliation
    ↓
04. Reliability Decision
    ↓
05. Audit
    ↓
06. Demo Scenarios

## Framework Components

### 01 - Ingestion

- API/source data ingestion
- Schema validation
- Data standardization
- Pipeline/run identification

### 02 - Data Quality

- Data quality validation
- Valid record processing
- Invalid record identification
- Quarantine processing
- Duplicate and data-format validation

### 03 - Reconciliation

The reconciliation engine provides configurable source-to-target validation including:

- Record count reconciliation
- Business key reconciliation
- Missing record detection
- Extra record detection
- Amount reconciliation
- Configurable amount tolerance
- Critical field validation
- Duplicate target key detection

### Detailed Mismatch Framework

The framework captures detailed mismatch information including:

- Run ID
- Business key
- Check type
- Reason code
- Column name
- Source value
- Target value
- Difference
- Timestamp

Example reason codes:

- TARGET_MISSING
- TARGET_EXTRA
- AMOUNT_MISMATCH
- FIELD_VALUE_MISMATCH
- DUPLICATE_TARGET_KEY

### 04 - Reliability Decision

The Reliability Decision Engine combines data quality and reconciliation results to determine an overall data reliability status.

Reliability statuses:

- PASS
- AT_RISK
- FAIL

The decision also provides the reason behind the reliability status.

### 05 - Audit

The audit layer provides run-level traceability and captures:

- Run information
- DQ status
- Reconciliation results
- Reliability status
- Reliability score
- Reliability reason
- Mismatch information

### 06 - Demo Scenarios

Planned customer demonstration scenarios include:

1. Healthy data
2. Data quality failure
3. Amount mismatch
4. Missing/extra records
5. Critical field mismatch
6. Duplicate target key

## Technology Stack

- Databricks
- PySpark
- Delta Lake
- Azure Functions
- Azure Data Factory
- API-based ingestion

## Current Milestone

### Completed

- API ingestion
- Schema validation
- Data standardization
- Data quality checks
- Valid/quarantine processing
- Count reconciliation
- Business key reconciliation
- Missing/extra detection
- Amount reconciliation
- Critical field validation
- Duplicate target key detection
- Detailed mismatch framework
- Unified reconciliation results
- Framework notebook structure

### In Progress

- Reliability Decision Engine
- Audit integration
- Customer demo scenarios
- Final documentation

### Planned

- GitHub packaging
- Final validation and testing
- Customer-demo-ready final version

## Final Goal

The final goal is to demonstrate an end-to-end data reliability process:

Ingest → Validate → Quarantine → Reconcile → Identify Issues → Explain Why → Determine Reliability → Audit

The framework is intended to move beyond basic PASS/FAIL validation by providing actionable mismatch information and an overall view of data reliability.
