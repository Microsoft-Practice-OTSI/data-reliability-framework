\# Databricks Data Reliability Framework



\## Overview



This project demonstrates a data reliability framework for transaction data received from an API using Databricks and PySpark.



\## Framework Flow



API Ingestion

→ Data Standardization

→ Data Quality Validation

→ Duplicate Detection

→ Valid / Quarantine

→ Reliability Metrics

→ Data Reconciliation

→ Audit and Run History



\## Data Quality Checks



\- Duplicate transaction IDs

\- Negative or zero transaction amounts

\- Null transaction IDs

\- Null account IDs

\- Invalid transaction dates

\- Date format standardization



\## Outputs



\- Valid transaction records

\- Quarantined transaction records

\- Data reliability metrics

\- Reconciliation audit results

\- Run-level reconciliation history



\## Security



API credentials are retrieved securely from Databricks Secret Scope.



No API keys or credentials are stored in the source code.



\## Future Extensions



\- Record-level reconciliation

\- Source-to-target comparison

\- Historical data comparison

\- Data change detection

\- Automated alerting

\- Monitoring and dashboards

