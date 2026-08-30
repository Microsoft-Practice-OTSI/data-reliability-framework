# ============================================================
# CELL 1 â€” API INGESTION
# ============================================================

import requests

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType
)

# ------------------------------------------------------------
# API CONFIGURATION
# ------------------------------------------------------------

API_URL = "https://func-pod2-banking-api-cfefa9c7g9ctefhf.centralindia-01.azurewebsites.net/api/transactions"

# Retrieve API key securely from Databricks Secrets
FUNCTION_KEY = dbutils.secrets.get(
    scope="pod2-secrets",
    key="banking-api-key"
).strip()

# ------------------------------------------------------------
# API CALL
# ------------------------------------------------------------

response = requests.get(
    API_URL,
    headers={
        "x-functions-key": FUNCTION_KEY,
        "Accept": "application/json"
    },
    timeout=30
)

print("===== API INGESTION =====")
print("Status Code:", response.status_code)

response.raise_for_status()

# ------------------------------------------------------------
# READ API RESPONSE
# ------------------------------------------------------------

records = response.json()["records"]

print("Records returned:", len(records))

# ------------------------------------------------------------
# EXPLICIT SCHEMA
# ------------------------------------------------------------

schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("account_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("transaction_date", StringType(), True)
])

# ------------------------------------------------------------
# CREATE SPARK DATAFRAME
# ------------------------------------------------------------

df = spark.createDataFrame(
    records,
    schema=schema
)

print("===== RAW API DATA =====")

display(df)
# COMMAND ----------
# ============================================================
# CELL 2 â€” DATA STANDARDIZATION
# ============================================================

from pyspark.sql.functions import (
    col,
    coalesce,
    try_to_date
)

df = (
    df
    .withColumn(
        "transaction_date_std",
        coalesce(
            try_to_date(
                col("transaction_date"),
                "yyyy-MM-dd"
            ),
            try_to_date(
                col("transaction_date"),
                "yyyy/MM/dd"
            )
        )
    )
)

print("===== STANDARDIZED DATA =====")

display(df)
# COMMAND ----------
# ============================================================
# CELL 3 â€” DATA QUALITY VALIDATION
# ============================================================

from pyspark.sql.functions import (
    col,
    count,
    when,
    lit,
    concat_ws
)

from pyspark.sql.window import Window

# ------------------------------------------------------------
# DUPLICATE DETECTION
# ------------------------------------------------------------

duplicate_window = Window.partitionBy("transaction_id")

df_quality = (
    df
    .withColumn(
        "duplicate_count",
        count("*").over(duplicate_window)
    )
    .withColumn(
        "is_duplicate",
        col("duplicate_count") > 1
    )
)

# ------------------------------------------------------------
# QUALITY STATUS
# ------------------------------------------------------------

df_quality = (
    df_quality
    .withColumn(
        "quality_status",
        when(col("transaction_id").isNull(), "INVALID")
        .when(col("account_id").isNull(), "INVALID")
        .when(col("amount").isNull(), "INVALID")
        .when(col("amount") <= 0, "INVALID")
        .when(col("transaction_date_std").isNull(), "INVALID")
        .when(col("is_duplicate"), "INVALID")
        .otherwise("VALID")
    )
)

# ------------------------------------------------------------
# REASON CODES
# ------------------------------------------------------------

df_quality = (
    df_quality
    .withColumn(
        "reason_code",
        concat_ws(
            " + ",
            when(
                col("amount") <= 0,
                lit("NON_POSITIVE_AMOUNT")
            ),
            when(
                col("is_duplicate"),
                lit("DUPLICATE_TRANSACTION_ID")
            ),
            when(
                col("transaction_date_std").isNull(),
                lit("INVALID_DATE")
            ),
            when(
                col("transaction_id").isNull(),
                lit("NULL_TRANSACTION_ID")
            ),
            when(
                col("account_id").isNull(),
                lit("NULL_ACCOUNT_ID")
            )
        )
    )
)

# ------------------------------------------------------------
# DISPLAY
# ------------------------------------------------------------

print("===== DATA RELIABILITY VALIDATION =====")

display(
    df_quality.select(
        "transaction_id",
        "account_id",
        "amount",
        "transaction_date",
        "transaction_date_std",
        "is_duplicate",
        "quality_status",
        "reason_code"
    )
)
# COMMAND ----------
# ============================================================
# CELL 3 â€” VALID / QUARANTINE + DELTA OUTPUTS
# ============================================================

# Split the validated data
df_valid = (
    df_quality
    .filter(col("quality_status") == "VALID")
    .drop("duplicate_count")
)

df_quarantine = (
    df_quality
    .filter(col("quality_status") == "INVALID")
    .drop("duplicate_count")
)

# Delta table names
VALID_TABLE = "demo_valid_transactions"
QUARANTINE_TABLE = "demo_quarantine_transactions"

# Write valid records
(
    df_valid.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(VALID_TABLE)
)

# Write invalid records
(
    df_quarantine.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(QUARANTINE_TABLE)
)

print("===== DELTA OUTPUT =====")
print(f"Valid records       : {df_valid.count()}")
print(f"Quarantined records : {df_quarantine.count()}")
print(f"Valid table         : {VALID_TABLE}")
print(f"Quarantine table    : {QUARANTINE_TABLE}")
# COMMAND ----------
# ============================================================
# CELL 4 â€” RELIABILITY + AUDIT METRICS
# ============================================================

from pyspark.sql.functions import col, current_timestamp

# ------------------------------------------------------------
# 1. Calculate metrics
# ------------------------------------------------------------

total_records = df_quality.count()

valid_records = df_quality.filter(
    col("quality_status") == "VALID"
).count()

invalid_records = df_quality.filter(
    col("quality_status") == "INVALID"
).count()

duplicate_records = df_quality.filter(
    col("is_duplicate") == True
).count()

negative_amount_records = df_quality.filter(
    col("amount") <= 0
).count()

invalid_date_records = df_quality.filter(
    col("transaction_date_std").isNull()
).count()

null_transaction_id_records = df_quality.filter(
    col("transaction_id").isNull()
).count()

null_account_id_records = df_quality.filter(
    col("account_id").isNull()
).count()


# ------------------------------------------------------------
# 2. Reliability score
# ------------------------------------------------------------

reliability_score = (
    valid_records * 100.0 / total_records
)

print("===== DATA RELIABILITY METRICS =====")
print(f"Total Records           : {total_records}")
print(f"Valid Records           : {valid_records}")
print(f"Invalid Records         : {invalid_records}")
print(f"Duplicate Records       : {duplicate_records}")
print(f"Negative Amount Records : {negative_amount_records}")
print(f"Invalid Date Records    : {invalid_date_records}")
print(f"Null Transaction IDs    : {null_transaction_id_records}")
print(f"Null Account IDs        : {null_account_id_records}")
print(f"Reliability Score       : {reliability_score}%")


# ------------------------------------------------------------
# 3. Create audit metrics DataFrame
# ------------------------------------------------------------

metrics = [{
    "total_records": int(total_records),
    "valid_records": int(valid_records),
    "invalid_records": int(invalid_records),
    "duplicate_records": int(duplicate_records),
    "negative_amount_records": int(negative_amount_records),
    "invalid_date_records": int(invalid_date_records),
    "null_transaction_id_records": int(null_transaction_id_records),
    "null_account_id_records": int(null_account_id_records),
    "reliability_score": float(reliability_score)
}]

df_metrics = spark.createDataFrame(metrics)

df_metrics = df_metrics.withColumn(
    "run_timestamp",
    current_timestamp()
)

display(df_metrics)


# ------------------------------------------------------------
# 4. Write audit metrics
# ------------------------------------------------------------

METRICS_TABLE = "demo_data_reliability_metrics"

(
    df_metrics.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(METRICS_TABLE)
)

print(f"Metrics written to: {METRICS_TABLE}")
# COMMAND ----------
# ============================================================
# CELL 6 â€” SOURCE-TO-TARGET RECONCILIATION
# ============================================================

from pyspark.sql.functions import (
    lit,
    current_timestamp
)

# ------------------------------------------------------------
# 1. Source record count
# ------------------------------------------------------------

source_count = len(records)

# ------------------------------------------------------------
# 2. Target counts
# ------------------------------------------------------------

valid_count = df_valid.count()

quarantine_count = df_quarantine.count()

processed_count = valid_count + quarantine_count

# ------------------------------------------------------------
# 3. Reconciliation calculation
# ------------------------------------------------------------

reconciliation_difference = (
    source_count - processed_count
)

if reconciliation_difference == 0:
    reconciliation_status = "PASS"
else:
    reconciliation_status = "FAIL"

# ------------------------------------------------------------
# 4. Display reconciliation result
# ------------------------------------------------------------

print("===== SOURCE-TO-TARGET RECONCILIATION =====")

print(f"Source Records          : {source_count}")
print(f"Valid Records           : {valid_count}")
print(f"Quarantined Records     : {quarantine_count}")
print(f"Total Accounted Records: {processed_count}")
print(f"Difference              : {reconciliation_difference}")
print(f"Reconciliation Status   : {reconciliation_status}")

# ------------------------------------------------------------
# 5. Create reconciliation audit record
# ------------------------------------------------------------

reconciliation_data = [{
    "source_count": int(source_count),
    "valid_count": int(valid_count),
    "quarantine_count": int(quarantine_count),
    "processed_count": int(processed_count),
    "reconciliation_difference": int(reconciliation_difference),
    "reconciliation_status": reconciliation_status
}]

df_reconciliation = spark.createDataFrame(
    reconciliation_data
).withColumn(
    "run_timestamp",
    current_timestamp()
)

display(df_reconciliation)

# ------------------------------------------------------------
# 6. Persist reconciliation results
# ------------------------------------------------------------

RECONCILIATION_TABLE = "demo_reconciliation_audit"

(
    df_reconciliation.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(RECONCILIATION_TABLE)
)

print(f"Reconciliation audit written to: {RECONCILIATION_TABLE}")
# COMMAND ----------
# ============================================================
# CELL 7 â€” RUN-LEVEL RECONCILIATION
# ============================================================

from pyspark.sql.functions import current_timestamp, lit

# ------------------------------------------------------------
# 1. Capture current run metrics
# ------------------------------------------------------------

run_timestamp = spark.sql(
    "SELECT current_timestamp()"
).collect()[0][0]

source_count = len(records)

valid_count = df_valid.count()

quarantine_count = df_quarantine.count()

processed_count = valid_count + quarantine_count

reconciliation_difference = source_count - processed_count

if reconciliation_difference == 0:
    reconciliation_status = "PASS"
else:
    reconciliation_status = "FAIL"

# ------------------------------------------------------------
# 2. Create run audit record
# ------------------------------------------------------------

run_metrics = [{
    "source_count": int(source_count),
    "valid_count": int(valid_count),
    "quarantine_count": int(quarantine_count),
    "processed_count": int(processed_count),
    "reconciliation_difference": int(reconciliation_difference),
    "reconciliation_status": reconciliation_status
}]

df_run = (
    spark.createDataFrame(run_metrics)
    .withColumn("run_timestamp", lit(run_timestamp))
)

# ------------------------------------------------------------
# 3. Display current run
# ------------------------------------------------------------

print("===== CURRENT RUN =====")

display(df_run)

# ------------------------------------------------------------
# 4. Append to run history
# ------------------------------------------------------------

RUN_HISTORY_TABLE = "demo_reconciliation_run_history"

(
    df_run.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(RUN_HISTORY_TABLE)
)

print(f"Run history written to: {RUN_HISTORY_TABLE}")
# COMMAND ----------
# ============================================================
# CELL 8 â€” RECONCILIATION RUN HISTORY
# ============================================================

df_history = spark.table(
    "demo_reconciliation_run_history"
).orderBy(
    col("run_timestamp").desc()
)

print("===== RECONCILIATION RUN HISTORY =====")

display(df_history)
# COMMAND ----------
%sql
SELECT
    transaction_id,
    account_id,
    amount,
    transaction_date,
    quality_status
FROM demo_valid_transactions
ORDER BY transaction_id;
# COMMAND ----------
%sql
SELECT
    transaction_id,
    account_id,
    amount,
    transaction_date,
    is_duplicate,
    quality_status,
    reason_code
FROM demo_quarantine_transactions
ORDER BY transaction_id;
# COMMAND ----------
%sql
SELECT
    run_timestamp,
    total_records,
    valid_records,
    invalid_records,
    duplicate_records,
    negative_amount_records,
    invalid_date_records,
    reliability_score
FROM demo_data_reliability_metrics
ORDER BY run_timestamp DESC;
# COMMAND ----------

