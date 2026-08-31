# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Silver: Customers (SCD Type 2)
# MAGIC
# MAGIC **Features:**
# MAGIC - SCD Type 2: Preserves full history of changes
# MAGIC - Handles multiple updates in same batch (all versions captured)
# MAGIC - Backfill support: full_load and date range reprocessing
# MAGIC - **Watermark-based incremental file loading**
# MAGIC
# MAGIC **Parameters:**
# MAGIC - `full_load`: Set to "true" for initial load or full rebuild
# MAGIC - `reprocess_start_date` / `reprocess_end_date`: For partial backfill

# COMMAND ----------

# MAGIC %run ./00_Config_and_Utilities

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Setup

# COMMAND ----------

catalog = PipelineConfig.CATALOG
schema = PipelineConfig.SILVER_SCHEMA
table_name = f"{catalog}.{schema}.customers"
batch_id = datetime.now().strftime("%Y%m%d%H%M%S")

# Get run mode FRESH from widgets
run_mode = get_run_mode()

print(f"Target Table: {table_name}")
print(f"Batch ID: {batch_id}")
print(f"Run Mode: {run_mode}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Setup Watermark

# COMMAND ----------

# Create watermark table if not exists
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.pipeline_watermarks (
        table_name STRING,
        last_file_time BIGINT,
        last_run_at TIMESTAMP,
        files_processed LONG,
        records_processed LONG
    )
    USING DELTA
""")

# Get existing watermark
watermark_df = spark.sql(f"""
    SELECT last_file_time 
    FROM {catalog}.{schema}.pipeline_watermarks 
    WHERE table_name = 'customers'
""")

if watermark_df.count() > 0:
    existing_watermark = watermark_df.collect()[0][0]
    print(f"Existing watermark: {existing_watermark}")
    print(f"Readable: {datetime.fromtimestamp(existing_watermark / 1000)}")
else:
    existing_watermark = 0
    print("No existing watermark found (first run)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Find Files to Process

# COMMAND ----------

bronze_path = f"{PipelineConfig.BRONZE_PATH}customers/"

# Re-read run_mode fresh
run_mode = get_run_mode()
print(f"Run Mode: {run_mode}")

# List all partition folders
all_folders = dbutils.fs.ls(bronze_path)

# Get parquet files from inside each folder
all_files = []
for folder in all_folders:
    if folder.isDir():
        try:
            files_inside = dbutils.fs.ls(folder.path)
            parquet_files = [f for f in files_inside if f.path.endswith('.parquet')]
            all_files.extend(parquet_files)
        except Exception as e:
            print(f"Warning: Could not read {folder.path}: {e}")

print(f"Total parquet files in Bronze: {len(all_files)}")

# ============================================
# Determine which files to process based on run mode
# ============================================

if run_mode == "full_backfill":
    # FULL: Process ALL files, ignore watermark
    new_files = all_files
    print(f"FULL BACKFILL: Processing all {len(new_files)} files (watermark ignored)")
    
elif run_mode == "partial_backfill":
    # PARTIAL: Process ALL files, will filter data by date later
    new_files = all_files
    print(f"PARTIAL BACKFILL: Processing all {len(new_files)} files (will filter by date)")
    
else:
    # INCREMENTAL: Only process files newer than watermark
    new_files = [f for f in all_files if f.modificationTime > existing_watermark]
    print(f"INCREMENTAL: {len(new_files)} new files to process")
    if existing_watermark > 0:
        print(f"  (files after {datetime.fromtimestamp(existing_watermark / 1000)})")

# Exit if no files to process
if len(new_files) == 0:
    print("No new files to process - exiting")
    dbutils.notebook.exit("SUCCESS: No new files to process")

# Calculate new watermark (will save only after success)
new_watermark = max(f.modificationTime for f in new_files)
print(f"New watermark (pending): {datetime.fromtimestamp(new_watermark / 1000)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Load Bronze Data

# COMMAND ----------

# Read ONLY the files we need (not all files!)
new_file_paths = [f.path for f in new_files]
customers_bronze = spark.read.parquet(*new_file_paths)
print(f"Loaded {customers_bronze.count()} rows from {len(new_files)} files")

# Apply backfill filter if needed
customers_bronze = filter_for_backfill(customers_bronze, date_column="modified_date")
record_count = customers_bronze.count()
print(f"After backfill filter: {record_count} rows")

if record_count == 0:
    print("No records after filtering - exiting")
    dbutils.notebook.exit("SUCCESS: No records in filter range")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Deduplicate
# MAGIC
# MAGIC Keep only the LATEST record per customer_id within this batch.

# COMMAND ----------

# Deduplicate: Keep latest record per customer_id
customers_deduped = remove_duplicates(
    df=customers_bronze,
    key_columns="customer_id",
    order_column="modified_date"
)

print(f"After deduplication: {customers_deduped.count()} unique customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Transform

# COMMAND ----------

# Standardize strings
customers_clean = standardize_strings(
    df=customers_deduped,
    columns_config={
        "first_name": "initcap",
        "last_name": "initcap",
        "city": "initcap",
        "state": "upper",
        "email": "lower"
    }
)

# Fill nulls
customers_clean = apply_null_defaults(
    df=customers_clean,
    null_defaults={
        "city": "Unknown",
        "state": "Unknown",
        "country": "USA"
    }
)

# Add audit columns
customers_clean = add_audit_columns(customers_clean, batch_id)

# Add SCD2 columns (handles multiple updates per customer)
customers_scd = add_scd2_columns_versioned(
    df=customers_clean,
    key_column="customer_id",
    order_column="modified_date"
)

print(f"Total versions created: {customers_scd.count()}")
print(f"Current records (is_current=1): {customers_scd.filter('is_current = 1').count()}")

# COMMAND ----------

customers_scd.printSchema()

# COMMAND ----------

# DBTITLE 1,Cell 16
from pyspark.sql.functions import regexp_replace, when

# modified_date contains two formats:
#   SQL Server: 'Aug 17 2026  2:54AM'  (starts with letter, double-space before single-digit hour)
#   ISO:        '2026-01-17 23:11:46'  (starts with digit)
# Spark's auto-cast only handles ISO; use conditional parsing for both.
_ts_fmt_ss  = "MMM dd yyyy h:mma"
_ts_fmt_iso = "yyyy-MM-dd HH:mm:ss"
_w = Window.partitionBy("customer_id").orderBy("modified_date")

def _parse_ts(c):
    return when(
        c.rlike(r"^[A-Za-z]"),
        to_timestamp(regexp_replace(c, "  ", " "), _ts_fmt_ss)
    ).otherwise(
        to_timestamp(c, _ts_fmt_iso)
    )

customers_display = customers_scd \
    .withColumn("effective_start_date", _parse_ts(col("modified_date"))) \
    .withColumn("effective_end_date",
        coalesce(
            _parse_ts(lead(col("modified_date")).over(_w)),
            to_timestamp(lit("9999-12-31 23:59:59"))
        ))

display(customers_display)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Validate Before Write

# COMMAND ----------

validate_primary_key(customers_scd, "customer_id", "customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Prepare Target for Backfill (if needed)

# COMMAND ----------

# For full_backfill: truncates table
# For partial_backfill: deletes affected date range
# For incremental: does nothing
prepare_target_for_backfill(spark, table_name, date_column="effective_start_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Write to Silver

# COMMAND ----------

# DBTITLE 1,Cell 22
# Fix SQL Server datetime strings ('Aug 17 2026  2:54AM') in effective dates before writing
from pyspark.sql.functions import regexp_replace, when
_ts_fmt_ss  = "MMM dd yyyy h:mma"
_ts_fmt_iso = "yyyy-MM-dd HH:mm:ss"
_wc = Window.partitionBy("customer_id").orderBy("modified_date")

def _fix_ts(c):
    return when(
        c.rlike(r"^[A-Za-z]"),
        to_timestamp(regexp_replace(c, "  ", " "), _ts_fmt_ss)
    ).otherwise(
        to_timestamp(c, _ts_fmt_iso)
    )

customers_scd_fixed = customers_scd \
    .withColumn("effective_start_date", _fix_ts(col("modified_date"))) \
    .withColumn("effective_end_date",
        coalesce(
            _fix_ts(lead(col("modified_date")).over(_wc)),
            to_timestamp(lit("9999-12-31 23:59:59"))
        ))

# Re-read run_mode fresh
run_mode = get_run_mode()

# =============================================
# FULL BACKFILL or FIRST RUN
# =============================================
if run_mode == "full_backfill" or not table_exists(spark, table_name):
    print("Full backfill - overwriting entire table")
    
    customers_scd_fixed.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(table_name)
    
    print(f"Full backfill completed: {customers_scd_fixed.count()} records written")

# =============================================
# PARTIAL BACKFILL
# =============================================
elif run_mode == "partial_backfill":
    print("Partial backfill - appending reprocessed records")
    
    customers_scd_fixed.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable(table_name)
    
    print(f"Partial backfill completed: {customers_scd_fixed.count()} records appended")

# =============================================
# INCREMENTAL (SCD2 MERGE)
# =============================================
else:
    print("Incremental mode - applying SCD2 MERGE")
    
    # Get existing current records
    existing_current = spark.table(table_name).filter("is_current = 1")
    
    # Get incoming current records
    incoming_current = customers_scd_fixed.filter("is_current = 1")
    
    # Changed records: exist in both, but hash is different
    changed = incoming_current.alias("new").join(
        existing_current.alias("old"),
        col("new.customer_id") == col("old.customer_id"),
        "inner"
    ).filter(col("new.row_hash") != col("old.row_hash")) \
     .select(col("new.customer_id").alias("customer_id"))
    
    # New records: in incoming but not in existing
    new_customers = incoming_current.join(
        existing_current,
        "customer_id",
        "left_anti"
    )
    
    changed_count = changed.count()
    new_count = new_customers.count()
    print(f"Changed records: {changed_count}")
    print(f"New records: {new_count}")
    
    # Skip if no changes
    if changed_count == 0 and new_count == 0:
        print("No changes detected - skipping MERGE")
    else:
        target = DeltaTable.forName(spark, table_name)
        
        # Step A: Expire old versions for changed customers
        if changed_count > 0:
            target.alias("target").merge(
                changed.alias("source"),
                "target.customer_id = source.customer_id AND target.is_current = 1"
            ).whenMatchedUpdate(
                set={
                    "is_current": lit(0),
                    "effective_end_date": current_timestamp()
                }
            ).execute()
            print(f"Expired {changed_count} old versions")
        
        # Step B: Append only changed + new records (not unchanged)
        changed_ids = changed.select("customer_id")
        new_ids = new_customers.select("customer_id")
        ids_to_append = changed_ids.union(new_ids)
        
        customers_to_append = customers_scd_fixed.join(ids_to_append, "customer_id", "inner")
        
        append_count = customers_to_append.count()
        if append_count > 0:
            customers_to_append.write \
                .format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .saveAsTable(table_name)
            print(f"Appended {append_count} new versions")
        else:
            print("No records to append")

print(f"Run mode: {run_mode}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: Update Watermark

# COMMAND ----------

# Update watermark ONLY for full_backfill and incremental
# Do NOT update for partial_backfill (preserve existing watermark)

run_mode = get_run_mode()

if run_mode == "partial_backfill":
    print("PARTIAL BACKFILL: Watermark NOT updated (preserved)")
else:
    spark.sql(f"""
        MERGE INTO {catalog}.{schema}.pipeline_watermarks AS target
        USING (
            SELECT 
                'customers' AS table_name,
                {new_watermark} AS last_file_time,
                current_timestamp() AS last_run_at,
                {len(new_files)} AS files_processed,
                {record_count} AS records_processed
        ) AS source
        ON target.table_name = source.table_name
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    
    print(f"Watermark UPDATED: {new_watermark}")
    print(f"Readable: {datetime.fromtimestamp(new_watermark / 1000)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11: Verify

# COMMAND ----------

final_df = spark.table(table_name)
total_count = final_df.count()
current_count = final_df.filter("is_current = 1").count()
historical_count = final_df.filter("is_current = 0").count()

print("")
print("=" * 50)
print("         CUSTOMERS - WRITE COMPLETE")
print("=" * 50)
print(f"  Run Mode:          {get_run_mode()}")
print(f"  Files Processed:   {len(new_files)}")
print(f"  Records Processed: {record_count}")
print(f"  Total Records:     {total_count}")
print(f"  Current Records:   {current_count}")
print(f"  Historical Records:{historical_count}")
print("=" * 50)

# COMMAND ----------

# MAGIC %sql
# MAGIC select city,is_current from tastybytes.silver.customers  where customer_id=1;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Troubleshooting
# MAGIC
# MAGIC ### Reset Watermark (Force Full Reprocess)
# MAGIC ```sql
# MAGIC DELETE FROM tastybytes.silver.pipeline_watermarks WHERE table_name = 'customers';
# MAGIC ```
# MAGIC
# MAGIC ### Check Watermark Status
# MAGIC ```sql
# MAGIC SELECT * FROM tastybytes.silver.pipeline_watermarks WHERE table_name = 'customers';
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM dwh_tastybytes.silver.customers
# MAGIC WHERE customer_id = 3
# MAGIC ORDER BY modified_date;