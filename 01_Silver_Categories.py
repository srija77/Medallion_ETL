# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Silver: Categories (SCD Type 1)
# MAGIC
# MAGIC **SCD Type 1:** Simple overwrite, no history tracking.
# MAGIC Categories rarely change and history is not needed.
# MAGIC
# MAGIC **Features:**
# MAGIC - Reads LATEST folder only from ADLS (ADF full extract)
# MAGIC - Deduplication before processing
# MAGIC - Supports full_load and partial backfill
# MAGIC - No watermark needed (ADF loads complete data each time)
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
table_name = f"{catalog}.{schema}.categories"
batch_id = datetime.now().strftime("%Y%m%d%H%M%S")

run_mode = get_run_mode()

print("=" * 60)
print("           CATEGORIES PIPELINE - STARTING")
print("=" * 60)
print(f"  Target Table: {table_name}")
print(f"  Batch ID:     {batch_id}")
print(f"  Run Mode:     {run_mode}")
if run_mode == "partial_backfill":
    print(f"  Date Range:   {PipelineConfig.REPROCESS_START} to {PipelineConfig.REPROCESS_END}")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Find Latest Folder in ADLS

# COMMAND ----------

bronze_path = f"{PipelineConfig.BRONZE_PATH}categories/"

# List all partition folders
all_folders = [f for f in dbutils.fs.ls(bronze_path) if f.isDir()]

if len(all_folders) == 0:
    print("ERROR: No folders found in Bronze path")
    dbutils.notebook.exit("FAILED: No folders in Bronze")

# Sort by modification time and get latest
latest_folder = max(all_folders, key=lambda x: x.modificationTime)

print(f"Total folders in Bronze: {len(all_folders)}")
print(f"Latest folder: {latest_folder.path}")
print(f"Modified: {datetime.fromtimestamp(latest_folder.modificationTime / 1000)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Load Bronze Data

# COMMAND ----------

# For full_backfill: Read ALL folders to get complete history, then dedupe
# For partial_backfill/incremental: Read only latest folder

if run_mode == "full_backfill":
    # Full backfill: Read all folders, deduplicate
    print("FULL BACKFILL: Reading ALL folders")
    categories_bronze = spark.read.parquet(bronze_path)
else:
    # Partial/Incremental: Read only latest folder
    print(f"Reading ONLY latest folder: {latest_folder.path}")
    categories_bronze = spark.read.parquet(latest_folder.path)

print(f"Loaded {categories_bronze.count()} rows from Bronze")

# Apply date filter for partial backfill
categories_bronze = filter_for_backfill(categories_bronze, date_column="modified_date")
record_count = categories_bronze.count()
print(f"After backfill filter: {record_count} rows")

if record_count == 0:
    print("No records to process - exiting")
    dbutils.notebook.exit("SUCCESS: No records in filter range")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Transform

# COMMAND ----------

# Remove duplicates - keep latest per category_id
categories_clean = remove_duplicates(
    df=categories_bronze,
    key_columns="category_id",
    order_column="modified_date"
)

print(f"After deduplication: {categories_clean.count()} unique categories")

# Standardize strings
categories_clean = standardize_strings(
    df=categories_clean,
    columns_config={
        "category_name": "initcap",
        "category_type": "initcap"
    }
)

# Fill nulls
categories_clean = apply_null_defaults(
    df=categories_clean,
    null_defaults={
        "category_name": "Unknown",
        "category_type": "Unknown"
    }
)

# Add audit columns
categories_clean = add_audit_columns(categories_clean, batch_id)

print(f"Transformed: {categories_clean.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Validate Before Write

# COMMAND ----------

validate_primary_key(categories_clean, "category_id", "categories")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Prepare Target for Backfill

# COMMAND ----------

# For partial_backfill: Delete affected date range
# Note: For SCD1, we can also just overwrite, but this allows selective updates
if run_mode == "partial_backfill" and table_exists(spark, table_name):
    start_date = PipelineConfig.REPROCESS_START
    end_date = PipelineConfig.REPROCESS_END
    
    # Get category_ids in the date range to delete
    category_ids_to_delete = categories_clean.select("category_id").distinct()
    
    print(f"PARTIAL BACKFILL: Deleting {category_ids_to_delete.count()} categories for reprocessing")
    
    # Delete these categories from target
    target = DeltaTable.forName(spark, table_name)
    target.alias("target").merge(
        category_ids_to_delete.alias("source"),
        "target.category_id = source.category_id"
    ).whenMatchedDelete().execute()
    
    print("Deleted affected records")

# COMMAND ----------




# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Write to Silver

# COMMAND ----------

# =============================================
# FULL BACKFILL or FIRST RUN
# =============================================
if run_mode == "full_backfill" or not table_exists(spark, table_name):
    print("=" * 50)
    print("FULL BACKFILL: Overwriting entire table")
    print("=" * 50)
    
    categories_clean.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(table_name)
    
    print(f"Full backfill completed: {categories_clean.count()} records written")

# =============================================
# PARTIAL BACKFILL
# =============================================
elif run_mode == "partial_backfill":
    print("=" * 50)
    print("PARTIAL BACKFILL: Appending reprocessed records")
    print("=" * 50)
    
    categories_clean.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable(table_name)
    
    print(f"Partial backfill completed: {categories_clean.count()} records appended")

# =============================================
# INCREMENTAL (SCD1 - MERGE)
# =============================================
else:
    print("=" * 50)
    print("INCREMENTAL: Applying SCD1 MERGE (upsert)")
    print("=" * 50)
    
    target = DeltaTable.forName(spark, table_name)
    
    # Get columns for update (exclude primary key)
    update_cols = [c for c in categories_clean.columns if c != "category_id"]
    
    target.alias("target").merge(
        categories_clean.alias("source"),
        "target.category_id = source.category_id"
    ).whenMatchedUpdate(
        set={c: f"source.{c}" for c in update_cols}
    ).whenNotMatchedInsertAll().execute()
    
    print("SCD1 MERGE completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Verify

# COMMAND ----------

final_df = spark.table(table_name)
total_count = final_df.count()

print("")
print("=" * 60)
print("           CATEGORIES PIPELINE - COMPLETE")
print("=" * 60)
print(f"  Run Mode:          {run_mode}")
print(f"  Records Processed: {categories_clean.count()}")
print(f"  Total in Table:    {total_count}")
print("=" * 60)