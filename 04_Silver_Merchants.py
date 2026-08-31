# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Silver: Merchants (SCD Type 2)
# MAGIC
# MAGIC **SCD Type 2:** Preserves full history of changes.
# MAGIC
# MAGIC **Features:**
# MAGIC - Reads LATEST folder only from ADLS (ADF full extract)
# MAGIC - Deduplication before processing
# MAGIC - SCD2 with hash comparison for change detection
# MAGIC - Supports full_load and partial backfill
# MAGIC - No watermark needed (ADF loads complete data each time)
# MAGIC
# MAGIC **Parameters:**
# MAGIC - `full_load`: Set to "true" for initial load or full rebuild
# MAGIC - `reprocess_start_date` / `reprocess_end_date`: For partial backfill

# COMMAND ----------

# ============================================
# Create Widgets (MUST be before %run)
# ============================================
dbutils.widgets.dropdown("environment", "dev", ["dev", "prod"])
dbutils.widgets.dropdown("full_load", "false", ["true", "false"])
dbutils.widgets.text("reprocess_start_date", "")
dbutils.widgets.text("reprocess_end_date", "")

# COMMAND ----------

# MAGIC %run ./00_Config_and_Utilities

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Setup

# COMMAND ----------

catalog = PipelineConfig.CATALOG
schema = PipelineConfig.SILVER_SCHEMA
table_name = f"{catalog}.{schema}.merchants"
batch_id = datetime.now().strftime("%Y%m%d%H%M%S")

run_mode = get_run_mode()

print("=" * 60)
print("           MERCHANTS PIPELINE - STARTING")
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

bronze_path = f"{PipelineConfig.BRONZE_PATH}merchants/"

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

run_mode = get_run_mode()

bronze_path = f"{PipelineConfig.BRONZE_PATH}merchants/"

# Get latest folder (for incremental)
all_folders = [f for f in dbutils.fs.ls(bronze_path) if f.isDir()]
latest_folder = max(all_folders, key=lambda x: x.modificationTime)

print(f"Run Mode: {run_mode}")

# ============================================
# Read data based on run mode
# ============================================
if run_mode == "full_backfill":
    # Read ALL folders
    print("FULL BACKFILL: Reading ALL folders")
    merchants_bronze = spark.read.parquet(bronze_path)
    
elif run_mode == "partial_backfill":
    # Read ALL folders (need to search across all dates)
    print("PARTIAL BACKFILL: Reading ALL folders (will filter by date)")
    merchants_bronze = spark.read.parquet(bronze_path)
    
else:
    # Incremental: Read only latest folder
    print(f"INCREMENTAL: Reading ONLY latest folder: {latest_folder.path}")
    merchants_bronze = spark.read.parquet(latest_folder.path)

print(f"Loaded {merchants_bronze.count()} rows from Bronze")

# COMMAND ----------

merchants_bronze.show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Deduplicate

# COMMAND ----------

# Check for duplicates before deduplication
dup_check = merchants_bronze.groupBy("merchant_id").count().filter("count > 1")
dup_count = dup_check.count()

if dup_count > 0:
    print(f"Found {dup_count} merchants with duplicate records")
    print("Sample duplicates:")
    dup_check.orderBy(col("count").desc()).show(5)
else:
    print("No duplicates found")

# Deduplicate: Keep latest record per merchant_id
merchants_deduped = remove_duplicates(
    df=merchants_bronze,
    key_columns="merchant_id",
    order_column="modified_date"
)

print(f"After deduplication: {merchants_deduped.count()} unique merchants")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Transform

# COMMAND ----------

# Standardize strings
merchants_clean = standardize_strings(
    df=merchants_deduped,
    columns_config={
        "merchant_name": "initcap",
        "city": "initcap",
        "state": "upper",
        "cuisine_type": "initcap"
    }
)

# Fill nulls
merchants_clean = apply_null_defaults(
    df=merchants_clean,
    null_defaults={
        "city": "Unknown",
        "state": "Unknown",
        "cuisine_type": "Unknown",
        "rating": 0.0
    }
)

# Clamp rating to valid range (0-5)
merchants_clean = merchants_clean.withColumn(
    "rating",
    when(col("rating") < 0, 0)
    .when(col("rating") > 5, 5)
    .otherwise(col("rating"))
)

# Add audit columns
merchants_clean = add_audit_columns(merchants_clean, batch_id)

# Add SCD2 columns
merchants_scd = add_scd2_columns_versioned(
    df=merchants_clean,
    key_column="merchant_id",
    order_column="modified_date"
)

print(f"Transformed records: {merchants_scd.count()}")
print(f"Current records (is_current=1): {merchants_scd.filter('is_current = 1').count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Validate Before Write

# COMMAND ----------

validate_primary_key(merchants_scd, "merchant_id", "merchants")

# Additional validation: Check for duplicate current records
current_dups = merchants_scd.filter("is_current = 1") \
    .groupBy("merchant_id").count().filter("count > 1")

if current_dups.count() > 0:
    print("WARNING: Multiple is_current=1 records per merchant!")
    current_dups.show(5)
    raise Exception("Validation failed: Duplicate current records")
else:
    print("VALIDATION PASSED: Exactly one current record per merchant")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Prepare Target for Backfill

# COMMAND ----------

# For full_backfill: TRUNCATES table (handled by prepare_target_for_backfill)
# For partial_backfill: DELETES affected date range
# For incremental: Does nothing
prepare_target_for_backfill(spark, table_name, date_column="effective_start_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Write to Silver

# COMMAND ----------

# =============================================
# FULL BACKFILL or FIRST RUN
# =============================================
if run_mode == "full_backfill" or not table_exists(spark, table_name):
    print("=" * 50)
    print("FULL BACKFILL: Overwriting entire table")
    print("=" * 50)
    
    merchants_scd.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(table_name)
    
    print(f"Full backfill completed: {merchants_scd.count()} records written")

# =============================================
# PARTIAL BACKFILL
# =============================================
elif run_mode == "partial_backfill":
    print("=" * 50)
    print("PARTIAL BACKFILL: Appending reprocessed records")
    print(f"Date range: {PipelineConfig.REPROCESS_START} to {PipelineConfig.REPROCESS_END}")
    print("=" * 50)
    
    merchants_scd.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable(table_name)
    
    print(f"Partial backfill completed: {merchants_scd.count()} records appended")

# =============================================
# INCREMENTAL (SCD2 MERGE)
# =============================================
else:
    print("=" * 50)
    print("INCREMENTAL: Applying SCD2 MERGE")
    print("=" * 50)
    
    # Get existing current records from target
    existing_current = spark.table(table_name).filter("is_current = 1")
    existing_count = existing_current.count()
    print(f"Existing current records in Silver: {existing_count}")
    
    # Get incoming current records
    incoming_current = merchants_scd.filter("is_current = 1")
    incoming_count = incoming_current.count()
    print(f"Incoming current records: {incoming_count}")
    
    # -----------------------------------------
    # Find CHANGED records (exist in both, hash differs)
    # -----------------------------------------
    changed = incoming_current.alias("new").join(
        existing_current.alias("old"),
        col("new.merchant_id") == col("old.merchant_id"),
        "inner"
    ).filter(col("new.row_hash") != col("old.row_hash")) \
     .select(col("new.merchant_id").alias("merchant_id"))
    
    changed_count = changed.count()
    print(f"Changed records: {changed_count}")
    
    # -----------------------------------------
    # Find NEW records (in incoming but not in existing)
    # -----------------------------------------
    new_merchants = incoming_current.join(
        existing_current,
        "merchant_id",
        "left_anti"
    )
    
    new_count = new_merchants.count()
    print(f"New records: {new_count}")
    
    # -----------------------------------------
    # Skip if no changes
    # -----------------------------------------
    if changed_count == 0 and new_count == 0:
        print("No changes detected - skipping MERGE")
    else:
        target = DeltaTable.forName(spark, table_name)
        
        # Step A: Expire old versions for CHANGED merchants
        if changed_count > 0:
            print(f"Expiring {changed_count} old versions...")
            
            target.alias("target").merge(
                changed.alias("source"),
                "target.merchant_id = source.merchant_id AND target.is_current = 1"
            ).whenMatchedUpdate(
                set={
                    "is_current": lit(0),
                    "effective_end_date": current_timestamp()
                }
            ).execute()
            
            print(f"Expired {changed_count} old versions")
        
        # Step B: Append NEW + CHANGED records
        changed_ids = changed.select("merchant_id")
        new_ids = new_merchants.select("merchant_id")
        ids_to_append = changed_ids.union(new_ids).distinct()
        
        merchants_to_append = merchants_scd.join(ids_to_append, "merchant_id", "inner")
        
        append_count = merchants_to_append.count()
        if append_count > 0:
            print(f"Appending {append_count} new versions...")
            
            merchants_to_append.write \
                .format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .saveAsTable(table_name)
            
            print(f"Appended {append_count} new versions")
        else:
            print("No records to append")

print(f"\nWrite completed for run mode: {run_mode}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Verify

# COMMAND ----------

final_df = spark.table(table_name)
total_count = final_df.count()
current_count = final_df.filter("is_current = 1").count()
historical_count = final_df.filter("is_current = 0").count()
unique_merchants = final_df.select("merchant_id").distinct().count()

print("")
print("=" * 60)
print("           MERCHANTS PIPELINE - COMPLETE")
print("=" * 60)
print("")
print("RUN SUMMARY:")
print(f"  Run Mode:           {run_mode}")
print(f"  Records Processed:  {merchants_scd.count()}")
print("")
print("TABLE STATUS:")
print(f"  Total Records:      {total_count:,}")
print(f"  Current Records:    {current_count:,}")
print(f"  Historical Records: {historical_count:,}")
print(f"  Unique Merchants:   {unique_merchants:,}")
print("")
print("=" * 60)

# Sanity check
if current_count != unique_merchants:
    print(" WARNING: current_count != unique_merchants")
    print("   This may indicate duplicate current records!")
else:
    print("Data integrity check passed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## How to Use This Notebook
# MAGIC
# MAGIC ### 1. FULL BACKFILL (Initial Load or Complete Rebuild)
# MAGIC ```
# MAGIC Parameters:
# MAGIC   full_load             = true
# MAGIC   reprocess_start_date  = (leave empty)
# MAGIC   reprocess_end_date    = (leave empty)
# MAGIC
# MAGIC What happens:
# MAGIC   1. Reads ALL folders from Bronze
# MAGIC   2. Deduplicates (keeps latest per merchant)
# MAGIC   3. TRUNCATES Silver table
# MAGIC   4. OVERWRITES with all data
# MAGIC ```
# MAGIC
# MAGIC ### 2. PARTIAL BACKFILL (Reprocess Specific Date Range)
# MAGIC ```
# MAGIC Parameters:
# MAGIC   full_load             = false
# MAGIC   reprocess_start_date  = 2026-01-15
# MAGIC   reprocess_end_date    = 2026-01-20
# MAGIC
# MAGIC What happens:
# MAGIC   1. Reads LATEST folder from Bronze
# MAGIC   2. FILTERS to modified_date between dates
# MAGIC   3. DELETES from Silver where effective_start_date in range
# MAGIC   4. APPENDS filtered records
# MAGIC ```
# MAGIC
# MAGIC ### 3. INCREMENTAL (Daily Run - Default)
# MAGIC ```
# MAGIC Parameters:
# MAGIC   full_load             = false
# MAGIC   reprocess_start_date  = (leave empty)
# MAGIC   reprocess_end_date    = (leave empty)
# MAGIC
# MAGIC What happens:
# MAGIC   1. Reads LATEST folder from Bronze
# MAGIC   2. Deduplicates
# MAGIC   3. Compares with existing Silver (hash comparison)
# MAGIC   4. Expires changed records (is_current = 0)
# MAGIC   5. Inserts new versions
# MAGIC ```