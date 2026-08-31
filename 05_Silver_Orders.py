# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Silver: Orders (CDC Processing)
# MAGIC
# MAGIC **Features:**
# MAGIC - CDC Processing: Handles INSERT, UPDATE, DELETE operations
# MAGIC - Soft Delete: Deleted orders marked as is_deleted=1
# MAGIC - File-based watermark: Only processes new files (incremental)
# MAGIC - Backfill support: Date range reprocessing (no full truncate for CDC!)
# MAGIC
# MAGIC **Parameters:**
# MAGIC - `full_load`: Set to "true" for initial load (base orders only)
# MAGIC - `reprocess_start_date` / `reprocess_end_date`: Reprocess specific date range
# MAGIC
# MAGIC **Important:** For CDC tables, `full_load=true` only reloads base orders.
# MAGIC CDC history cannot be fully rebuilt - only partial backfill by date range.

# COMMAND ----------

# MAGIC %run ./00_Config_and_Utilities

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Setup

# COMMAND ----------

catalog = PipelineConfig.CATALOG
schema = PipelineConfig.SILVER_SCHEMA
table_name = f"{catalog}.{schema}.orders"
batch_id = datetime.now().strftime("%Y%m%d%H%M%S")

print(f"Target Table: {table_name}")
print(f"Batch ID: {batch_id}")
print(f"Run Mode: {get_run_mode()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Setup Watermark Table

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

# Get last watermark
watermark_df = spark.sql(f"""
    SELECT last_file_time 
    FROM {catalog}.{schema}.pipeline_watermarks 
    WHERE table_name = 'orders'
""")

run_mode = get_run_mode()

if run_mode == "full_backfill":
    last_file_time = 0
    print("FULL BACKFILL: Ignoring watermark, processing all files")
elif watermark_df.count() > 0:
    last_file_time = watermark_df.collect()[0][0]
    print(f"Last watermark: {last_file_time}")
    print(f"Readable: {datetime.fromtimestamp(last_file_time / 1000)}")
else:
    last_file_time = 0
    print("No watermark found - first run, processing all files")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Find Files to Process

# COMMAND ----------

bronze_path = f"{PipelineConfig.BRONZE_PATH}orders_cdc/"

# List all folders
all_folders = dbutils.fs.ls(bronze_path)

# Get actual parquet files from inside folders
all_files = []
for folder in all_folders:
    if folder.isDir():
        try:
            files_inside = dbutils.fs.ls(folder.path)
            parquet_files = [f for f in files_inside if f.path.endswith('.parquet')]
            all_files.extend(parquet_files)
        except:
            pass

print(f"Total parquet files found: {len(all_files)}")

# Filter by FILE modification time (not folder time)
if run_mode == "full_backfill":
    new_files = all_files
    print(f"FULL BACKFILL: Processing all {len(new_files)} files")
else:
    new_files = [f for f in all_files if f.modificationTime > last_file_time]
    print(f"Incremental: {len(new_files)} new files to process")

if len(new_files) == 0:
    print("No new files to process - exiting")
    dbutils.notebook.exit("SUCCESS: No new files")

# Get new watermark (will save ONLY after success)
new_watermark = max(f.modificationTime for f in new_files)
print(f"New watermark (pending): {new_watermark}")
print(f"Readable: {datetime.fromtimestamp(new_watermark / 1000)}")


# COMMAND ----------

# Run this in a new cell to debug
bronze_path = f"{PipelineConfig.BRONZE_PATH}orders_cdc/"

all_files = dbutils.fs.ls(bronze_path)
for f in all_files:
    print(f"Path: {f.path}")
    print(f"  ModTime: {f.modificationTime} ({datetime.fromtimestamp(f.modificationTime / 1000)})")
    print(f"  IsDir: {f.isDir()}")
    print()

print(f"Last watermark: {last_file_time} ({datetime.fromtimestamp(last_file_time / 1000)})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Load and Transform

# COMMAND ----------

# Read files
#new_file_paths = [f.path for f in new_files]
#orders_bronze = spark.read.parquet(*new_file_paths)
#display(orders_bronze)
#orders_bronze.printSchema()


# COMMAND ----------

# Read files
new_file_paths = [f.path for f in new_files]
orders_bronze = spark.read.parquet(*new_file_paths)

# Apply date filter for partial backfill
orders_bronze = filter_for_backfill(orders_bronze, date_column="order_date")

record_count = orders_bronze.count()
print(f"Records to process: {record_count}")

if record_count == 0:
    print("No records after filtering - exiting")
    dbutils.notebook.exit("SUCCESS: No records in filter range")

# COMMAND ----------

display(orders_bronze)

# COMMAND ----------

# Standardize strings (only existing columns)
orders_clean = standardize_strings(
    df=orders_bronze,
    columns_config={
        "order_status": "upper"
    }
)

# Fill nulls (only existing columns)
orders_clean = apply_null_defaults(
    df=orders_clean,
    null_defaults={
        "order_status": "PENDING"
    }
)

# Validate amounts
orders_clean = orders_clean.withColumn(
    "total_amount", 
    when(col("total_amount") < 0, 0).otherwise(col("total_amount"))
)

# Add soft delete column
if "is_deleted" not in orders_clean.columns:
    orders_clean = orders_clean.withColumn("is_deleted", lit(0))

# Add audit columns
orders_clean = add_audit_columns(orders_clean, batch_id)

print(f"Transformed: {orders_clean.count()} records")
print(f"Columns: {orders_clean.columns}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Validate Before Write

# COMMAND ----------

validate_primary_key(orders_clean, "order_id", "orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Prepare Target for Partial Backfill

# COMMAND ----------

# For partial backfill, delete affected date range before inserting
# NOTE: We DON'T truncate for orders because CDC data is cumulative
run_mode = get_run_mode()

if run_mode == "partial_backfill" and table_exists(spark, table_name):
    start_date = PipelineConfig.REPROCESS_START
    end_date = PipelineConfig.REPROCESS_END
    
    # Delete orders in the date range
    delete_sql = f"""
        DELETE FROM {table_name} 
        WHERE order_date BETWEEN '{start_date}' AND '{end_date}'
    """
    print(f"Deleting orders from {start_date} to {end_date}")
    spark.sql(delete_sql)
    print("Deleted affected records for reprocessing")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Write to Silver

# COMMAND ----------


# Drop CDC metadata columns - not needed in target
cols_to_drop = [c for c in ["cdc_operation", "cdc_lsn"] if c in orders_clean.columns]
has_cdc_operation = "cdc_operation" in orders_clean.columns

if has_cdc_operation:
    print("CDC data detected")
    orders_clean.groupBy("cdc_operation").count().show()

# =============================================
# FULL BACKFILL or FIRST RUN
# =============================================
if run_mode == "full_backfill" or not table_exists(spark, table_name):
    print("Writing full dataset (overwrite mode)")
    
    # For full load, filter out DELETEs and keep latest per order
    if has_cdc_operation:
        orders_to_write = orders_clean.filter(col("cdc_operation") != "DELETE")
    else:
        orders_to_write = orders_clean
    
    orders_to_write = orders_to_write.drop(*cols_to_drop)
    
    # Deduplicate
    orders_to_write = remove_duplicates(
        df=orders_to_write,
        key_columns="order_id",
        order_column="modified_date"
    )
    
    print(f"Records to write: {orders_to_write.count()}")
    
    orders_to_write.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(table_name)
    
    print("Full load completed")

# =============================================
# PARTIAL BACKFILL
# =============================================
elif run_mode == "partial_backfill":
    print("Partial backfill mode")
    
    if has_cdc_operation:
        orders_to_write = orders_clean.filter(col("cdc_operation") != "DELETE")
    else:
        orders_to_write = orders_clean
    
    orders_to_write = orders_to_write.drop(*cols_to_drop)
    
    # Deduplicate
    orders_to_write = remove_duplicates(
        df=orders_to_write,
        key_columns="order_id",
        order_column="modified_date"
    )
    
    print(f"Records to write: {orders_to_write.count()}")
    
    orders_to_write.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(table_name)
    
    print("Partial backfill completed")

# =============================================
# INCREMENTAL (CDC)
# =============================================
else:
    print("Incremental mode - processing CDC changes")
    
    target = DeltaTable.forName(spark, table_name)
    
    # -----------------------------------------
    # Step A: Handle INSERTs and UPDATEs FIRST
    # -----------------------------------------
    if has_cdc_operation:
        orders_upsert = orders_clean.filter(col("cdc_operation").isin("INSERT", "UPDATE"))
    else:
        orders_upsert = orders_clean
    
    upsert_count = orders_upsert.count()
    
    if upsert_count == 0:
        print("No INSERT/UPDATE records to process")
    else:
        # Drop CDC columns
        orders_to_write = orders_upsert.drop(*cols_to_drop)
        
        # Deduplicate - keep latest per order_id
        orders_to_write = remove_duplicates(
            df=orders_to_write,
            key_columns="order_id",
            order_column="modified_date"
        )
        
        print(f"Records to upsert: {orders_to_write.count()}")
        
        # Align schemas (add missing columns from target)
        target_cols = set(spark.table(table_name).columns)
        source_cols = set(orders_to_write.columns)
        missing_cols = target_cols - source_cols
        
        for col_name in missing_cols:
            print(f"Adding missing column: {col_name}")
            if col_name in ["load_date", "_loaded_at", "_processed_at"]:
                orders_to_write = orders_to_write.withColumn(col_name, current_timestamp())
            else:
                orders_to_write = orders_to_write.withColumn(col_name, lit(None))
        
        # MERGE for INSERT/UPDATE
        update_cols = [c for c in orders_to_write.columns if c != "order_id"]
        
        target.alias("target").merge(
            orders_to_write.alias("source"),
            "target.order_id = source.order_id"
        ).whenMatchedUpdate(
            set={c: f"source.{c}" for c in update_cols}
        ).whenNotMatchedInsertAll().execute()
        
        print("INSERT/UPDATE MERGE completed")
    
    # -----------------------------------------
    # Step B: Handle DELETEs LAST (soft delete wins)
    # -----------------------------------------
    if has_cdc_operation:
        deletes = orders_clean.filter(col("cdc_operation") == "DELETE") \
            .select("order_id").distinct()
        
        delete_count = deletes.count()
        
        if delete_count > 0:
            print(f"Soft deleting {delete_count} orders")
            
            target.alias("target").merge(
                deletes.alias("source"),
                "target.order_id = source.order_id"
            ).whenMatchedUpdate(
                set={
                    "is_deleted": lit(1),
                    "_processed_at": current_timestamp()
                }
            ).execute()
            
            print("Soft deletes applied")
        else:
            print("No DELETE records to process")



# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Update Watermark (ONLY after success)

# COMMAND ----------

# Now safe to update watermark
spark.sql(f"""
    MERGE INTO {catalog}.{schema}.pipeline_watermarks AS target
    USING (
        SELECT 
            'orders' AS table_name,
            {new_watermark} AS last_file_time,
            current_timestamp() AS last_run_at,
            {len(new_files)} AS files_processed,
            {record_count} AS records_processed
    ) AS source
    ON target.table_name = source.table_name
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

print(f"Watermark updated: {new_watermark}")
print(f"Readable: {datetime.fromtimestamp(new_watermark / 1000)}")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tastybytes.silver.pipeline_watermarks;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: Verify

# COMMAND ----------

final_df = spark.table(table_name)
total_count = final_df.count()
active_count = final_df.filter("is_deleted = 0").count()
deleted_count = final_df.filter("is_deleted = 1").count()

print("")
print("=" * 50)
print("         ORDERS - WRITE COMPLETE")
print("=" * 50)
print(f"  Run Mode:          {get_run_mode()}")
print(f"  Files Processed:   {len(new_files)}")
print(f"  Records Processed: {record_count}")
print(f"  Total in Table:    {total_count}")
print(f"  Active Records:    {active_count}")
print(f"  Soft Deleted:      {deleted_count}")
print("=" * 50)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Backfill Notes for Orders (CDC Table)
# MAGIC
# MAGIC **Why no full truncate?**
# MAGIC - Orders uses CDC (Change Data Capture)
# MAGIC - CDC only contains recent changes, not full history
# MAGIC - If you truncate, you can't rebuild from CDC alone
# MAGIC
# MAGIC **Backfill options:**
# MAGIC
# MAGIC | Parameter | What Happens |
# MAGIC |-----------|--------------|
# MAGIC | `full_load=true` | Reloads base orders from `orders/` folder only |
# MAGIC | `reprocess_start_date` + `reprocess_end_date` | Deletes and reprocesses specific date range |
# MAGIC | (default) | Incremental - processes only new files |
# MAGIC
# MAGIC **To fully rebuild orders:**
# MAGIC 1. Keep base load files forever in `orders/` folder
# MAGIC 2. Set `full_load=true` to reload base
# MAGIC 3. Run again with `full_load=false` to apply CDC
# MAGIC
# MAGIC **To fix specific dates:**
# MAGIC ```
# MAGIC reprocess_start_date = "2026-01-15"
# MAGIC reprocess_end_date = "2026-01-20"
# MAGIC ```
# MAGIC This deletes orders in that range and reprocesses from Bronze.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM tastybytes.silver.orders WHERE order_status = 'CANCELLED' order by order_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM tastybytes.silver.orders order by order_id desc;
# MAGIC
# MAGIC