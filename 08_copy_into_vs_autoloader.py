# Databricks notebook source
# MAGIC %md
# MAGIC # Data Ingestion: COPY INTO vs Auto Loader
# MAGIC
# MAGIC This notebook compares two approaches for ingesting data from ADLS.
# MAGIC
# MAGIC | Approach | Type | Trigger |
# MAGIC |----------|------|---------|
# MAGIC | COPY INTO | Batch | Manual/Scheduled |
# MAGIC | Auto Loader | Streaming | Event-based (automatic) |

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 1: Configuration

# COMMAND ----------

# Storage Configuration
STORAGE_ACCOUNT = "tastybytesstgacc"
CONTAINER = "bronze"

# Paths
LANDING_PATH = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/landing/products/"
CHECKPOINT_PATH = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/checkpoints/autoloader/"

# Table Names (catalog.schema.table)
CATALOG = "tastybytes"
SCHEMA = "bronze"
BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.products_raw"

print(f"Landing Path: {LANDING_PATH}")
print(f"Checkpoint Path: {CHECKPOINT_PATH}")
print(f"Bronze Table: {BRONZE_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 2: Setup - Create Catalog, Schema, and Table
# MAGIC
# MAGIC **Run this BEFORE using COPY INTO or Auto Loader**

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create catalog (if not exists)
# MAGIC CREATE CATALOG IF NOT EXISTS tastybytes;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Use the catalog
# MAGIC USE CATALOG tastybytes;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create schema (database)
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify setup
# MAGIC SELECT current_catalog(), current_schema();

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create empty Delta table for COPY INTO
# MAGIC -- COPY INTO requires table to exist first!
# MAGIC CREATE TABLE IF NOT EXISTS tastybytes.bronze.products_copyinto_demo
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true');

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # OPTION 1: COPY INTO (Batch)
# MAGIC ---
# MAGIC
# MAGIC ### How it works:
# MAGIC - Runs when YOU trigger it (manually or scheduled)
# MAGIC - Scans directory for files
# MAGIC - Tracks loaded files in table history
# MAGIC - Skips already-loaded files
# MAGIC
# MAGIC ### Pros:
# MAGIC - Simple SQL syntax
# MAGIC - Easy to understand
# MAGIC - Good for initial/historical loads
# MAGIC
# MAGIC ### Cons:
# MAGIC - Not real-time
# MAGIC - Must be scheduled or triggered manually
# MAGIC - Scans file list every run (slower with many files)

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE IF EXISTS tastybytes.bronze.products_copyinto;
# MAGIC
# MAGIC CREATE TABLE tastybytes.bronze.products_copyinto (
# MAGIC     
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- COPY INTO: Simple batch loading
# MAGIC -- ============================================================
# MAGIC -- Run this manually or on a schedule
# MAGIC -- It will only load NEW files (tracks what's been loaded)
# MAGIC
# MAGIC COPY INTO tastybytes.bronze.products_copyinto_demo
# MAGIC FROM 'abfss://bronze@tastybytesstgacc.dfs.core.windows.net/landing/products/'
# MAGIC FILEFORMAT = JSON
# MAGIC FORMAT_OPTIONS (
# MAGIC     'inferSchema' = 'true',
# MAGIC     'multiLine' = 'false'
# MAGIC )
# MAGIC COPY_OPTIONS (
# MAGIC     'mergeSchema' = 'true',
# MAGIC     'force' = 'false'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ### COPY INTO with Python

# COMMAND ----------

# Python version of COPY INTO
spark.sql(f"""
    COPY INTO {BRONZE_TABLE}
    FROM '{LANDING_PATH}'
    FILEFORMAT = JSON
    FORMAT_OPTIONS ('inferSchema' = 'true')
    COPY_OPTIONS ('mergeSchema' = 'true')
""")

print(" COPY INTO complete!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify Data Loaded

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) as total_records FROM tastybytes.bronze.products_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM tastybytes.bronze.products_copyinto LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # OPTION 2: AUTO LOADER (Streaming - Event Based)
# MAGIC ---
# MAGIC
# MAGIC ### How it works:
# MAGIC - Runs CONTINUOUSLY (always listening)
# MAGIC - Automatically detects new files as they arrive
# MAGIC - Processes immediately (event-driven)
# MAGIC - Uses checkpointing (exactly-once guarantee)
# MAGIC
# MAGIC ### Two Modes:
# MAGIC
# MAGIC | Mode | How it detects files | Best for |
# MAGIC |------|---------------------|----------|
# MAGIC | **Directory Listing** | Periodically lists directory | < 1 million files |
# MAGIC | **File Notification** | Azure Event Grid pushes events | > 1 million files |
# MAGIC
# MAGIC ### Pros:
# MAGIC - Real-time / near real-time
# MAGIC - Automatic (no scheduling needed)
# MAGIC - Exactly-once processing
# MAGIC - Scales to billions of files
# MAGIC
# MAGIC ### Cons:
# MAGIC - Cluster must be running (costs money)
# MAGIC - Slightly more complex setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Auto Loader Mode 1: Directory Listing (Default)
# MAGIC
# MAGIC - Periodically scans directory for new files
# MAGIC - Simple setup, no Azure configuration needed
# MAGIC - Good for most use cases

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col

# ============================================================
# AUTO LOADER: Directory Listing Mode (Default)
# ============================================================
# Runs continuously, checks for new files periodically
# No Azure Event Grid setup required

df_stream = (
    spark.readStream
    .format("cloudFiles")
    
    # Source format
    .option("cloudFiles.format", "json")
    
    # Schema handling
    .option("cloudFiles.schemaLocation", f"{CHECKPOINT_PATH}/schema")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    
    # Directory listing mode (default)
    .option("cloudFiles.useNotifications", "false")
    
    # Load from landing path
    .load(LANDING_PATH)
    
    # Add metadata columns (Unity Catalog compatible)
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file", col("_metadata.file_path"))
)

# Write stream - runs continuously
query = (
    df_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_PATH}/bronze")
    .option("mergeSchema", "true")
    
    # EVENT-BASED: Check for new files every 10 seconds
    .trigger(processingTime="10 seconds")
    
    .toTable(BRONZE_TABLE)
)

print(" Auto Loader started! Listening for new files...")
print("   Stop with: query.stop()")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check Stream Status

# COMMAND ----------

# Check if stream is running
print(f"Is Active: {query.isActive}")
print(f"Status: {query.status}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stop the Stream (When Done Testing)

# COMMAND ----------

# Uncomment to stop
# query.stop()
# print(" Stream stopped")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Auto Loader Mode 2: File Notification (True Event-Driven)
# MAGIC
# MAGIC - Azure Event Grid notifies Databricks when files arrive
# MAGIC - Truly event-driven (no polling)
# MAGIC - Requires Azure configuration

# COMMAND ----------

# ============================================================
# AUTO LOADER: File Notification Mode (True Event-Based)
# ============================================================
# Azure Event Grid pushes file arrival events
# Requires Azure configuration

# PREREQUISITE: Set up Azure Event Grid
# 1. Azure Portal → Storage Account → Events
# 2. Create Event Subscription:
#    - Event Types: Blob Created
#    - Endpoint: Storage Queue
# 3. Grant Databricks access to the queue

df_stream_events = (
    spark.readStream
    .format("cloudFiles")
    
    # Source format
    .option("cloudFiles.format", "json")
    
    # Schema handling
    .option("cloudFiles.schemaLocation", f"{CHECKPOINT_PATH}/schema_events")
    .option("cloudFiles.inferColumnTypes", "true")
    
    # FILE NOTIFICATION MODE - True event-driven!
    .option("cloudFiles.useNotifications", "true")
    
    # Azure connection (required for notification mode)
    # Uncomment and fill in your values:
    # .option("cloudFiles.resourceGroup", "<YOUR_RESOURCE_GROUP>")
    # .option("cloudFiles.subscriptionId", "<YOUR_SUBSCRIPTION_ID>")
    # .option("cloudFiles.tenantId", "<YOUR_TENANT_ID>")
    
    .load(LANDING_PATH)
    
    # Add metadata columns (Unity Catalog compatible)
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file", col("_metadata.file_path"))
)

# Write stream - truly event-driven
query_events = (
    df_stream_events.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_PATH}/bronze_events")
    .option("mergeSchema", "true")
    .toTable(BRONZE_TABLE)
)

print(" Auto Loader (Event Mode) started!")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Trigger Options 

# COMMAND ----------

# ============================================================
# TRIGGER OPTIONS - Control when processing happens
# ============================================================

# OPTION A: Process continuously (true streaming)
# Checks for new files every N seconds
# Cluster must stay running
# .trigger(processingTime="10 seconds")

# OPTION B: Process all available and stop (batch-like)
# Good for scheduled jobs
# Cluster can terminate after
# .trigger(availableNow=True)

# OPTION C: Continuous with micro-batches (lowest latency)
# For sub-second latency requirements
# .trigger(continuous="1 second")

print("See comments above for trigger options")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Continous Auto Loader
# MAGIC ---

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col


def start_auto_loader(
    landing_path: str,
    checkpoint_path: str,
    target_table: str,
    trigger_interval: str = "10 seconds"
):
    """
    Starts Auto Loader that processes files as they arrive.
    Returns the streaming query handle.
    """
    
    # Read stream with Auto Loader
    df = (
        spark.readStream
        .format("cloudFiles")
        
        # Format and schema
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{checkpoint_path}/schema")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        
        # Include existing files on first run
        .option("cloudFiles.includeExistingFiles", "true")
        
        # Recursively look in subdirectories (year=/month=/day=)
        .option("recursiveFileLookup", "true")
        
        .load(landing_path)
        
        # Add audit columns (Unity Catalog compatible)
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn("_file_modification_time", col("_metadata.file_modification_time"))
    )
    
    # Write stream
    query = (
        df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{checkpoint_path}/bronze")
        .option("mergeSchema", "true")
        .trigger(processingTime=trigger_interval)
        .queryName("bronze_auto_loader")
        .toTable(target_table)
    )
    
    return query


# Start the Auto Loader
query = start_auto_loader(
    landing_path=LANDING_PATH,
    checkpoint_path=CHECKPOINT_PATH,
    target_table=BRONZE_TABLE,
    trigger_interval="10 seconds"
)

print(" Auto Loader running!")
print(f"   Query ID: {query.id}")
print(f"   Status: {query.status}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitor the Stream

# COMMAND ----------

# Check stream status
if query.isActive:
    print(f" Stream is ACTIVE")
    print(f"   Status: {query.status}")
else:
    print(" Stream is NOT active")

# COMMAND ----------

# MAGIC %md
# MAGIC ## View Processed Files

# COMMAND ----------

# MAGIC %sql
# MAGIC -- See what files have been processed
# MAGIC SELECT 
# MAGIC     _source_file,
# MAGIC     _ingested_at,
# MAGIC     COUNT(*) as records
# MAGIC FROM tastybytes.bronze.products_raw
# MAGIC GROUP BY _source_file, _ingested_at
# MAGIC ORDER BY _ingested_at DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stop the Stream

# COMMAND ----------

# Stop the streaming query when done
# query.stop()
# print(" Stream stopped")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Auto Loader with availableNow (Batch-Like)
# MAGIC ---
# MAGIC
# MAGIC Use this when you want Auto Loader benefits but don't need continuous streaming.
# MAGIC Perfect for scheduled jobs!

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col

# ============================================================
# AUTO LOADER: Batch-like mode (process and stop)
# ============================================================
# Perfect for scheduled Databricks jobs

def run_batch_auto_loader():
    """Run Auto Loader once and stop - like COPY INTO but better."""
    
    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{CHECKPOINT_PATH}/schema_batch")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("recursiveFileLookup", "true")
        .load(LANDING_PATH)
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )
    
    query = (
        df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/bronze_batch")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)  # Process all available, then STOP
        .toTable(BRONZE_TABLE)
    )
    
    # Wait for completion
    query.awaitTermination()
    print(" Batch Auto Loader complete!")


# Run it
run_batch_auto_loader()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) as total_records FROM tastybytes.bronze.products_raw;

# COMMAND ----------

run_batch_auto_loader()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Summary: COPY INTO vs Auto Loader
# MAGIC ---
# MAGIC
# MAGIC | Feature | COPY INTO | Auto Loader |
# MAGIC |---------|-----------|-------------|
# MAGIC | **Trigger** | Manual/Scheduled | Event-based (automatic) 
# MAGIC | **Real-time** |  No | Yes |
# MAGIC | **File Tracking** | Table history | Checkpoint |
# MAGIC | **Scalability** | Millions | Billions |
# MAGIC | **Cluster Required** | Only during run | Must stay running* |
# MAGIC | **Setup** | Simple | Slightly more |
# MAGIC | **Best For** | Scheduled batch | Real-time ingestion |
# MAGIC
# MAGIC *Unless using `trigger(availableNow=True)`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select *from tastybytes.bronze.products_raw;

# COMMAND ----------

# MAGIC  %md
# MAGIC #  Recursive Flattener
# MAGIC # Works for ANY nested/semi-structured data

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, ArrayType, MapType
from pyspark.sql.functions import (
    col, explode_outer, map_keys, map_values,
    current_timestamp, posexplode_outer
)

# COMMAND ----------

# Configuration
CATALOG = "tastybytes"
BRONZE_TABLE = f"{CATALOG}.bronze.products_raw"
SILVER_TABLE = f"{CATALOG}.silver.products_clean"



# %sql
# CREATE SCHEMA IF NOT EXISTS tastybytes.silver;

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col, lit
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, ArrayType, MapType

# ============================================================
# Generic Flattening Functions
# ============================================================

def flatten_struct(df: DataFrame, separator: str = "_") -> DataFrame:
    complex_cols = True
    while complex_cols:
        complex_cols = False
        new_cols = []
        for field in df.schema.fields:
            col_name = field.name
            if isinstance(field.dataType, StructType):
                complex_cols = True
                for sub_field in field.dataType.fields:
                    new_col_name = f"{col_name}{separator}{sub_field.name}"
                    new_cols.append(col(f"`{col_name}`.`{sub_field.name}`").alias(new_col_name))
            else:
                new_cols.append(col(f"`{col_name}`"))
        df = df.select(new_cols)
    return df


def explode_arrays(df: DataFrame, separator: str = "_") -> DataFrame:
    array_found = True
    while array_found:
        array_found = False
        for field in df.schema.fields:
            if isinstance(field.dataType, ArrayType):
                array_found = True
                col_name = field.name
                if isinstance(field.dataType.elementType, StructType):
                    df = df.withColumn(col_name, explode_outer(col(f"`{col_name}`")))
                    df = flatten_struct(df, separator)
                else:
                    df = df.withColumn(col_name, explode_outer(col(f"`{col_name}`")))
                break
    return df


def explode_maps(df: DataFrame, separator: str = "_") -> DataFrame:
    map_found = True
    while map_found:
        map_found = False
        for field in df.schema.fields:
            if isinstance(field.dataType, MapType):
                map_found = True
                col_name = field.name
                df = (
                    df
                    .withColumn(f"{col_name}{separator}key", explode_outer(map_keys(col(f"`{col_name}`"))))
                    .withColumn(f"{col_name}{separator}value", explode_outer(map_values(col(f"`{col_name}`"))))
                    .drop(col_name)
                )
                df = flatten_struct(df, separator)
                break
    return df


def flatten_df(df: DataFrame, separator: str = "_") -> DataFrame:
    df = flatten_struct(df, separator)
    df = explode_arrays(df, separator)
    df = explode_maps(df, separator)
    df = flatten_struct(df, separator)
    return df


# ============================================================
# foreachBatch: Flatten + Write to Silver
# ============================================================

SILVER_TABLE = f"{CATALOG}.silver.products_clean"

def process_batch(batch_df: DataFrame, batch_id: int):
    if batch_df.isEmpty():
        return

    df_flat = flatten_df(batch_df)
    df_flat = (
        df_flat
        .withColumn("_silver_loaded_at", current_timestamp())
        .withColumn("_batch_id", lit(batch_id))
    )

    (
        df_flat.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(SILVER_TABLE)
    )
    print(f"Batch {batch_id}: {df_flat.count()} rows → silver")


# ============================================================
# Auto Loader → Flatten → Silver (batch mode)
# ============================================================

def run_batch_auto_loader():
    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{CHECKPOINT_PATH}/schema_batch")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("recursiveFileLookup", "true")
        .load(LANDING_PATH)
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )

    query = (
        df.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/silver_batch")
        .trigger(availableNow=True)
        .queryName("silver_batch_loader")
        .start()
    )

    query.awaitTermination()
    print("Auto Loader → Silver complete!")


# Run it
run_batch_auto_loader()