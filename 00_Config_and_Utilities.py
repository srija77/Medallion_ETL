# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Configuration and Utilities
# MAGIC
# MAGIC Shared configuration and helper functions for the medallion pipeline.
# MAGIC
# MAGIC **Production Features:**
# MAGIC - Environment switching (dev/prod)
# MAGIC - Backfill support (full and partial)
# MAGIC - Reusable utility functions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1: Parameters

# COMMAND ----------

# ============================================
# Define widgets WITHOUT resetting existing values
# ============================================

# Only create widgets if they don't exist
try:
    _ = dbutils.widgets.get("environment")
except:
    dbutils.widgets.dropdown("environment", "dev", ["dev", "prod"])

try:
    _ = dbutils.widgets.get("full_load")
except:
    dbutils.widgets.dropdown("full_load", "false", ["true", "false"])

try:
    _ = dbutils.widgets.get("reprocess_start_date")
except:
    dbutils.widgets.text("reprocess_start_date", "")

try:
    _ = dbutils.widgets.get("reprocess_end_date")
except:
    dbutils.widgets.text("reprocess_end_date", "")

# Now read the values
ENV = dbutils.widgets.get("environment")
FULL_LOAD = dbutils.widgets.get("full_load") == "false"
REPROCESS_START = dbutils.widgets.get("reprocess_start_date")
REPROCESS_END = dbutils.widgets.get("reprocess_end_date")

# Determine run mode
if FULL_LOAD:
    RUN_MODE = "full_backfill"
elif REPROCESS_START and REPROCESS_END:
    RUN_MODE = "partial_backfill"
else:
    RUN_MODE = "incremental"

print(f"Environment: {ENV}")
print(f"Run Mode: {RUN_MODE}")
if RUN_MODE == "partial_backfill":
    print(f"Reprocess Range: {REPROCESS_START} to {REPROCESS_END}")


# COMMAND ----------

# Remove widget (if needed)
#dbutils.widgets.remove("reprocess_start_date")

# Remove all widgets
#dbutils.widgets.removeAll()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2: Configuration

# COMMAND ----------

# MAGIC %sql
# MAGIC show catalogs;

# COMMAND ----------

class PipelineConfig:
    """Central configuration for the pipeline."""
    
    # Environment
    ENV = dbutils.widgets.get("environment")
    
    # Storage - UPDATE THESE VALUES
    if ENV == "prod":
        STORAGE_ACCOUNT = "tastybytesstorage"
        CATALOG = "dwh_tastybytes"
    else:
        STORAGE_ACCOUNT = "tastybytesstorage"
        CATALOG = "dwh_tastybytes"
    
    BRONZE_CONTAINER = "bronze"
    BRONZE_PATH = f"abfss://{BRONZE_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/"
    
    # Schemas
    SILVER_SCHEMA = "silver"
    GOLD_SCHEMA = "gold"
    
    # Run mode flags - Use @property to read fresh each time
    @property
    def FULL_LOAD(self):
        return dbutils.widgets.get("full_load") == "true"
    
    @property
    def REPROCESS_START(self):
        return dbutils.widgets.get("reprocess_start_date")
    
    @property
    def REPROCESS_END(self):
        return dbutils.widgets.get("reprocess_end_date")


print(f"Catalog: {PipelineConfig.CATALOG}")
print(f"Bronze Path: {PipelineConfig.BRONZE_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3: Imports

# COMMAND ----------

from pyspark.sql.functions import (
    col, lit, current_timestamp, current_date, to_timestamp, to_date,
    trim, upper, lower, initcap,
    coalesce, when, isnull,
    row_number, rank, dense_rank, lead, lag, desc, asc,
    sha2, concat, concat_ws, md5,
    year, quarter, month, dayofmonth, dayofweek, weekofyear, date_format,
    min as spark_min, max as spark_max, sum as spark_sum, avg as spark_avg,
    count, countDistinct, 
    explode, sequence
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime, timedelta

print("Libraries imported")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 4: Helper Functions

# COMMAND ----------

def table_exists(spark, table_name):
    """Check if a table exists."""
    try:
        spark.table(table_name).limit(1).count()
        return True
    except:
        return False


def remove_duplicates(df, key_columns, order_column):
    """Remove duplicates keeping the latest record."""
    if isinstance(key_columns, str):
        key_columns = [key_columns]
    window = Window.partitionBy(key_columns).orderBy(desc(order_column))
    return df.withColumn("_rn", row_number().over(window)) \
             .filter(col("_rn") == 1) \
             .drop("_rn")


def add_audit_columns(df, batch_id):
    """Add audit columns for tracking."""
    return df \
        .withColumn("_processed_at", current_timestamp()) \
        .withColumn("_batch_id", lit(batch_id))


def validate_primary_key(df, key_column, table_name):
    """
    Validate primary key is not null.
    Raises exception if validation fails.
    """
    null_count = df.filter(col(key_column).isNull()).count()
    if null_count > 0:
        raise Exception(f"VALIDATION FAILED: {table_name} has {null_count} null {key_column}s")
    print(f"VALIDATION PASSED: {table_name}.{key_column} has no nulls")
    return True
def get_run_mode():
    """Determine run mode based on widget parameters - reads fresh each time"""
    # Read widgets fresh (not from PipelineConfig class attributes)
    full_load = dbutils.widgets.get("full_load") == "true"
    reprocess_start = dbutils.widgets.get("reprocess_start_date")
    reprocess_end = dbutils.widgets.get("reprocess_end_date")
    
    if full_load:
        return "full_backfill"
    elif reprocess_start and reprocess_end:
        return "partial_backfill"
    else:
        return "incremental"



print("Helper functions defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 5: Transformation Functions

# COMMAND ----------

def standardize_strings(df, columns_config):
    """
    Standardize string columns.
    columns_config: {"column_name": "initcap|upper|lower"}
    """
    result = df
    for col_name, transform_type in columns_config.items():
        if col_name in result.columns:
            if transform_type == 'initcap':
                result = result.withColumn(col_name, initcap(trim(col(col_name))))
            elif transform_type == 'upper':
                result = result.withColumn(col_name, upper(trim(col(col_name))))
            elif transform_type == 'lower':
                result = result.withColumn(col_name, lower(trim(col(col_name))))
    return result


def apply_null_defaults(df, null_defaults):
    """
    Replace nulls with default values.
    null_defaults: {"column_name": default_value}
    """
    result = df
    for col_name, default_val in null_defaults.items():
        if col_name in result.columns:
            result = result.fillna({col_name: default_val})
    return result

def add_scd2_columns_versioned(df, key_column, order_column="modified_date"):
    # Exclude ALL technical/audit columns from hash
    exclude_from_hash = [
        key_column,
        order_column,
        # Audit columns
        "_processed_at", "_batch_id", "_pipeline", "_loaded_at", "load_date",
        # Timestamps
        "created_date", "modified_date",
        # SCD columns
        "effective_start_date", "effective_end_date", "is_current", "row_hash", "version"
    ]
    
    hash_cols = [c for c in df.columns if c not in exclude_from_hash]
    print(f"Row hash columns: {hash_cols}")




print("Transformation functions defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 6: Backfill Utilities

# COMMAND ----------

def filter_for_backfill(df, date_column="modified_date"):
    """
    Filter dataframe based on run mode and backfill parameters.
    """
    run_mode = get_run_mode()
    
    if run_mode == "full_backfill":
        print(f"FULL BACKFILL: Processing all data (no date filter)")
        return df
    
    elif run_mode == "partial_backfill":
        # READ DIRECTLY FROM WIDGETS - NOT from PipelineConfig!
        start_date = dbutils.widgets.get("reprocess_start_date")
        end_date = dbutils.widgets.get("reprocess_end_date")
        
        print(f"PARTIAL BACKFILL: Filtering {date_column} between {start_date} and {end_date}")
        
        return df.filter(
            col(date_column).between(
                to_timestamp(lit(start_date)), 
                to_timestamp(lit(end_date + " 23:59:59"))
            )
        )
    
    else:
        # Incremental - return all, filtering done elsewhere
        print("INCREMENTAL: No date filter applied")
        return df


def prepare_target_for_backfill(spark, table_name, date_column="effective_start_date"):
    """
    Prepare target table for backfill by deleting affected records.
    """
    run_mode = get_run_mode()
    
    if not table_exists(spark, table_name):
        print(f"Table {table_name} does not exist - nothing to prepare")
        return
    
    if run_mode == "full_backfill":
        print(f"FULL BACKFILL: Truncating {table_name}")
        spark.sql(f"TRUNCATE TABLE {table_name}")
        print("Table truncated")
    
    elif run_mode == "partial_backfill":
        # READ DIRECTLY FROM WIDGETS!
        start_date = dbutils.widgets.get("reprocess_start_date")
        end_date = dbutils.widgets.get("reprocess_end_date")
        
        print(f"PARTIAL BACKFILL: Deleting from {table_name} where {date_column} between {start_date} and {end_date}")
        spark.sql(f"""
            DELETE FROM {table_name} 
            WHERE {date_column} BETWEEN '{start_date}' AND '{end_date} 23:59:59'
        """)
        print("Affected records deleted")
    
    else:
        print("INCREMENTAL: No preparation needed")


def add_scd2_columns_versioned(df, key_column, order_column="modified_date"):
    """
    Add SCD Type 2 columns with proper versioning.
    Handles multiple updates for same key in single batch.
    
    Creates:
    - effective_start_date: When this version became active
    - effective_end_date: When this version expired (9999-12-31 if current)
    - is_current: 1 if this is the latest version, 0 otherwise
    - row_hash: Hash of BUSINESS columns only for change detection
    - version: Version number within batch
    """
    
    # Exclude ALL technical/audit columns from hash - ONLY business data should trigger changes
    exclude_from_hash = [
        # Keys
        key_column,
        order_column,
        # Audit columns (change every run)
        "_processed_at",
        "_batch_id",
        "_pipeline",
        "_loaded_at",
        "load_date",
        # Timestamps (don't indicate business change)
        "created_date",
        "modified_date",
        # SCD columns
        "effective_start_date",
        "effective_end_date",
        "is_current",
        "row_hash",
        "version"
    ]
    
    # Only BUSINESS columns in hash
    hash_cols = [c for c in df.columns if c not in exclude_from_hash]
    print(f"Row hash columns ({len(hash_cols)}): {hash_cols}")
    
    # Create hash from business columns only
    df_with_hash = df.withColumn(
        "row_hash",
        sha2(concat_ws("||", *[coalesce(col(c).cast("string"), lit("")) for c in hash_cols]), 256)
    )
    
    # Window for versioning within batch
    window = Window.partitionBy(key_column).orderBy(col(order_column))
    window_desc = Window.partitionBy(key_column).orderBy(col(order_column).desc())
    
    # Add version and is_current
    df_versioned = df_with_hash \
        .withColumn("version", row_number().over(window)) \
        .withColumn("_max_version", row_number().over(window_desc)) \
        .withColumn("is_current", when(col("_max_version") == 1, 1).otherwise(0)) \
        .drop("_max_version")
    
    # Add effective dates
    df_final = df_versioned \
        .withColumn("effective_start_date", col(order_column).cast("timestamp")) \
        .withColumn("effective_end_date", 
            coalesce(
                lead(col(order_column)).over(window).cast("timestamp"),
                to_timestamp(lit("9999-12-31 23:59:59"))
            )
        )
    
    return df_final


print("Backfill and SCD2 utilities defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("")
print("=" * 60)
print("       CONFIG AND UTILITIES LOADED")
print("=" * 60)
print("")
print("PARAMETERS:")
print(f"  environment:           {ENV}")
print(f"  full_load:             {FULL_LOAD}")
print(f"  reprocess_start_date:  {REPROCESS_START or '(not set)'}")
print(f"  reprocess_end_date:    {REPROCESS_END or '(not set)'}")
print(f"  RUN MODE:              {RUN_MODE}")
print("")
print("AVAILABLE FUNCTIONS:")
print("  table_exists()              - Check if table exists")
print("  remove_duplicates()         - Deduplicate DataFrame")
print("  add_audit_columns()         - Add tracking columns")
print("  validate_primary_key()      - Check primary key not null")
print("  standardize_strings()       - Clean string columns")
print("  apply_null_defaults()       - Fill null values")
print("  add_scd2_columns_versioned()- Add SCD2 columns (handles multiple updates)")
print("  filter_for_backfill()       - Filter data for backfill mode")
print("  prepare_target_for_backfill()- Prepare target table for backfill")
print("=" * 60)