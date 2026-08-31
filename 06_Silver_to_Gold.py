# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Silver to Gold
# MAGIC
# MAGIC Creates Gold layer with Star Schema:
# MAGIC - Dimension tables (who, what, when, where)
# MAGIC - Fact table (transactions)
# MAGIC - Basic daily aggregation (for dashboards)

# COMMAND ----------

# MAGIC %run ./00_Config_and_Utilities

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1: Setup

# COMMAND ----------

catalog = PipelineConfig.CATALOG
silver_schema = PipelineConfig.SILVER_SCHEMA
gold_schema = PipelineConfig.GOLD_SCHEMA

# Create gold schema if not exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{gold_schema}")

batch_id = datetime.now().strftime("%Y%m%d%H%M%S")
print(f"Batch ID: {batch_id}")
print(f"Gold Schema: {catalog}.{gold_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2: Load Silver Tables

# COMMAND ----------

categories = spark.table(f"{catalog}.{silver_schema}.categories")
riders = spark.table(f"{catalog}.{silver_schema}.riders")
customers = spark.table(f"{catalog}.{silver_schema}.customers")
merchants = spark.table(f"{catalog}.{silver_schema}.merchants")
orders = spark.table(f"{catalog}.{silver_schema}.orders")

print("Silver tables loaded:")
print(f"  categories: {categories.count()}")
print(f"  riders: {riders.count()}")
print(f"  customers (current): {customers.filter('is_current = 1').count()}")
print(f"  merchants (current): {merchants.filter('is_current = 1').count()}")
print(f"  orders (active): {orders.filter('is_deleted = 0').count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3: Create Dimensions

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 dim_date

# COMMAND ----------

# Get date range from orders (dynamic)
date_range = orders.select(
    spark_min("order_date").alias("min_date"),
    spark_max("order_date").alias("max_date")
).collect()[0]

start_date = date_range.min_date
end_date = date_range.max_date

print(f"Date range: {start_date} to {end_date}")

# Generate calendar
date_df = spark.sql(f"""
    SELECT explode(sequence(
        to_date('{start_date}'), 
        to_date('{end_date}'), 
        interval 1 day
    )) as full_date
""")

dim_date = date_df.select(
    date_format(col("full_date"), "yyyyMMdd").cast("int").alias("sk_date"),
    col("full_date"),
    year(col("full_date")).alias("year"),
    quarter(col("full_date")).alias("quarter"),
    month(col("full_date")).alias("month"),
    date_format(col("full_date"), "MMMM").alias("month_name"),
    weekofyear(col("full_date")).alias("week_of_year"),
    dayofmonth(col("full_date")).alias("day_of_month"),
    dayofweek(col("full_date")).alias("day_of_week"),
    date_format(col("full_date"), "EEEE").alias("day_name"),
    when(dayofweek(col("full_date")).isin(1, 7), 1).otherwise(0).alias("is_weekend")
)

validate_primary_key(dim_date, "sk_date", "dim_date")

dim_date.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{gold_schema}.dim_date")
print(f"dim_date created: {dim_date.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 dim_category

# COMMAND ----------

dim_category = categories.select(
    col("category_id").alias("sk_category"),
    col("category_id"),
    col("category_name"),
    col("category_type"),
    col("is_active"),
    current_timestamp().alias("_loaded_at")
)

validate_primary_key(dim_category, "sk_category", "dim_category")

dim_category.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{gold_schema}.dim_category")
print(f"dim_category created: {dim_category.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 dim_rider

# COMMAND ----------

dim_rider = riders.select(
    col("rider_id").alias("sk_rider"),
    col("rider_id"),
    concat_ws(" ", col("first_name"), col("last_name")).alias("full_name"),
    col("city"),
    col("state"),
    col("vehicle_type"),
    col("rating"),
    col("total_deliveries"),
    col("is_active"),
    current_timestamp().alias("_loaded_at")
)

validate_primary_key(dim_rider, "sk_rider", "dim_rider")

dim_rider.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{gold_schema}.dim_rider")
print(f"dim_rider created: {dim_rider.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.4 dim_customer (SCD Type 2)

# COMMAND ----------

window_spec = Window.orderBy("customer_id", "effective_start_date")

dim_customer = customers.select(
    col("customer_id"),
    concat_ws(" ", col("first_name"), col("last_name")).alias("full_name"),
    col("email"),
    col("city"),
    col("state"),
    col("country"),
    col("loyalty_tier"),
    col("is_active"),
    col("effective_start_date"),
    col("effective_end_date"),
    col("is_current")
).withColumn("sk_customer", row_number().over(window_spec)) \
 .withColumn("_loaded_at", current_timestamp())

# Reorder columns
dim_customer = dim_customer.select(
    "sk_customer", "customer_id", "full_name", "email", "city", "state", 
    "country", "loyalty_tier", "is_active", "effective_start_date", 
    "effective_end_date", "is_current", "_loaded_at"
)

validate_primary_key(dim_customer, "sk_customer", "dim_customer")

dim_customer.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{gold_schema}.dim_customer")
print(f"dim_customer created: {dim_customer.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.5 dim_merchant (SCD Type 2)

# COMMAND ----------

window_spec = Window.orderBy("merchant_id", "effective_start_date")

dim_merchant = merchants.select(
    col("merchant_id"),
    col("merchant_name"),
    col("cuisine_type"),
    col("city"),
    col("state"),
    col("rating"),
    col("is_active"),
    col("effective_start_date"),
    col("effective_end_date"),
    col("is_current")
).withColumn("sk_merchant", row_number().over(window_spec)) \
 .withColumn("_loaded_at", current_timestamp())

# Reorder columns
dim_merchant = dim_merchant.select(
    "sk_merchant", "merchant_id", "merchant_name", "cuisine_type", "city", 
    "state", "rating", "is_active", "effective_start_date", 
    "effective_end_date", "is_current", "_loaded_at"
)

validate_primary_key(dim_merchant, "sk_merchant", "dim_merchant")

dim_merchant.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{gold_schema}.dim_merchant")
print(f"dim_merchant created: {dim_merchant.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 4: Create Fact Table

# COMMAND ----------

# Load dimension lookups
dim_date_lookup = spark.table(f"{catalog}.{gold_schema}.dim_date").select("sk_date", "full_date")
dim_customer_lookup = spark.table(f"{catalog}.{gold_schema}.dim_customer").filter("is_current = 1").select("sk_customer", "customer_id")
dim_merchant_lookup = spark.table(f"{catalog}.{gold_schema}.dim_merchant").filter("is_current = 1").select("sk_merchant", "merchant_id")
dim_rider_lookup = spark.table(f"{catalog}.{gold_schema}.dim_rider").select("sk_rider", "rider_id")

# Active orders only
orders_active = orders.filter("is_deleted = 0")

# Build fact table
fact_orders = orders_active \
    .join(dim_date_lookup, orders_active.order_date == dim_date_lookup.full_date, "left") \
    .join(dim_customer_lookup, "customer_id", "left") \
    .join(dim_merchant_lookup, "merchant_id", "left") \
    .join(dim_rider_lookup, "rider_id", "left") \
    .select(
        coalesce(dim_date_lookup.sk_date, lit(-1)).alias("sk_date"),
        coalesce(dim_customer_lookup.sk_customer, lit(-1)).alias("sk_customer"),
        coalesce(dim_merchant_lookup.sk_merchant, lit(-1)).alias("sk_merchant"),
        coalesce(dim_rider_lookup.sk_rider, lit(-1)).alias("sk_rider"),
        orders_active.order_id,
        orders_active.order_date,
        orders_active.customer_id,
        orders_active.merchant_id,
        orders_active.rider_id,
        orders_active.total_amount,
        orders_active.tax_amount,
        orders_active.delivery_fee,
        orders_active.tip_amount,
        orders_active.discount_amount,
        orders_active.rating,
        orders_active.order_status,
        orders_active.payment_status,
        orders_active.payment_method,
        orders_active.is_deleted,
        current_timestamp().alias("_loaded_at")
    )

validate_primary_key(fact_orders, "order_id", "fact_orders")

# Log missing dimension matches
print(f"Missing dimension matches:")
print(f"  Dates: {fact_orders.filter('sk_date = -1').count()}")
print(f"  Customers: {fact_orders.filter('sk_customer = -1').count()}")
print(f"  Merchants: {fact_orders.filter('sk_merchant = -1').count()}")
print(f"  Riders: {fact_orders.filter('sk_rider = -1').count()}")

fact_orders.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{gold_schema}.fact_orders")
print(f"fact_orders created: {fact_orders.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 5: Create Daily Aggregation

# COMMAND ----------

agg_daily_sales = spark.sql(f"""
    SELECT 
        d.full_date as order_date,
        d.year,
        d.month,
        d.month_name,
        d.day_of_week,
        d.day_name,
        d.is_weekend,
        COUNT(DISTINCT f.order_id) as total_orders,
        COUNT(DISTINCT f.sk_customer) as unique_customers,
        COUNT(DISTINCT f.sk_merchant) as unique_merchants,
        SUM(f.total_amount) as total_revenue,
        SUM(f.tax_amount) as total_tax,
        SUM(f.tip_amount) as total_tips,
        SUM(f.discount_amount) as total_discounts,
        ROUND(AVG(f.total_amount), 2) as avg_order_value,
        ROUND(AVG(f.rating), 2) as avg_rating,
        CURRENT_TIMESTAMP() as _loaded_at
    FROM {catalog}.{gold_schema}.fact_orders f
    JOIN {catalog}.{gold_schema}.dim_date d ON f.sk_date = d.sk_date
    WHERE f.is_deleted = 0
    GROUP BY d.full_date, d.year, d.month, d.month_name, d.day_of_week, d.day_name, d.is_weekend
    ORDER BY d.full_date
""")

agg_daily_sales.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{gold_schema}.agg_daily_sales")
print(f"agg_daily_sales created: {agg_daily_sales.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 6: Summary

# COMMAND ----------

print("")
print("=" * 60)
print("           GOLD LAYER SUMMARY")
print("=" * 60)
print("")
print("DIMENSIONS:")
print(f"  dim_date:      {spark.table(f'{catalog}.{gold_schema}.dim_date').count():>10} rows")
print(f"  dim_category:  {spark.table(f'{catalog}.{gold_schema}.dim_category').count():>10} rows")
print(f"  dim_rider:     {spark.table(f'{catalog}.{gold_schema}.dim_rider').count():>10} rows")
print(f"  dim_customer:  {spark.table(f'{catalog}.{gold_schema}.dim_customer').count():>10} rows")
print(f"  dim_merchant:  {spark.table(f'{catalog}.{gold_schema}.dim_merchant').count():>10} rows")
print("")
print("FACT:")
print(f"  fact_orders:   {spark.table(f'{catalog}.{gold_schema}.fact_orders').count():>10} rows")
print("")
print("AGGREGATION:")
print(f"  agg_daily_sales: {spark.table(f'{catalog}.{gold_schema}.agg_daily_sales').count():>8} rows")
print("")
print("=" * 60)
print("           GOLD LAYER COMPLETE")
print("=" * 60)