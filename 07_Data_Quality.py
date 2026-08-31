# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Data Quality: Final Verification
# MAGIC
# MAGIC Cross-table checks and summary report after all tables are loaded.

# COMMAND ----------

# MAGIC %run ./00_Config_and_Utilities

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load All Tables

# COMMAND ----------

catalog = PipelineConfig.CATALOG
silver_schema = PipelineConfig.SILVER_SCHEMA
gold_schema = PipelineConfig.GOLD_SCHEMA

# Silver tables
categories = spark.table(f"{catalog}.{silver_schema}.categories")
riders = spark.table(f"{catalog}.{silver_schema}.riders")
customers = spark.table(f"{catalog}.{silver_schema}.customers").filter("is_current = 1")
merchants = spark.table(f"{catalog}.{silver_schema}.merchants").filter("is_current = 1")
orders = spark.table(f"{catalog}.{silver_schema}.orders").filter("is_deleted = 0")

# Gold tables
fact_orders = spark.table(f"{catalog}.{gold_schema}.fact_orders").filter("is_deleted = 0")

print("All tables loaded")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Row Count Summary

# COMMAND ----------

print("")
print("=" * 50)
print("         ROW COUNT SUMMARY")
print("=" * 50)
print("")
print("SILVER LAYER:")
print(f"  categories:   {categories.count():>10}")
print(f"  riders:       {riders.count():>10}")
print(f"  customers:    {customers.count():>10} (current)")
print(f"  merchants:    {merchants.count():>10} (current)")
print(f"  orders:       {orders.count():>10} (active)")
print("")
print("GOLD LAYER:")
print(f"  fact_orders:  {fact_orders.count():>10}")
print("=" * 50)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Primary Key Checks

# COMMAND ----------

print("PRIMARY KEY CHECKS (No Nulls)")
print("=" * 50)

checks_passed = 0
checks_failed = 0

pk_checks = [
    ("categories", categories, "category_id"),
    ("riders", riders, "rider_id"),
    ("customers", customers, "customer_id"),
    ("merchants", merchants, "merchant_id"),
    ("orders", orders, "order_id"),
    ("fact_orders", fact_orders, "order_id")
]

for table_name, df, pk in pk_checks:
    null_count = df.filter(col(pk).isNull()).count()
    if null_count == 0:
        print(f"  PASS: {table_name}.{pk}")
        checks_passed += 1
    else:
        print(f"  FAIL: {table_name}.{pk} has {null_count} nulls")
        checks_failed += 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Referential Integrity Checks

# COMMAND ----------

print("")
print("REFERENTIAL INTEGRITY CHECKS")
print("=" * 50)

# Orders -> Customers
orphan_customers = orders.join(
    customers,
    orders.customer_id == customers.customer_id,
    "left_anti"
).count()

if orphan_customers == 0:
    print("  PASS: All orders have valid customer_id")
    checks_passed += 1
else:
    print(f"  FAIL: {orphan_customers} orders have invalid customer_id")
    checks_failed += 1

# Orders -> Merchants
orphan_merchants = orders.join(
    merchants,
    orders.merchant_id == merchants.merchant_id,
    "left_anti"
).count()

if orphan_merchants == 0:
    print("  PASS: All orders have valid merchant_id")
    checks_passed += 1
else:
    print(f"  FAIL: {orphan_merchants} orders have invalid merchant_id")
    checks_failed += 1

# Orders -> Riders (rider can be null for unassigned orders)
if "rider_id" in orders.columns:
    orphan_riders = orders.filter(col("rider_id").isNotNull()).join(
        riders,
        orders.rider_id == riders.rider_id,
        "left_anti"
    ).count()
    
    if orphan_riders == 0:
        print("  PASS: All assigned orders have valid rider_id")
        checks_passed += 1
    else:
        print(f"  FAIL: {orphan_riders} orders have invalid rider_id")
        checks_failed += 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Data Range Checks

# COMMAND ----------

print("")
print("DATA RANGE CHECKS")
print("=" * 50)

# Rating range (0-5)
for table_name, df in [("riders", riders), ("merchants", merchants)]:
    if "rating" in df.columns:
        invalid_ratings = df.filter((col("rating") < 0) | (col("rating") > 5)).count()
        if invalid_ratings == 0:
            print(f"  PASS: {table_name}.rating in range [0-5]")
            checks_passed += 1
        else:
            print(f"  FAIL: {table_name} has {invalid_ratings} ratings outside [0-5]")
            checks_failed += 1

# Order amounts (non-negative)
negative_amounts = orders.filter(col("total_amount") < 0).count()
if negative_amounts == 0:
    print("  PASS: orders.total_amount >= 0")
    checks_passed += 1
else:
    print(f"  FAIL: {negative_amounts} orders have negative total_amount")
    checks_failed += 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Gold Layer Consistency

# COMMAND ----------

print("")
print("GOLD LAYER CONSISTENCY")
print("=" * 50)

# Fact orders count matches silver orders
silver_order_count = orders.count()
gold_order_count = fact_orders.count()

if silver_order_count == gold_order_count:
    print(f"  PASS: fact_orders count ({gold_order_count}) matches silver.orders")
    checks_passed += 1
else:
    print(f"  WARN: fact_orders ({gold_order_count}) != silver.orders ({silver_order_count})")
    checks_failed += 1

# Missing dimension keys in fact
missing_dims = fact_orders.filter(
    (col("sk_customer") == -1) | 
    (col("sk_merchant") == -1) | 
    (col("sk_date") == -1)
).count()

if missing_dims == 0:
    print("  PASS: All fact_orders have valid dimension keys")
    checks_passed += 1
else:
    print(f"  WARN: {missing_dims} fact_orders have missing dimension keys (sk=-1)")
    # Not counting as failure, just warning

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Final Report

# COMMAND ----------

print("")
print("=" * 60)
print("              DATA QUALITY REPORT")
print("=" * 60)
print("")
print(f"  Checks Passed:  {checks_passed}")
print(f"  Checks Failed:  {checks_failed}")
print("")

if checks_failed == 0:
    status = "SUCCESS"
    print("  STATUS: ALL CHECKS PASSED")
else:
    status = "WARNING"
    print(f"  STATUS: {checks_failed} CHECKS FAILED")

print("")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Exit with Status

# COMMAND ----------

if checks_failed > 0:
    dbutils.notebook.exit(f"WARNING: {checks_failed} quality checks failed")
else:
    dbutils.notebook.exit("SUCCESS: All quality checks passed")