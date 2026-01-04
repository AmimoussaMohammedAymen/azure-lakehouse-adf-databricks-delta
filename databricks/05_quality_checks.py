# 05_quality_checks.py
from pyspark.sql import functions as F

SILVER = "abfss://silver@<storage-account>.dfs.core.windows.net"

customers = spark.read.format("delta").load(f"{SILVER}/dim_customers")
products = spark.read.format("delta").load(f"{SILVER}/dim_products")
tx = spark.read.format("delta").load(f"{SILVER}/fact_transactions")

def assert_zero(df, condition, msg):
  c = df.filter(condition).count()
  if c != 0:
    raise Exception(f"[QUALITY FAIL] {msg}. Count={c}")

# Null checks
assert_zero(customers, "customer_id IS NULL", "customer_id null")
assert_zero(products, "product_id IS NULL", "product_id null")
assert_zero(tx, "transaction_id IS NULL", "transaction_id null")

# Range check
assert_zero(tx, "amount <= 0", "amount <= 0")
assert_zero(tx, "quantity <= 0", "quantity <= 0")

# Uniqueness for dimension IDs (basic)
dup_customers = customers.groupBy("customer_id").count().filter("count > 1")
if dup_customers.count() > 0:
  raise Exception("[QUALITY FAIL] duplicate customer_id in dim_customers")

# Referential integrity: tx.customer_id must exist
missing_customers = tx.join(customers, "customer_id", "left_anti").count()
if missing_customers > 0:
  raise Exception(f"[QUALITY FAIL] transactions reference missing customers: {missing_customers}")

print("✅ All quality checks passed")
