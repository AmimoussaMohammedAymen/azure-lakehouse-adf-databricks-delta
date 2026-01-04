# 04_incremental_merge.py
# MERGE incremental transactions into Silver fact table
from delta.tables import DeltaTable
from pyspark.sql import functions as F

BRONZE = "abfss://bronze@<storage-account>.dfs.core.windows.net"
SILVER = "abfss://silver@<storage-account>.dfs.core.windows.net"

src = spark.read.format("delta").load(f"{BRONZE}/transactions_delta") \
  .withColumn("transaction_id", F.col("transaction_id").cast("long")) \
  .withColumn("transaction_ts", F.to_timestamp("transaction_ts")) \
  .withColumn("amount", F.col("amount").cast("decimal(18,2)")) \
  .withColumn("quantity", F.col("quantity").cast("int")) \
  .withColumn("customer_id", F.col("customer_id").cast("long")) \
  .withColumn("product_id", F.col("product_id").cast("long"))

target_path = f"{SILVER}/fact_transactions"
if not DeltaTable.isDeltaTable(spark, target_path):
  src.write.format("delta").mode("overwrite").save(target_path)

tgt = DeltaTable.forPath(spark, target_path)

(tgt.alias("t")
 .merge(
    src.alias("s"),
    "t.transaction_id = s.transaction_id"
  )
 .whenMatchedUpdate(set={
    "customer_id": "s.customer_id",
    "product_id": "s.product_id",
    "quantity": "s.quantity",
    "amount": "s.amount",
    "transaction_ts": "s.transaction_ts"
  })
 .whenNotMatchedInsert(values={
    "transaction_id": "s.transaction_id",
    "customer_id": "s.customer_id",
    "product_id": "s.product_id",
    "quantity": "s.quantity",
    "amount": "s.amount",
    "transaction_ts": "s.transaction_ts"
  })
 .execute()
)
