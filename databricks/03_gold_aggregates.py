# 03_gold_aggregates.py
from pyspark.sql import functions as F

SILVER = "abfss://silver@<storage-account>.dfs.core.windows.net"
GOLD = "abfss://gold@<storage-account>.dfs.core.windows.net"

tx = spark.read.format("delta").load(f"{SILVER}/fact_transactions")
products = spark.read.format("delta").load(f"{SILVER}/dim_products")

daily_revenue = (tx
  .withColumn("date", F.to_date("transaction_ts"))
  .groupBy("date")
  .agg(
    F.count("*").alias("transactions"),
    F.sum("amount").alias("revenue")
  )
  .orderBy("date")
)

top_products = (tx.join(products, "product_id", "left")
  .groupBy("product_id", "product_name")
  .agg(F.sum("amount").alias("revenue"))
  .orderBy(F.desc("revenue"))
  .limit(20)
)

daily_revenue.write.format("delta").mode("overwrite").save(f"{GOLD}/daily_revenue")
top_products.write.format("delta").mode("overwrite").save(f"{GOLD}/top_products")
