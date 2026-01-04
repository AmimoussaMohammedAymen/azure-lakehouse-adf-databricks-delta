# 02_silver_clean.py
from pyspark.sql import functions as F
from pyspark.sql.window import Window

BRONZE = "abfss://bronze@<storage-account>.dfs.core.windows.net"
SILVER = "abfss://silver@<storage-account>.dfs.core.windows.net"

customers_b = spark.read.format("delta").load(f"{BRONZE}/customers_delta")
products_b = spark.read.format("delta").load(f"{BRONZE}/products_delta")
transactions_b = spark.read.format("delta").load(f"{BRONZE}/transactions_delta")

# Cast types (basic)
customers = (customers_b
  .withColumn("customer_id", F.col("customer_id").cast("long"))
  .withColumn("updated_ts", F.to_timestamp("updated_ts"))
  .withColumn("email", F.lower(F.col("email")))
  .dropDuplicates(["customer_id", "updated_ts"])
)

# Deduplicate customers: keep latest per customer_id
w = Window.partitionBy("customer_id").orderBy(F.col("updated_ts").desc_nulls_last())
customers_latest = (customers
  .withColumn("rn", F.row_number().over(w))
  .filter("rn=1")
  .drop("rn")
)

products = (products_b
  .withColumn("product_id", F.col("product_id").cast("long"))
  .withColumn("price", F.col("price").cast("decimal(18,2)"))
  .withColumn("updated_ts", F.to_timestamp("updated_ts"))
)

transactions = (transactions_b
  .withColumn("transaction_id", F.col("transaction_id").cast("long"))
  .withColumn("customer_id", F.col("customer_id").cast("long"))
  .withColumn("product_id", F.col("product_id").cast("long"))
  .withColumn("quantity", F.col("quantity").cast("int"))
  .withColumn("amount", F.col("amount").cast("decimal(18,2)"))
  .withColumn("transaction_ts", F.to_timestamp("transaction_ts"))
)

(customers_latest.write.format("delta").mode("overwrite").save(f"{SILVER}/dim_customers"))
(products.write.format("delta").mode("overwrite").save(f"{SILVER}/dim_products"))
(transactions.write.format("delta").mode("overwrite").save(f"{SILVER}/fact_transactions"))
