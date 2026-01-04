# Databricks Notebook (Python)
# 01_bronze_ingest.py
from pyspark.sql import functions as F

RAW = "abfss://raw@<storage-account>.dfs.core.windows.net"
BRONZE = "abfss://bronze@<storage-account>.dfs.core.windows.net"

customers = (spark.read.option("header", True).csv(f"{RAW}/customers/")
             .withColumn("_ingest_ts", F.current_timestamp())
             .withColumn("_source", F.lit("raw/customers")))

products = (spark.read.option("header", True).csv(f"{RAW}/products/")
            .withColumn("_ingest_ts", F.current_timestamp())
            .withColumn("_source", F.lit("raw/products")))

transactions = (spark.read.option("header", True).csv(f"{RAW}/transactions/")
                .withColumn("_ingest_ts", F.current_timestamp())
                .withColumn("_source", F.lit("raw/transactions")))

# Write Bronze as Delta (append-only)
(customers.write.format("delta").mode("append").save(f"{BRONZE}/customers_delta"))
(products.write.format("delta").mode("append").save(f"{BRONZE}/products_delta"))
(transactions.write.format("delta").mode("append").save(f"{BRONZE}/transactions_delta"))
