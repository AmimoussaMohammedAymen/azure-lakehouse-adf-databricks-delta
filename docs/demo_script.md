# Demo Script (2 minutes)

Hi, I’m Aymen. This project demonstrates an Azure Lakehouse pipeline using ADF + Databricks + Delta.

1) Data lands in ADLS Gen2 under raw.  
2) ADF ingests it into Bronze (immutable storage) and triggers Databricks notebooks.  
3) Databricks cleans and standardizes the data into Silver, deduplicates, and applies schema rules.  
4) Incremental loads use a watermark strategy and Delta MERGE upserts into Silver.  
5) Gold tables provide business aggregates like daily revenue and top products.  
6) Finally, data quality checks validate nulls, uniqueness, ranges, and referential integrity.

This is production-style: parameterized ADF, incremental strategy, Delta MERGE, and quality gates.
