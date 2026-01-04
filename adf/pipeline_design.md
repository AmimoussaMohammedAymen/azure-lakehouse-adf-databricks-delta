# ADF Pipeline Design (Blueprint)

## Pipeline: pl_ingest_and_process
Parameters:
- p_dataset (string) -> "customers" | "products" | "transactions"
- p_raw_path (string) -> e.g. raw/customers/
- p_bronze_path (string) -> e.g. bronze/customers/
- p_trigger_databricks (bool)

Activities:
1) GetMetadata (folder exists?) on ADLS raw path
2) If Condition: if exists
   2.1) Copy Activity: ADLS raw → ADLS bronze (or → Azure SQL staging)
   2.2) Databricks Notebook activity (optional) to run bronze ingestion
3) If not exists → Fail with message

## Pipeline: pl_orchestrate_all
Activities in order:
1) Execute Pipeline: pl_ingest_and_process customers
2) Execute Pipeline: pl_ingest_and_process products
3) Execute Pipeline: pl_ingest_and_process transactions
4) Databricks Notebook: 02_silver_clean
5) Databricks Notebook: 04_incremental_merge
6) Databricks Notebook: 03_gold_aggregates
7) Databricks Notebook: 05_quality_checks

Triggers:
- Schedule trigger (daily) or Storage event trigger (file arrival)
