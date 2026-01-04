# Azure Lakehouse Pipeline (ADF → Databricks → Delta Lake)

Production-style **Azure Data Engineering** project implementing a **Medallion Lakehouse**:
**ADF** ingests data → **Databricks** transforms → **Delta Lake** stores Bronze/Silver/Gold → Gold tables ready for BI/analytics.

## Stack
- Azure Data Factory (ADF)
- Azure Data Lake Storage Gen2 (ADLS)
- Azure Databricks
- Delta Lake
- Azure SQL (optional: staging + watermark)
- SQL + PySpark

## Architecture (Medallion)
- **Bronze**: raw ingestion (immutable)
- **Silver**: cleaned + standardized + deduplicated
- **Gold**: business-ready aggregates + dimensions/facts

> Add screenshot: `docs/architecture.png`

---

## Data
This repo includes **sample CSVs** for a realistic pipeline:
- `customers.csv`
- `products.csv`
- `transactions.csv`

Tables:
- Customers (dimension)
- Products (dimension)
- Transactions (fact)

---

## What’s implemented
✅ ADF ingestion pipelines (parameterized)  
✅ Incremental loads using watermark table  
✅ Delta MERGE upsert into Silver  
✅ Gold aggregates (daily revenue, top products)  
✅ Data quality checks (nulls, uniqueness, ranges, referential integrity)

---

## How to run (high-level)
### 1) Azure resources
Create:
- ADLS Gen2 containers: `raw/bronze/silver/gold`
- Azure Databricks workspace
- Azure Data Factory
- (Optional) Azure SQL database for watermark + staging

### 2) Upload sample data to ADLS
Upload CSVs to:
- `raw/customers/`
- `raw/products/`
- `raw/transactions/`

### 3) Create ADF pipeline (design in `/adf/pipeline_design.md`)
Pipeline steps:
- GetMetadata → IfExists → Copy to Bronze
- Lookup watermark → Copy incremental (if needed)
- Execute Databricks notebooks in order

### 4) Run Databricks notebooks
Run in this order:
1. `01_bronze_ingest.py`
2. `02_silver_clean.py`
3. `04_incremental_merge.py`
4. `03_gold_aggregates.py`
5. `05_quality_checks.py`

---

## Incremental load strategy
A watermark table stores last processed timestamp for each dataset.
- If new data exists → ingest only new files/rows
- MERGE into Silver Delta (upsert)

See:
- `sql/01_watermark.sql`
- `databricks/04_incremental_merge.py`

---

## Data Quality
Implemented checks:
- Null checks on required fields
- Uniqueness checks for dimension keys
- Range checks (amount > 0)
- Referential integrity (transactions.customer_id exists)
- Freshness check (new data within expected window)

See: `databricks/05_quality_checks.py`

---

## Demo script (2 minutes)
See `docs/demo_script.md`

---

## Author
Aymen Amimoussa  
- LinkedIn: https://www.linkedin.com/in/amimoussaaymen  
- GitHub: https://github.com/AmimoussamohammedAymen
