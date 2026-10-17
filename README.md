# E-commerce Data Pipeline

An end-to-end **ETL pipeline** built using **Python, PySpark, and Delta Lake**, implementing the **Bronze → Silver → Gold Medallion Architecture** to process e-commerce datasets and generate analytics-ready tables.

## Tech Stack

Python | PySpark | Apache Spark | Delta Lake | Git

## Architecture

**Bronze Layer:** Ingests raw CSV datasets into Delta tables.

**Silver Layer:** Performs data cleaning, schema normalization, and transformations.

**Gold Layer:** Produces analytics-ready user, buyer, seller, and country-level tables.

## Key Features

- Automated end-to-end ETL pipeline using Python and PySpark.
- Processed **119,656 user records** across multiple source datasets.
- Implemented data cleaning, schema normalization, and Delta Lake storage.
- Corrected join cardinality to eliminate unintended user duplication.
- Generated user-level and country-level analytical tables.
- Validated **14 Delta tables** for data integrity and consistency.

## Verified Results

| Metric | Result |
|---|---:|
| User records processed | 119,656 |
| Unique Gold user records | 119,656 |
| Delta tables validated | 14 |
| Missing or duplicated user keys | 0 |

## Run Locally

**Requirements:** Python 3.11, Java 17, and the required CSV datasets.

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe setup_runtime.py
.\.venv\Scripts\python.exe run_pipeline.py
```

For detailed installation and dataset requirements, see [LOCAL_SETUP.md](LOCAL_SETUP.md
