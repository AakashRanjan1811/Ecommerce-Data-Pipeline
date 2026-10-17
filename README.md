# E-commerce Data Pipeline

A local batch data pipeline built with **PySpark** and **Delta Lake**. It processes
real e-commerce CSV datasets through a Bronze → Silver → Gold Medallion
Architecture and produces validated Delta tables for user analysis and country
reporting.

The current implementation runs on a Windows workstation using local storage.
It preserves the original Bronze and Silver transformations, corrects the Gold
join granularity, and requires no Azure account, Databricks workspace, or cloud
credentials. The original notebooks remain unchanged as historical references.

**Verified result:** 14 Delta tables, including a user One Big Table (OBT) with
**119,656 unique user/source-version keys**, zero missing users, and unchanged
user product totals.

## Medallion Architecture

~~~mermaid
flowchart TD
    CSV["Local CSV inputs<br/>10 user chunks + 3 summary datasets"] --> Bronze["Bronze: 4 Delta tables<br/>Typed ingestion and source provenance"]
    Bronze --> Silver["Silver: 4 Delta tables<br/>Cleansing, normalization, and derived features"]
    Silver --> Users["Gold user OBT<br/>One row per source version and user"]
    Silver --> Country["Gold country aggregates<br/>Buyers, pivoted sellers, and country statistics"]
    Silver --> Detail["Gold seller detail<br/>Country and seller sex"]
    Silver --> Rollups["Gold seller rollups<br/>Global totals stored separately"]
    Country -->|"Unique country keys; LEFT JOIN historical context"| Users
~~~

| Layer | Responsibility | Output |
| --- | --- | --- |
| Bronze | Read existing CSVs with explicit types, normalize column names, combine user schemas by name, and retain source-file/source-version provenance. | Four Delta tables: users, buyers, sellers, countries. |
| Silver | Normalize countries and codes; cast numeric and boolean values; derive account, engagement, seller, and country features using the preserved transformations. | Four cleansed Delta tables with all source rows retained. |
| Gold | Enrich users without multiplying records; separate facts by their natural grain; isolate global rollups; validate population definitions and additive totals. | One user OBT and five aggregate/detail tables. |

Every table uses Delta Lake transaction logs and Parquet data files. Reruns use
Delta overwrite transactions to replace active table snapshots. A run is not a
transaction across all tables; rerun the full pipeline after a failure to refresh
all layers consistently.

## Technologies

| Component | Verified version / role |
| --- | --- |
| Python | 3.11.16; Python 3.11 recommended |
| Apache Spark / PySpark | 3.5.8, local execution, Scala 2.12 |
| Delta Lake | 3.3.2, transactional local tables |
| Java | Zulu OpenJDK 17.0.18 |
| Hadoop client | 3.3.4, bundled with PySpark |
| Windows native helpers | Community Hadoop 3.3.6 build, pinned and cached locally |
| Environment | Isolated Python virtual environment and PowerShell commands |

Exact Python dependencies are pinned in [requirements.txt](requirements.txt).
[setup_runtime.py](setup_runtime.py) downloads the required JVM libraries and
Windows helpers into `.runtime/` and checks their hashes. Runtime configuration
is applied to the process, leaving system environment settings unchanged.
See [LOCAL_SETUP.md](LOCAL_SETUP.md) for runtime provenance and Windows details.

## Features

- Automated Bronze → Silver → Gold execution through one Python command.
- Existing source data and ten user chunks; no synthetic data generation.
- Schema-aware ingestion of two user versions with different column layouts.
- Source-file lineage and explicit historical population definitions.
- One Gold row per `(source_snapshot, identifierHash)` with male/female seller
  statistics in separate columns.
- Native-grain country tables for reliable aggregation and isolated rollups.
- Automated checks for Delta validity, row counts, unique keys, missing users,
  user attributes, product totals, pivot fidelity, and historical context.
- Read-only verification of saved tables in a fresh Spark process.
- Original notebooks and source datasets preserved locally.

## Project structure

~~~text
Ecommerce-Data-Pipeline-main/
├── run_pipeline.py              # CLI: ingestion, orchestration, persistence, checks
├── gold_layer.py                # Corrected Gold model, contracts, semantic validation
├── setup_runtime.py             # Local JVM dependency and Windows helper setup
├── requirements.txt            # Pinned Python dependencies
├── README.md
├── LOCAL_SETUP.md               # Detailed setup and data contracts
├── GOLD_AUDIT.md                # Historical audit of the former Gold joins
├── .gitignore
├── Bronze_Layer.ipynb           # Local legacy reference; ignored (inline credentials)
├── Silver_Layer.ipynb           # Preserved Silver reference notebook
├── Gold_Layer.ipynb             # Local legacy reference; ignored (saved data output)
├── chunk-user-data.ipynb        # Local chunking reference; ignored (saved data output)
├── data/                       # Local source datasets; ignored except placeholders
│   └── chunk-data/             # chunk1.csv through chunk10.csv
├── .venv/                      # Generated Python environment; ignored
├── .runtime/                   # Downloaded libraries, Spark temporary state; ignored
├── backups/                    # Pre-correction implementation and Gold table; ignored
└── output/                     # Generated Delta tables and reports; ignored
    ├── bronze/{users,buyers,sellers,countries}/
    ├── silver/{users,buyers,sellers,countries}/
    ├── gold/
    │   ├── ecom_one_big_table/
    │   ├── country_buyers/
    │   ├── country_sellers/
    │   ├── country_statistics/
    │   ├── seller_country_gender/
    │   └── seller_global_rollups/
    ├── validation_report.json
    ├── validation_only_report.json
    └── gold_revision_verification.json
~~~

Generated directories and local-only references are shown for orientation; they
are not required to be committed to version control.

## Source data

The runner expects these local inputs relative to the project root:

| Input path under `data/` | Rows | Purpose |
| --- | ---: | --- |
| `chunk-data/chunk1.csv` through `chunk5.csv` | 98,913 combined | Original user source, 24 columns |
| `chunk-data/chunk6.csv` through `chunk10.csv` | 20,743 combined | Newer user source, 21 columns |
| `Buyers-repartition-by-country.csv` | 62 | Country buyer summary |
| `Comparison-of-Sellers-by-Gender-and-Country.csv` | 73 | Seller country/sex detail plus three global rollups |
| `Countries-with-Top-Sellers-(Fashion-C2C).csv` | 19 | Country statistics for the supplied top-seller countries |

The chunk groups were checked against all records in
`6M-0K-99K.users.dataset.public.csv` and `users.6M0xxK.2024.public.csv`, respectively.
The parent files are not ingested again. The additional
`users.6M0xxK.2020.public.csv` is also not added to the pipeline's input population.

The newer user schema changes column order, omits three app flags and login age
in days, and adds `websiteLongevity`. Files are parsed separately and combined
using `unionByName`, preserving unavailable fields as null in Bronze. Silver
retains the established handling of those fields. See
[LOCAL_SETUP.md](LOCAL_SETUP.md#actual-source-schemas-and-azure-input-mapping)
for exact mappings and transformation details.

**Data availability:** datasets are local prerequisites, not bundled publication
assets. A source-only checkout must obtain permitted copies of the required CSVs
and the ten existing chunks, maintaining the documented headers and layout.
Redistribution terms are not documented in this checkout, so `.gitignore`
excludes local data by default. No dataset files are deleted by this policy.

## Installation

The verified environment is Windows x64 with Python 3.11 and Java 17. The runner
accepts Python 3.10–3.11; Python 3.11 is the tested configuration. Java must be
available on `PATH` or through a valid `JAVA_HOME`.

From PowerShell, open your project directory and run:

~~~powershell
Set-Location D:\Ecommerce-Data-Pipeline-main

python --version
java -version

python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe setup_runtime.py
.\.venv\Scripts\python.exe -m pip check
~~~

Adjust the project path for your machine. Calling the virtual environment's
interpreter directly avoids activation and PowerShell execution-policy changes.
Package/runtime setup requires internet access; subsequent pipeline runs use
the cached libraries and local source files. No separate Spark installation,
Azure credentials, Databricks runtime, or Hive metastore is required.

## Execution

Run the complete pipeline after placing the required input files:

~~~powershell
.\.venv\Scripts\python.exe run_pipeline.py
~~~

Validate existing tables without rewriting them:

~~~powershell
.\.venv\Scripts\python.exe run_pipeline.py --validate-only
~~~

Choose a local thread count and a separate output directory:

~~~powershell
.\.venv\Scripts\python.exe run_pipeline.py --master "local[4]" --output-dir .\output_alt
~~~

The default is `local[2]`, a 2 GB Spark driver, and four shuffle partitions.
Output must remain inside the project and outside source, backup, and runtime
directories. Run one pipeline process at a time for a given output directory.

## Gold-layer corrections and data contracts

The previous country-only seller join produced **229,793 rows**: 110,134 user
records appeared twice because their countries had both seller-sex groups, and
three global seller rollups were mixed into the result. The audit is preserved
in [GOLD_AUDIT.md](GOLD_AUDIT.md).

The corrected model:

1. Verifies unique, non-null user keys in Silver.
2. Separates the three global seller rollups from named-country detail.
3. Pivots female and male seller metrics into one row per country.
4. Verifies unique country enrichment keys before joining.
5. Uses LEFT JOINs from Silver users to retain each source user exactly once.
6. Stores country facts and rollups at their own documented grains.

| Gold table | Grain | Verified rows |
| --- | --- | ---: |
| `ecom_one_big_table` | `(Users_source_snapshot, Users_identifierHash)` | **119,656** |
| `country_buyers` | Metric source version + country | 62 |
| `country_sellers` | Metric source version + country; separate female/male columns | 47 |
| `country_statistics` | Metric source version + country | 19 |
| `seller_country_gender` | Metric source version + country + sex | 70 |
| `seller_global_rollups` | Metric source version + rollup key | 3 |

User attributes use `Users_` prefixes, except `Country`. Historical context uses
names such as `Context_original_sellers_female_nbsellers` and
`Context_original_sellers_male_nbsellers`.

**Aggregation rules:** sum user measures within the desired source version from
the OBT. Sum country facts from their dedicated country tables. The
`Context_original_*` values repeat across users and are **not additive across
user rows**; each field carries schema metadata documenting that rule. Do not
sum global subtotals with their grand total or with country detail. Combined
seller product means are recomputed from additive totals and seller counts.

The country summaries reconcile with the **original** user source. Aggregate
tables include `metrics_source_snapshot`, `population_definition`,
`source_dataset`, and `aggregation_level`. For newer users, historical context is
explicitly marked with `context_matches_user_snapshot=false`. Dataset labels do
not establish shared person identities or an exact observation date.

Population definitions also differ: buyer summaries describe users with
purchases; seller comparisons describe users with sales **or listings**; country
statistics describe users with sales in the supplied top-seller countries.
These populations should not be treated as interchangeable.

## Verified results

The corrected pipeline and a fresh-process, read-only validation both passed on
**17 October 2026**. All **14 Delta tables** were reopened and checked.

| Dataset | Bronze rows | Silver rows |
| --- | ---: | ---: |
| Users | 119,656 | 119,656 |
| Buyers | 62 | 62 |
| Sellers | 73 | 73 |
| Countries | 19 | 19 |

Gold user keys are unique: **119,656**, with **zero missing users** and **zero
unexpected users**. Every user attribute is preserved. The following totals
match Silver exactly:

| User measure | Silver total | Gold total |
| --- | ---: | ---: |
| Products sold | 56,299 | 56,299 |
| Products bought | 73,487 | 73,487 |
| Products wished | 466,643 | 466,643 |
| Products listed | 22,148 | 22,148 |
| Social products liked | 1,513,050 | 1,513,050 |

The verified full run took **108.05 seconds**; read-only validation took **76.47
seconds** on the local machine. These are observed timings, not performance
guarantees.

Automated checks cover source row counts and nulls, Delta metadata/history,
Silver values and types, unique user/country keys, complete user preservation,
product totals by source version, seller pivot round trips, rollup reconciliation,
historical populations, and non-additive context metadata. A failed check exits
with a nonzero status and records the failure.

Local reports are generated under `output/`:

- `validation_report.json`: full-run counts, schemas, versions, contracts, and checks.
- `validation_only_report.json`: fresh-process verification without table rewrites.
- `gold_revision_verification.json`: preserved-source and backup checksum checks
  from the Gold correction; this companion record is not regenerated by every run.

Original notebooks and all source CSVs retained their checksums. The Bronze and
Silver execution block is unchanged. The earlier runner and 229,793-row Gold
Delta snapshot remain in `backups/gold_before_user_grain_20261017/`.

## Local data and credentials

`.gitignore` retains the existing exclusions and also covers environments,
runtime caches, generated Delta/Parquet files, checkpoints, logs, temporary files,
credentials, backups, and local datasets with unverified redistribution terms.

The legacy Bronze notebook contains inline Azure credential configuration;
the Gold and chunking notebooks contain saved source-data outputs. These three
root notebooks are ignored and preserved locally. The Silver reference notebook
remains eligible for version control. The Python runner does not load notebook
cells or use their credentials. Review sanitized copies and data permissions
separately before including local-only artifacts in a future publication.

Ignore rules do not remove files that are already tracked by Git. No files were
untracked, committed, or pushed as part of this documentation update.

## Further documentation

- [LOCAL_SETUP.md](LOCAL_SETUP.md): detailed setup, schemas, Windows runtime,
  Gold contracts, backup information, and examples for reading Delta tables.
- [GOLD_AUDIT.md](GOLD_AUDIT.md): the historical join-cardinality and aggregation
  audit that motivated the Gold corrections.
- [gold_layer.py](gold_layer.py): implemented Gold grains, population definitions,
  and automated semantic validation.



