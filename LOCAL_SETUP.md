# Local Spark / Delta Lake setup

This is a local adaptation of `Bronze_Layer.ipynb`, `Silver_Layer.ipynb`, and
`Gold_Layer.ipynb`. The original notebooks and CSV files remain unchanged.
`run_pipeline.py` runs their transformations as ordinary Python functions.
Bronze and Silver are preserved; `gold_layer.py` implements the corrected user
grain and separate Gold aggregate tables following `GOLD_AUDIT.md`.
The runner never executes notebook cells, mounts Azure storage, or reads credentials.

## Working environment

Verified on this Windows x64 machine on 17 October 2026:

| Component | Version / location |
| --- | --- |
| Python | 3.11.16, 64-bit CPython, provided by the existing Anaconda environment |
| Project virtual environment | `.venv` (isolated, without system site packages) |
| Java | Zulu OpenJDK 17.0.18, existing installation |
| PySpark / Apache Spark | 3.5.8, Scala 2.12 |
| Delta Lake | `delta-spark` 3.3.2, with matching JVM libraries |
| Hadoop client | 3.3.4, included in PySpark |
| Windows Hadoop helpers | Community Windows native build 3.3.6, project-local |
| Other Python dependencies | Exact versions in `requirements.txt` |

The existing Python executable is
`C:\Users\Aakash\anaconda3\envs\ecommerce-pipeline\python.exe`.
Java is available through that environment's `Library\bin\java.exe`.
The runner discovers Java from `JAVA_HOME` or `PATH`; those machine-specific
paths are not hardcoded into the pipeline.

Spark 3.5 supports Python 3.8+ and Java 17; the pinned metadata packages require
Python 3.10+. This setup checks for Python 3.10-3.11 and Java 17, with Python 3.11
verified here. Delta 3.3.x supports
Spark 3.5.x. References: [Apache Spark 3.5.8](https://spark.apache.org/docs/3.5.8/),
[Delta compatibility](https://docs.delta.io/releases/),
[Delta project package requirements](https://pypi.org/project/delta-spark/3.3.2/).

## Run in PowerShell

From the project directory, the installed environment is ready:

```powershell
Set-Location D:\Ecommerce-Data-Pipeline-main
.\.venv\Scripts\python.exe run_pipeline.py
```

For a fresh setup using an available Python 3.11 and Java 17:

```powershell
python --version
java -version
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe setup_runtime.py
.\.venv\Scripts\python.exe -m pip check
.\.venv\Scripts\python.exe run_pipeline.py
```

Calling the virtual environment's interpreter directly avoids PowerShell script
execution-policy changes and does not require environment activation. Python's
`py` launcher is absent on this machine and is not required.

Only package/runtime setup needs internet access. All subsequent pipeline runs
use local CSVs and cached libraries. If a terminal sandbox blocks network access
or Java, permit the setup/run command in that sandbox. Administrator installation,
Databricks, Azure, a separate Spark installation, and a Hive metastore are not
required.

Validate the existing tables in a fresh Spark process without rewriting them:

```powershell
.\.venv\Scripts\python.exe run_pipeline.py --validate-only
```

Optional settings:

```powershell
.\.venv\Scripts\python.exe run_pipeline.py --master "local[4]" --output-dir .\output_alt
```

The default is `local[2]`, a 2 GB driver, and four shuffle partitions. Output must
stay inside the project, outside the source data, virtual environment, and runtime
directories. Run one pipeline process at a time for a given output directory.

## Actual source schemas and Azure input mapping

The Azure Parquet files themselves are not in this folder, so their exact
serialized types and contents cannot be compared. The mapping below is established
from the original Bronze paths, Silver column references, and the actual CSV
headers. Header names match the columns required by the notebook transformations.
CSV types are made explicit rather than depending on unavailable Parquet metadata.

| Notebook Azure input | Local source | Rows | CSV columns |
| --- | --- | ---: | ---: |
| `users-raw-2` | `data/chunk-data/chunk1.csv` through `chunk5.csv` | 98,913 | 24 |
| `users-raw-2` | `data/chunk-data/chunk6.csv` through `chunk10.csv` | 20,743 | 21 |
| `buyers-raw-2` | `data/Buyers-repartition-by-country.csv` | 62 | 32 |
| `sellers-raw-2` | `data/Comparison-of-Sellers-by-Gender-and-Country.csv` | 73 | 19 |
| `countries-raw-2` | `data/Countries-with-Top-Sellers-(Fashion-C2C).csv` | 19 | 26 |

Chunks 1-5 were independently compared as a multiset of complete CSV records with
`6M-0K-99K.users.dataset.public.csv`: all 98,913 records match. Chunks 6-10 likewise
match all 20,743 records in `users.6M0xxK.2024.public.csv`. The existing chunks are
used directly. Their parent files are not ingested again. The additional
`users.6M0xxK.2020.public.csv` is not a source used by the chunking notebook and is
not added as another overlapping snapshot. No synthetic records are created.

The older user header is:

```text
identifierHash, type, country, language, socialNbFollowers, socialNbFollows,
socialProductsLiked, productsListed, productsSold, productsPassRate,
productsWished, productsBought, gender, civilityGenderId, civilityTitle,
hasAnyApp, hasAndroidApp, hasIosApp, hasProfilePicture, daysSinceLastLogin,
seniority, seniorityAsMonths, seniorityAsYears, countryCode
```

The newer header moves `countryCode`, omits `hasAnyApp`, `hasAndroidApp`,
`hasIosApp`, and `daysSinceLastLogin`, and adds `websiteLongevity`.
Each file is parsed against its own header and combined with `unionByName`,
allowing missing columns. This preserves the extra field and avoids positional
column corruption. Missing fields remain null in Bronze. Bronze users therefore
have 25 source columns plus `source_file` and `source_snapshot`.

Column names become lowercase consistently. Identifiers and `websiteLongevity`
are 64-bit integers, count fields are integers, app/profile flags are booleans,
and source decimal metrics are doubles in Bronze. Text remains UTF-8. Silver
casts the notebook's financial/ratio/mean columns to `decimal(10,2)` and retains
the notebook's integer/boolean casts. The exact schema and null counts for every
persisted table, along with source headers and SHA-256 hashes, are saved in the
validation report.

## Preserved transformations and necessary fixes

All layers use actual Delta Lake tables: Parquet data files with transaction logs
under `_delta_log`. Bronze retains all source rows; Silver keeps all rows and
applies the original cleansing and derived metrics. Gold keeps one row for each
`(source_snapshot, identifierHash)` and uses separate tables for country facts and
seller rollups. All Silver user attributes, including provenance, reach the user
Gold table; its user attributes have `Users_` prefixes, except `Country`.

The local adaptation includes these fixes:

- Replace Azure mounts and `/mnt/...` paths with local paths. The countries
  filename contains parentheses; pass an unescaped Hadoop file URI so `%28` and
  `%29` are not interpreted as literal filename characters.
- Parse differently ordered user schemas separately, preserve missing fields,
  and retain source filename/snapshot for each user record.
- Normalize country names with `initcap(trim(...))` in **all four** Silver
  tables, including users. Country codes become uppercase as in the notebook.
- Make language comparisons case-insensitive so source `en`/`fr` map to the
  intended English/French labels. Make title cleaning case-insensitive so source
  `mrs` is cleaned by the original title rule.
- Keep account age, age categories, login age, descriptor, title-length flag,
  buyer engagement/ratios, seller categories/means, pass-rate imputation,
  country performance/activity, and original rounding rules.
- Retain the notebook's zero default for missing `daysSinceLastLogin` in Silver.
  Its derived login age is calculated beforehand, as in the notebook, and stays
  null for the newer snapshot. Missing app flags stay null rather than becoming
  fabricated false values.
- Prefix historical context fields before joining to eliminate ambiguous
  references and make their source version visible. Pivot male/female seller
  groups to a unique country row, then LEFT JOIN from Silver users.
- Configure Java, the virtual environment's Python/Spark, and Windows helpers
  only for the running process. Use forward slashes in Java options: Spark's
  option parser otherwise removes Windows path backslashes.

The original thresholds are preserved literally. Some source "ratio" fields
are percentages on a 0-100 scale while the notebooks compare them with values
such as `0.5` or `0.75`. This setup does not reinterpret those business rules.

## Corrected Gold architecture

The user table is `output/gold/ecom_one_big_table`, with exactly **119,656 rows**.
Its key is `(Users_source_snapshot, Users_identifierHash)`. Source filenames are
lineage, not part of the business key. The pipeline fails on duplicate or null
keys rather than arbitrarily deduplicating records. Different source versions
are not assumed to share an identity system or describe distinct real people.

The former seller country join made 110,134 user records appear twice. The new
runner separates the three seller rollups, pivots each named country's FEMALE
and MALE statistics into separate columns, verifies a unique country key, and
uses LEFT JOINs from Silver users. Missing country/sex context stays null; it is
not fabricated as zero. Every user survives once, regardless of available context.

| Gold table | Grain | Rows | Population / use |
| --- | --- | ---: | --- |
| `ecom_one_big_table` | Source version + user identifier | 119,656 | All 98,913 original and 20,743 newer user records |
| `country_buyers` | Metric source version + country | 62 | Original-source users with `productsBought > 0` |
| `seller_country_gender` | Metric source version + country + sex | 70 | Original-source users with sales OR listings; no global rollups |
| `country_sellers` | Metric source version + country | 47 | Pivoted female/male seller metrics, combined additive totals, and recomputed means |
| `country_statistics` | Metric source version + country | 19 | Original-source users with `productsSold > 0`, for supplied top-seller countries |
| `seller_global_rollups` | Metric source version + rollup key | 3 | Female subtotal, male subtotal, and all-sexes total; separate aggregation levels |

Country aggregate tables carry `metrics_source_snapshot='original'`, an explicit
`population_definition`, `source_dataset`, and `aggregation_level`. The summary
CSVs numerically reconcile with the original user source, not the combined
original/newer population. Their dates are not invented from filenames. The
original and `2024` labels identify the source datasets used by ingestion.

For example, France's 713 sellers in `country_statistics` have actual sales.
Its 966 sellers in `country_sellers` have sales OR listings. These are different
populations and have deliberately different definitions and field names.

`country_sellers` has fields such as `female_nbsellers`, `male_nbsellers`,
`female_meanproductssold`, and `male_meanproductssold`. All original sex-specific
metrics and Silver seller classifications are preserved in that pivot. Its
combined `seller_count_sales_or_listings` and `total_products_*` fields sum the
70 detail groups only. Combined product means are calculated as total / seller
count to four decimal places, avoiding an unweighted average of rounded means.
Do not add combined totals and their male/female components together.

In the user OBT, contextual fields have explicit names such as:

```text
Context_original_buyers_buyers
Context_original_countries_sellers
Context_original_sellers_female_nbsellers
Context_original_sellers_male_nbsellers
```

Every contextual field also carries Delta schema metadata including
`aggregation_rule='do_not_sum_across_users'`, source version, native grain, and
population. Row fields `context_source_snapshot`, `context_matches_user_snapshot`,
and `context_measure_usage` describe how to interpret that context. The 98,913
original users have matching source versions; the 20,743 newer users receive
explicitly labeled historical context with `context_matches_user_snapshot=false`.

**How to aggregate:** count user keys and sum `Users_productsSold`,
`Users_productsBought`, and other user measures within the desired source version
from the OBT. Sum country buyer counts from `country_buyers`, and seller counts
from either `country_sellers` or `seller_country_gender` at their stated grain.
Copied `Context_original_*` measures repeat across users and must never be summed
over user rows. Metadata documents this rule; Spark does not prohibit arbitrary
SQL expressions. Ratios/means are not additive, even in the aggregate tables.

The global seller rows reconcile to 2,127 female sellers, 592 male sellers, and
2,719 total sellers. They remain in `seller_global_rollups`, with `rollup_key`
FEMALE, MALE, or ALL and explicit aggregation levels. Select the desired level;
do not sum these subtotals with their grand total or with country detail.

## Backup before the Gold correction

`backups/gold_before_user_grain_20261017/` preserves the previous runner, setup
documentation, audit, successful validation reports, and a physical copy of the
old 229,793-row Gold Delta table under `gold/ecom_one_big_table/`. `manifest.json`
contains verified SHA-256 hashes and the original CSV/Silver transformation
fingerprints. No backup files or old Delta data versions are vacuumed/deleted.
`GOLD_AUDIT.md` describes the pre-correction implementation.

To inspect the previous table with the configured session:

```python
old = spark.read.format('delta').load(
    (ROOT / 'backups/gold_before_user_grain_20261017/gold/ecom_one_big_table').as_uri()
)
```

Restoring only that table or the old script would restore the historical mixed
grain. Treat the backed-up runner and its table/schema as a matching version.

## Windows JVM runtime

`setup_runtime.py` downloads the Delta Spark 2.12, Delta storage, and ANTLR JARs
from Maven Central into `.runtime/jars`. It checks published Maven checksums and
pinned SHA-256 hashes. They are loaded directly on the local driver's classpath,
so Spark does not lock temporary copies during Windows shutdown.

Native Windows filesystem operations need `winutils.exe` and `hadoop.dll`.
The inherited `HADOOP_HOME=C:\hadoop` contains no working helpers. This setup uses
the [community Windows Hadoop build](https://github.com/cdarlint/winutils/tree/7386986d5d8a079b5cd4464f4599766dd27e7d13/hadoop-3.3.6/bin)
at immutable commit `7386986d5d8a079b5cd4464f4599766dd27e7d13`, stored under
`.runtime/hadoop/bin`. These are community binaries, not Apache-published Windows
releases. The exact downloaded binaries are SHA-256 pinned in `setup_runtime.py`;
URLs and checksums are also recorded in `.runtime/downloads.json`.

Hadoop's native loader is checked before any table processing. This addresses
`NativeIO$Windows.access0` errors when Delta lists its transaction logs. No global
`PATH`, `JAVA_HOME`, `HADOOP_HOME`, or conda installation is changed.

During the Gold update, inherited permissions were restored on generated
`output/` folders after a mismatch between the sandbox account that created them
and the normal Windows account. This permits both accounts to rerun the pipeline
using the existing project permissions. Source data and transformation logic
were not changed to address that Windows filesystem issue.

## Outputs and validation

```text
output/
  bronze/{users,buyers,sellers,countries}/
  silver/{users,buyers,sellers,countries}/
  gold/ecom_one_big_table/
  gold/country_buyers/
  gold/country_sellers/
  gold/country_statistics/
  gold/seller_country_gender/
  gold/seller_global_rollups/
  validation_report.json
  validation_only_report.json   # after --validate-only
```

Successful execution prints these independently checked counts:

| Table | Bronze | Silver |
| --- | ---: | ---: |
| users | 119,656 | 119,656 |
| buyers | 62 | 62 |
| sellers | 73 | 73 |
| countries | 19 | 19 |

Gold table counts are listed in the architecture table above. There are now
**14 Delta tables** in total: four Bronze, four Silver, and six Gold.

Every execution reopens each output using the Delta reader, validates Delta
metadata/history, compares row counts against Python's independent CSV scan,
checks Bronze null counts against missing source fields, and checks Silver types
and normalization. It compares persisted Silver values with the full
transformations in both directions using `exceptAll`, preserving duplicate
semantics. The new Gold validations check:

- Exactly 119,656 rows and non-null, unique `(snapshot, identifier)` user keys.
- Zero missing source users and zero unexpected output users.
- Every user attribute preserved, including source provenance.
- Identical product totals overall and separately for each source version.
- Unique country enrichment keys before joins and in the persisted tables.
- No global rollups in detail tables; exact source records retained separately.
- A round trip from stored male/female pivot columns to all 70 seller detail
  records, plus country totals and means reconciled with detail.
- Original-source buyer and seller population definitions, verified against
  Silver users; correct source-version labels on historical metrics.
- Global rollups reconciled with detail, at the matching sex or all-sexes level.
- Exact historical context values and non-additive Delta schema metadata.

`validation_report.json` records actual row counts, schemas, null counts, Delta
versions, Gold contracts, product totals, and all semantic check results.
`--validate-only` reopens the same 14 tables without rewriting them and writes
`validation_only_report.json`. Notebook/source hashes must remain unchanged.
Any failed check gives a nonzero exit code and records failure details.

The corrected full pipeline passed all 14 validations in 108.05 seconds under
the normal Windows account, following a successful sandbox run. Actual user
product totals matched Silver exactly:

| User measure | Silver total | Corrected Gold total |
| --- | ---: | ---: |
| Products sold | 56,299 | 56,299 |
| Products bought | 73,487 | 73,487 |
| Products wished | 466,643 | 466,643 |
| Products listed | 22,148 | 22,148 |
| Social products liked | 1,513,050 | 1,513,050 |

There were 119,656 unique user keys, zero missing users, zero unexpected users,
and no global rollups in any detail table. The preserved Bronze CSV reader and
all Silver transformation functions match the backed-up implementation's syntax
trees. All 21 original notebook/CSV files and all 11 backed-up files match their
pre-change checksums. These preservation results are recorded separately in
`output/gold_revision_verification.json`.

A fresh Spark process subsequently passed read-only validation of all 14 tables
in 76.5 seconds. Row counts and Delta versions matched the full-run report;
`validation_only_report.json` also has `status: passed`.

The pipeline uses Delta `overwrite` transactions rather than appending input
rows, so reruns replace each table's active snapshot without accumulating
duplicate records. Previous Delta versions may retain older data files; use
Delta readers and transaction metadata, not a raw glob over all Parquet files.
There is no cross-table transaction: a failed run may leave some layers refreshed;
rerun the full pipeline to bring all tables into agreement. Nothing is vacuumed
or recursively deleted.

To read outputs from Python, reuse the configured session:

```python
from run_pipeline import create_spark, ROOT

spark = create_spark()
try:
    gold = spark.read.format('delta').load(
        (ROOT / 'output/gold/ecom_one_big_table').as_uri()
    )
    gold.filter("Country = 'France'").select('Country', 'Users_identifierHash', 'Users_source_snapshot',
                'Users_productsSold',
                'Context_original_sellers_female_nbsellers',
                'Context_original_sellers_male_nbsellers').show(10)
    # Country facts are summed at country grain, outside the user OBT.
    buyers = spark.read.format('delta').load(
        (ROOT / 'output/gold/country_buyers').as_uri()
    )
    buyers.selectExpr('sum(buyers) AS original_source_buyers').show()
finally:
    spark.stop()
```
