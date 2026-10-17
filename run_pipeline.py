"""Run the notebooks' Bronze -> Silver -> Gold transformations on local CSVs.

Original notebooks and CSVs are read-only inputs. All tables are actual Delta
tables, addressed by path, so neither a metastore nor a cloud account is needed.
"""
from __future__ import annotations

import argparse
from collections import Counter
import csv
from datetime import datetime, timezone
import hashlib
from importlib.metadata import version
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys
import time
from urllib.parse import unquote

from pyspark.sql import DataFrame, SparkSession, functions as F, types as T
from delta.tables import DeltaTable

from setup_runtime import JARS, ROOT, RUNTIME
from gold_layer import (CONTRACTS as GOLD_CONTRACTS, EXPECTED_ROWS as GOLD_EXPECTED_ROWS,
                        build_gold_aggregates, gold_table, validate_gold)

SOURCES = {
    'buyers': 'Buyers-repartition-by-country.csv',
    'sellers': 'Comparison-of-Sellers-by-Gender-and-Country.csv',
    'countries': 'Countries-with-Top-Sellers-(Fashion-C2C).csv',
}
BUYER_INTS = '''buyers topbuyers femalebuyers malebuyers topfemalebuyers
topmalebuyers totalproductsbought totalproductswished totalproductsliked
toptotalproductsbought toptotalproductswished toptotalproductsliked'''.split()
BUYER_DECIMALS = '''topbuyerratio femalebuyersratio topfemalebuyersratio
boughtperwishlistratio boughtperlikeratio topboughtperwishlistratio
topboughtperlikeratio meanproductsbought meanproductswished meanproductsliked
topmeanproductsbought topmeanproductswished topmeanproductsliked meanofflinedays
topmeanofflinedays meanfollowers meanfollowing topmeanfollowers topmeanfollowing'''.split()
SELLER_INTS = 'nbsellers totalproductssold totalproductslisted totalbought totalwished totalproductsliked'.split()
SELLER_DECIMALS = '''meanproductssold meanproductslisted meansellerpassrate
meanproductsbought meanproductswished meanproductsliked meanfollowers meanfollows
percentofappusers percentofiosusers meanseniority'''.split()
COUNTRY_INTS = '''sellers topsellers femalesellers malesellers topfemalesellers
topmalesellers toptotalproductssold totalproductssold toptotalproductslisted totalproductslisted'''.split()
COUNTRY_DECIMALS = '''topsellerratio femalesellersratio topfemalesellersratio
countrysoldratio bestsoldratio topmeanproductssold topmeanproductslisted
meanproductssold meanproductslisted meanofflinedays topmeanofflinedays
meanfollowers meanfollowing topmeanfollowers topmeanfollowing'''.split()
USER_INTS = '''socialnbfollowers socialnbfollows socialproductsliked productslisted
productssold productswished productsbought civilitygenderid dayssincelastlogin seniority'''.split()
USER_BOOLS = 'hasanyapp hasandroidapp hasiosapp hasprofilepicture'.split()
USER_DECIMALS = 'productspassrate seniorityasmonths seniorityasyears'.split()
USER_TEXT = 'type country language gender civilitytitle countrycode'.split()
NOTEBOOKS = [*sorted(ROOT.glob('*.ipynb')), *sorted((ROOT / 'data').glob('*.ipynb'))]


def check(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError('Validation failed: ' + message)


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def local_uri(path: Path) -> str:
    # Hadoop's Path keeps percent escapes literally, including the parentheses
    # in the actual countries CSV filename. Supply an unescaped file URI.
    return unquote(path.resolve().as_uri())


def inspect_csv(path: Path) -> dict:
    """Independently count/inspect source rows with Python, before Spark reads."""
    check(path.is_file(), f'missing source {path}')
    missing = Counter()
    with path.open(encoding='utf-8-sig', newline='') as handle:
        reader = csv.DictReader(handle)
        columns = reader.fieldnames
        check(bool(columns), f'empty CSV {path}')
        check(len(set(c.lower() for c in columns)) == len(columns), f'duplicate CSV columns: {path}')
        count = 0
        for row in reader:
            check(None not in row and all(v is not None for v in row.values()), f'malformed CSV record {path}:{count + 2}')
            count += 1
            missing.update(c.lower() for c, value in row.items() if value == '')
    return {'file': str(path.relative_to(ROOT)), 'rows': count, 'columns': columns,
            'missing': dict(missing), 'sha256': sha256(path)}


def configure_environment() -> dict:
    check((3, 10) <= sys.version_info[:2] <= (3, 11), 'use Python 3.10-3.11 for this pinned setup (3.11 verified)')
    check(version('pyspark') == '3.5.8' and version('delta-spark') == '3.3.2', 'install requirements.txt into .venv')
    for name in JARS:
        check((RUNTIME / 'jars' / name).is_file(), 'run setup_runtime.py first')
    # Use this interpreter for both Spark driver and workers, ignoring any
    # inherited global Spark/Python installation from the active conda shell.
    import pyspark
    os.environ['SPARK_HOME'] = str(Path(pyspark.__file__).parent)
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'
    java = shutil.which('java')
    configured_java = Path(os.environ.get('JAVA_HOME', '')) / 'bin' / ('java.exe' if os.name == 'nt' else 'java')
    if configured_java.is_file():
        java = str(configured_java)
    check(bool(java), 'Java 17 must be available on PATH or under JAVA_HOME')
    result = subprocess.run([java, '-version'], capture_output=True, text=True, check=True)
    java_version = (result.stderr or result.stdout).strip()
    check('version "17.' in java_version, 'this reproducible setup requires Java 17')
    # Conda puts java.exe in Library/bin and the JVM in Library/lib/jvm.
    # Point Spark's launcher at the directory that actually has bin/java.exe.
    os.environ['JAVA_HOME'] = str(Path(java).parent.parent)
    if os.name == 'nt':
        native = RUNTIME / 'hadoop' / 'bin'
        check(all((native / n).is_file() for n in ('hadoop.dll', 'winutils.exe')), 'run setup_runtime.py for Windows helpers')
        os.environ['HADOOP_HOME'] = str(native.parent)
        os.environ['PATH'] = str(native) + os.pathsep + os.environ['PATH']
    (RUNTIME / 'spark-tmp').mkdir(parents=True, exist_ok=True)
    os.environ['SPARK_LOCAL_DIRS'] = str(RUNTIME / 'spark-tmp')
    return {'python': platform.python_version(), 'python_executable': sys.executable,
            'platform': platform.platform(), 'java': java_version,
            'java_home': os.environ['JAVA_HOME'],
            'pyspark': version('pyspark'), 'delta_spark': version('delta-spark'),
            'hadoop_home': os.environ.get('HADOOP_HOME')}


def create_spark(master: str = 'local[2]') -> SparkSession:
    configure_environment()
    check(master == 'local' or (master.startswith('local[') and master.endswith(']')),
          'this runner is configured for local Spark execution')
    builder = (SparkSession.builder.appName('LocalEcommerceMedallion').master(master)
               # Local workers share this JVM. Loading on the driver classpath
               # also avoids Windows locking Spark's temporary copies of JARs.
               .config('spark.driver.extraClassPath', os.pathsep.join((RUNTIME / 'jars' / name).as_posix() for name in JARS))
               .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
               .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
               .config('spark.sql.warehouse.dir', (RUNTIME / 'warehouse').as_uri())
               .config('spark.driver.host', '127.0.0.1')
               .config('spark.driver.bindAddress', '127.0.0.1')
               .config('spark.driver.memory', '2g')
               .config('spark.sql.shuffle.partitions', '4')
               .config('spark.default.parallelism', '2')
               .config('spark.databricks.delta.snapshotPartitions', '2')
               .config('spark.sql.session.timeZone', 'Asia/Kolkata')
               .config('spark.sql.ansi.enabled', 'false')
               .config('spark.ui.enabled', 'false')
               .config('spark.ui.showConsoleProgress', 'false'))
    if os.name == 'nt':
        # Spark's Java option parser consumes backslashes as escapes.
        native_path = (RUNTIME / 'hadoop' / 'bin').as_posix()
        builder = builder.config('spark.driver.extraJavaOptions', f'-Djava.library.path="{native_path}"')
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')
    if os.name == 'nt':
        check(spark._jvm.org.apache.hadoop.util.NativeCodeLoader.isNativeCodeLoaded(),
              'Windows Hadoop native library failed to load; check .runtime/hadoop/bin')
    return spark


def csv_schema(headers: list[str], table: str) -> T.StructType:
    ints = {'users': USER_INTS, 'buyers': BUYER_INTS, 'sellers': SELLER_INTS, 'countries': COUNTRY_INTS}[table]
    decimals = {'users': USER_DECIMALS, 'buyers': BUYER_DECIMALS,
                'sellers': SELLER_DECIMALS, 'countries': COUNTRY_DECIMALS}[table]
    fields = []
    for name in headers:
        lower = name.lower()
        if lower in ('identifierhash', 'websitelongevity'):
            dtype = T.LongType()
        elif lower in ints:
            dtype = T.IntegerType()
        elif lower in decimals:
            dtype = T.DoubleType()
        elif table == 'users' and lower in USER_BOOLS:
            dtype = T.BooleanType()
        else:
            dtype = T.StringType()
        fields.append(T.StructField(lower, dtype, True))
    return T.StructType(fields)


def read_csv(spark: SparkSession, path: Path, profile: dict, table: str) -> DataFrame:
    return (spark.read.schema(csv_schema(profile['columns'], table))
            .option('header', 'true').option('encoding', 'UTF-8')
            .option('enforceSchema', 'false').option('mode', 'FAILFAST')
            .csv(local_uri(path)).withColumn('source_file', F.lit(str(path.relative_to(ROOT)))))


def cast_columns(df: DataFrame, ints: list[str], decimals: list[str]) -> DataFrame:
    casts = {**{c: T.IntegerType() for c in ints}, **{c: T.DecimalType(10, 2) for c in decimals}}
    return df.select(*(F.col(c).cast(casts[c]).alias(c) if c in casts else F.col(c) for c in df.columns))


def silver_users(df: DataFrame) -> DataFrame:
    # Country normalization must also apply to users so all four join keys agree.
    df = (df.withColumn('country', F.initcap(F.trim('country')))
          .withColumn('countrycode', F.upper('countrycode'))
          .withColumn('language_full', F.when(F.upper('language') == 'EN', 'English')
                      .when(F.upper('language') == 'FR', 'French').otherwise('Other'))
          .withColumn('gender', F.when(F.col('gender').startswith('M'), 'Male')
                      .when(F.col('gender').startswith('F'), 'Female').otherwise('Other'))
          .withColumn('civilitytitle_clean', F.regexp_replace('civilitytitle', '(?i)(Mme|Ms|Mrs)', 'Ms'))
          .withColumn('years_since_last_login', F.col('dayssincelastlogin') / 365)
          .withColumn('account_age_years', F.round(F.col('seniority') / 365, 2))
          .withColumn('account_age_group', F.when(F.col('account_age_years') < 1, 'New')
                      .when(F.col('account_age_years') < 3, 'Intermediate').otherwise('Experienced'))
          .withColumn('current_year', F.year(F.current_date()))
          .withColumn('user_descriptor', F.concat(F.col('gender'), F.lit('_'), F.col('countrycode'),
                      F.lit('_'), F.substring('civilitytitle_clean', 1, 3), F.lit('_'), F.col('language_full')))
          .withColumn('flag_long_title', F.length('civilitytitle') > 10))
    for name in USER_BOOLS:
        df = df.withColumn(name, F.col(name).cast('boolean'))
    df = cast_columns(df, ['socialnbfollowers', 'socialnbfollows'], USER_DECIMALS)
    # Preserve the notebook's ordering: unknown login age remains null, while
    # daysSinceLastLogin gets its original zero default only in Silver.
    return df.withColumn('dayssincelastlogin', F.coalesce(F.col('dayssincelastlogin').cast('int'), F.lit(0)))


def silver_buyers(df: DataFrame) -> DataFrame:
    df = cast_columns(df, BUYER_INTS, BUYER_DECIMALS)
    return (df.withColumn('country', F.initcap(F.trim('country'))).fillna(0, subset=BUYER_INTS)
            .withColumn('female_to_male_ratio', F.round(F.col('femalebuyers') / (F.col('malebuyers') + 1), 2))
            .withColumn('wishlist_to_purchase_ratio', F.round(F.col('totalproductswished') / (F.col('totalproductsbought') + 1), 2))
            .withColumn('high_engagement', F.when(F.col('boughtperwishlistratio') > 0.5, True).otherwise(False))
            .withColumn('growing_female_market', F.when(F.col('femalebuyersratio') > F.col('topfemalebuyersratio'), True).otherwise(False)))


def silver_sellers(df: DataFrame) -> DataFrame:
    df = cast_columns(df, SELLER_INTS, SELLER_DECIMALS)
    df = (df.withColumn('country', F.initcap(F.trim('country'))).withColumn('sex', F.upper('sex'))
          .withColumn('seller_size_category', F.when(F.col('nbsellers') < 500, 'Small')
                      .when(F.col('nbsellers') < 2000, 'Medium').otherwise('Large'))
          .withColumn('mean_products_listed_per_seller', F.round(F.col('totalproductslisted') / F.col('nbsellers'), 2))
          .withColumn('high_seller_pass_rate', F.when(F.col('meansellerpassrate') > 0.75, 'High').otherwise('Normal')))
    average = df.select(F.round(F.avg('meansellerpassrate'), 2)).first()[0]
    return df.withColumn('meansellerpassrate', F.coalesce('meansellerpassrate', F.lit(average)).cast(T.DecimalType(10, 2)))


def silver_countries(df: DataFrame) -> DataFrame:
    df = cast_columns(df, COUNTRY_INTS, COUNTRY_DECIMALS)
    return (df.withColumn('country', F.initcap(F.trim('country')))
            .withColumn('top_seller_ratio', F.round(F.col('topsellers') / F.col('sellers'), 2))
            .withColumn('high_female_seller_ratio', F.when(F.col('femalesellersratio') > 0.5, True).otherwise(False))
            .withColumn('performance_indicator', F.round(F.col('toptotalproductssold') / (F.col('toptotalproductslisted') + 1), 2))
            .withColumn('high_performance', F.when(F.col('performance_indicator') > 0.8, True).otherwise(False))
            .withColumn('activity_level', F.when(F.col('meanofflinedays') < 30, 'Highly Active')
                        .when(F.col('meanofflinedays') < 60, 'Moderately Active').otherwise('Low Activity')))


def validate_table(spark: SparkSession, path: Path, expected: int) -> tuple[DataFrame, dict]:
    uri = path.as_uri()
    check(DeltaTable.isDeltaTable(spark, uri), f'not a Delta table: {path}')
    df = spark.read.format('delta').load(uri)
    count = df.count()
    check(count == expected, f'{path.name} row count {count} != expected {expected}')
    check(len({c.lower() for c in df.columns}) == len(df.columns), f'ambiguous columns: {path}')
    table = DeltaTable.forPath(spark, uri)
    detail = table.detail().first().asDict()
    history = table.history(1).first().asDict()
    check(detail['format'] == 'delta' and detail['numFiles'] > 0, f'invalid Delta metadata: {path}')
    # Check active files from the transaction snapshot, not globbed stale files
    # left by earlier Delta overwrite versions. Spark inputFiles understands it.
    active_files = df.inputFiles()
    check(bool(active_files), f'no active data files: {path}')
    nulls = df.select(*(F.sum(F.col(c).isNull().cast('long')).alias(c) for c in df.columns)).first().asDict()
    return df, {'rows': count, 'columns': len(df.columns), 'path': str(path),
                'format': detail['format'], 'active_files': detail['numFiles'],
                'size_bytes': detail['sizeInBytes'], 'delta_version': history['version'],
                'last_operation': history['operation'], 'schema': df.schema.jsonValue(),
                'null_counts': nulls}


def run(args: argparse.Namespace) -> None:
    started = time.monotonic()
    output = args.output_dir.resolve()
    check(output.is_relative_to(ROOT) and output != ROOT, 'output directory must be a subdirectory of this project')
    check(not output.is_relative_to(ROOT / 'data') and not output.is_relative_to(RUNTIME)
          and not output.is_relative_to(ROOT / '.venv') and not output.is_relative_to(ROOT / 'backups'),
          'output must not overwrite input, backup, or runtime directories')
    output.mkdir(parents=True, exist_ok=True)
    notebooks_before = {str(p.relative_to(ROOT)): sha256(p) for p in NOTEBOOKS}
    report = {'status': 'running', 'started_at_utc': datetime.now(timezone.utc).isoformat(),
              'environment': configure_environment(), 'tables': {}, 'sources': {}, 'checks': {}}
    chunks = [ROOT / 'data' / 'chunk-data' / f'chunk{i}.csv' for i in range(1, 11)]
    profiles = {p: inspect_csv(p) for p in chunks}
    summary_paths = {name: ROOT / 'data' / filename for name, filename in SOURCES.items()}
    profiles.update({p: inspect_csv(p) for p in summary_paths.values()})
    report['sources'] = {str(p.relative_to(ROOT)): profile for p, profile in profiles.items()}
    # Each snapshot has a different CSV header. Never read all chunks together
    # under one positional schema; union their individually parsed columns.
    required_users = {'identifierhash', *USER_TEXT, *USER_INTS, *USER_BOOLS, *USER_DECIMALS}
    new_missing = {'hasanyapp', 'hasandroidapp', 'hasiosapp', 'dayssincelastlogin'}
    for i, path in enumerate(chunks, 1):
        cols = {c.lower() for c in profiles[path]['columns']}
        check(required_users - cols == (set() if i <= 5 else new_missing), f'unexpected user schema: {path.name}')
        check(cols - required_users == (set() if i <= 5 else {'websitelongevity'}), f'unexpected user columns: {path.name}')
    for name, path in summary_paths.items():
        expected_cols = {
            'buyers': {'country', *BUYER_INTS, *BUYER_DECIMALS},
            'sellers': {'country', 'sex', *SELLER_INTS, *SELLER_DECIMALS},
            'countries': {'country', *COUNTRY_INTS, *COUNTRY_DECIMALS},
        }[name]
        check({c.lower() for c in profiles[path]['columns']} == expected_cols, f'{name} schema differs from notebooks')
    spark = None
    try:
        spark = create_spark(args.master)
        report['environment']['spark'] = spark.version
        report['environment']['hadoop'] = spark._jvm.org.apache.hadoop.util.VersionInfo.getVersion()
        bronze = {}
        expected_rows = {'users': sum(profiles[p]['rows'] for p in chunks)}
        expected_rows.update({name: profiles[p]['rows'] for name, p in summary_paths.items()})
        if not args.validate_only:
            print('Bronze: ingesting existing CSV files', flush=True)
            users = None
            for i, path in enumerate(chunks, 1):
                frame = read_csv(spark, path, profiles[path], 'users').withColumn(
                    'source_snapshot', F.lit('original' if i <= 5 else '2024'))
                users = frame if users is None else users.unionByName(frame, allowMissingColumns=True)
            raw = {'users': users, **{name: read_csv(spark, path, profiles[path], name)
                                     for name, path in summary_paths.items()}}
            for name, frame in raw.items():
                path = output / 'bronze' / name
                frame.coalesce(2).write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(path.as_uri())
                bronze[name], report['tables'][f'bronze.{name}'] = validate_table(spark, path, expected_rows[name])
                print(f'  bronze.{name}: {expected_rows[name]:,} rows', flush=True)
        else:
            for name in expected_rows:
                bronze[name], report['tables'][f'bronze.{name}'] = validate_table(spark, output / 'bronze' / name, expected_rows[name])
        # Explicit schema + FAILFAST prevents malformed values from silently
        # becoming null. Verify null preservation against the independent scan.
        for name, frame in bronze.items():
            relevant = chunks if name == 'users' else [summary_paths[name]]
            expected_nulls = Counter()
            for path in relevant:
                profile = profiles[path]
                expected_nulls.update(profile['missing'])
                present = {c.lower() for c in profile['columns']}
                for column in frame.columns:
                    if column not in present and column not in ('source_file', 'source_snapshot'):
                        expected_nulls[column] += profile['rows']
            nulls = report['tables'][f'bronze.{name}']['null_counts']
            check(all(nulls[c] == expected_nulls[c] for c in nulls), f'{name} unexpected nulls from CSV parsing')
        report['checks']['bronze_nulls_match_source_schemas'] = True
        transforms = {'users': silver_users, 'buyers': silver_buyers,
                      'sellers': silver_sellers, 'countries': silver_countries}
        silver = {}
        print('Silver: validating cleansed Delta tables' if args.validate_only else 'Silver: applying notebook transformations', flush=True)
        for name, transform in transforms.items():
            path = output / 'silver' / name
            if not args.validate_only:
                transform(bronze[name]).coalesce(2).write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(path.as_uri())
            silver[name], report['tables'][f'silver.{name}'] = validate_table(spark, path, expected_rows[name])
            print(f'  silver.{name}: {expected_rows[name]:,} rows', flush=True)
            types = dict(silver[name].dtypes)
            decimals = {'users': USER_DECIMALS, 'buyers': BUYER_DECIMALS,
                        'sellers': SELLER_DECIMALS, 'countries': COUNTRY_DECIMALS}[name]
            check(all(types[c] == 'decimal(10,2)' for c in decimals), f'{name} decimal casts')
            check(silver[name].filter(~F.col('country').eqNullSafe(F.initcap(F.trim('country')))).limit(1).count() == 0,
                  f'{name} country normalization')
            # Verify the complete persisted transformation, not just row counts.
            recomputed = transform(bronze[name])
            persisted = silver[name].select(recomputed.columns)
            check(persisted.exceptAll(recomputed).limit(1).count() == 0
                  and recomputed.exceptAll(persisted).limit(1).count() == 0,
                  f'{name} Silver values differ from transformations')
        report['checks']['silver_values_match_transformations'] = True
        language_errors = silver['users'].filter((F.upper('language') == 'EN') & (F.col('language_full') != 'English')).count()
        check(language_errors == 0, 'English language mapping')
        report['checks']['language_mapping'] = True
        print('Gold: validating user-grain and aggregate tables' if args.validate_only
              else 'Gold: separating rollups and pivoting seller groups before LEFT JOINs', flush=True)
        aggregates = build_gold_aggregates(silver)
        gold_outputs = {}
        report['gold_contracts'] = GOLD_CONTRACTS
        # Persist native-grain aggregates before enriching users with historical
        # context. User rows always enter LEFT JOINs against unique country keys.
        for name in GOLD_EXPECTED_ROWS:
            path = output / 'gold' / name
            if not args.validate_only:
                frame = gold_table(silver, gold_outputs) if name == 'ecom_one_big_table' else aggregates[name]
                (frame.coalesce(2).write.format('delta').mode('overwrite').option('overwriteSchema', 'true')
                 .option('userMetadata', json.dumps(GOLD_CONTRACTS[name])).save(local_uri(path)))
            gold_outputs[name], report['tables']['gold.' + name] = validate_table(spark, path, GOLD_EXPECTED_ROWS[name])
            print(f'  gold.{name}: {GOLD_EXPECTED_ROWS[name]:,} rows', flush=True)
        print('Gold: checking user keys, product totals, historical populations and non-additive context', flush=True)
        report['checks'].update(validate_gold(silver, gold_outputs))
        report['checks']['all_input_users_preserved'] = True
        gold = gold_outputs['ecom_one_big_table']
        print('\nGold preview (one row per user/source version; country context is non-additive):', flush=True)
        gold.select('Country', 'Users_identifierHash', 'Users_source_snapshot',
                    'Context_original_sellers_female_nbsellers', 'Context_original_sellers_male_nbsellers').orderBy(
            F.col('Country').asc_nulls_last(), 'Users_identifierHash', 'Users_source_snapshot').show(5, truncate=False)
        report['status'] = 'passed'
    except Exception as exc:
        report['status'] = 'failed'
        report['error'] = f'{type(exc).__name__}: {exc}'
        raise
    finally:
        if spark is not None:
            spark.stop()
        notebooks_after = {str(p.relative_to(ROOT)): sha256(p) for p in NOTEBOOKS}
        report['checks']['notebooks_unchanged'] = notebooks_before == notebooks_after
        report['notebook_sha256'] = notebooks_after
        sources_unchanged = all(sha256(p) == profile['sha256'] for p, profile in profiles.items())
        report['checks']['source_csvs_unchanged'] = sources_unchanged
        if not sources_unchanged or notebooks_before != notebooks_after:
            report['status'] = 'failed'
        report['finished_at_utc'] = datetime.now(timezone.utc).isoformat()
        report['elapsed_seconds'] = round(time.monotonic() - started, 2)
        report_path = output / ('validation_only_report.json' if args.validate_only else 'validation_report.json')
        report_path.write_text(json.dumps(report, indent=2, default=str) + '\n', encoding='utf-8')
        print(f'\nValidation report: {report_path}', flush=True)
    check(report['status'] == 'passed', 'input files changed during execution')
    print(f'All {len(report["tables"])} Delta tables validated in {report["elapsed_seconds"]:.1f}s.', flush=True)


def main() -> None:
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8')
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir', type=Path, default=ROOT / 'output')
    parser.add_argument('--master', default='local[2]', help='Spark local master, default local[2]')
    parser.add_argument('--validate-only', action='store_true', help='read and validate existing Delta tables without rewriting')
    run(parser.parse_args())


if __name__ == '__main__':
    main()
