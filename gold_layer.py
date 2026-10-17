"""Gold data contracts, user-grain enrichment, and semantic validation.

Silver is an unchanged input. Historical country measures are explicitly
non-additive context on user rows and additive only at their own stated grain.
"""
from __future__ import annotations

from functools import reduce
from pyspark.sql import DataFrame, functions as F, types as T

EXPECTED_USERS = 119_656
HISTORICAL_SNAPSHOT = 'original'
SELLER_POPULATION = 'original-source users with productsSold > 0 OR productsListed > 0'
BUYER_POPULATION = 'original-source users with productsBought > 0'
COUNTRY_POPULATION = 'original-source users with productsSold > 0; supplied top-seller countries only'
NON_ADDITIVE = 'do_not_sum_across_users'
SELLER_TOTALS = {
    'nbsellers': 'seller_count_sales_or_listings',
    'totalproductssold': 'total_products_sold',
    'totalproductslisted': 'total_products_listed',
    'totalbought': 'total_products_bought',
    'totalwished': 'total_products_wished',
    'totalproductsliked': 'total_products_liked',
}
USER_ALIASES = {
    'country': 'Country',
    'identifierhash': 'Users_identifierHash',
    'productssold': 'Users_productsSold',
    'productswished': 'Users_productsWished',
    'productsbought': 'Users_productsBought',
    'productslisted': 'Users_productsListed',
}
EXPECTED_ROWS = {
    'country_buyers': 62,
    'seller_country_gender': 70,
    'country_sellers': 47,
    'country_statistics': 19,
    'seller_global_rollups': 3,
    'ecom_one_big_table': EXPECTED_USERS,
}
CONTRACTS = {
    'ecom_one_big_table': {
        'grain': ['Users_source_snapshot', 'Users_identifierHash'],
        'population': 'all source-user records; original and 2024 remain separate source versions',
        'aggregation': 'sum user measures within source version; never sum Context_original_* across users',
    },
    'country_buyers': {
        'grain': ['metrics_source_snapshot', 'country'], 'population': BUYER_POPULATION,
        'aggregation': 'sum counts/totals over distinct countries within this source version; ratios/means are not additive',
    },
    'country_statistics': {
        'grain': ['metrics_source_snapshot', 'country'], 'population': COUNTRY_POPULATION,
        'aggregation': 'sum counts/totals over the 19 supplied countries; ratios/means are not additive',
    },
    'seller_country_gender': {
        'grain': ['metrics_source_snapshot', 'country', 'sex'], 'population': SELLER_POPULATION,
        'aggregation': 'sum detail counts/totals across disjoint country/sex groups; ratios/means are not additive',
    },
    'country_sellers': {
        'grain': ['metrics_source_snapshot', 'country'], 'population': SELLER_POPULATION,
        'aggregation': 'sum combined totals OR sex-specific totals; never add both representations together',
    },
    'seller_global_rollups': {
        'grain': ['metrics_source_snapshot', 'rollup_key'], 'population': SELLER_POPULATION,
        'aggregation': 'select one aggregation level; never sum rollups with detail or grand total with sex subtotals',
    },
}


def require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError('Gold validation failed: ' + message)


def user_alias(column: str) -> str:
    return USER_ALIASES.get(column, 'Users_' + column)


def assert_unique(df: DataFrame, keys: list[str], label: str) -> None:
    null_key = reduce(lambda left, right: left | right, (F.col(k).isNull() for k in keys))
    require(df.filter(null_key).limit(1).count() == 0, label + ' has null keys')
    require(df.groupBy(*keys).count().filter(F.col('count') > 1).limit(1).count() == 0,
            label + ' has duplicate keys: ' + ', '.join(keys))


def assert_country_detail(df: DataFrame, label: str) -> None:
    require(df.filter(F.col('country').isNull() | (F.trim('country') == '')).limit(1).count() == 0,
            label + ' contains a global rollup or blank country')


def same_rows(actual: DataFrame, expected: DataFrame, label: str) -> None:
    actual = actual.select(*expected.columns)
    require(actual.exceptAll(expected).limit(1).count() == 0
            and expected.exceptAll(actual).limit(1).count() == 0, label + ' differs from source values')


def seller_metrics(sellers: DataFrame) -> list[str]:
    return [c for c in sellers.columns if c not in ('country', 'sex', 'source_file')]


def tag(df: DataFrame, population: str, dataset: str, level: str) -> DataFrame:
    return (df.withColumn('metrics_source_snapshot', F.lit(HISTORICAL_SNAPSHOT))
            .withColumn('population_definition', F.lit(population))
            .withColumn('source_dataset', F.lit(dataset))
            .withColumn('aggregation_level', F.lit(level)))


def seller_parts(sellers: DataFrame) -> tuple[DataFrame, DataFrame]:
    is_rollup = F.col('country').isNull() | (F.trim('country') == '')
    return sellers.filter(~is_rollup), sellers.filter(is_rollup)


def build_gold_aggregates(silver: dict[str, DataFrame]) -> dict[str, DataFrame]:
    """Build explicit-grain tables; never deduplicate arbitrary source records."""
    assert_unique(silver['users'], ['source_snapshot', 'identifierhash'], 'Silver users')
    for name in ('buyers', 'countries'):
        assert_country_detail(silver[name], name)
        assert_unique(silver[name], ['country'], name + ' enrichment')
    details, rollups = seller_parts(silver['sellers'])
    assert_unique(details, ['country', 'sex'], 'seller country/sex detail')
    require(details.filter(~F.col('sex').isin('FEMALE', 'MALE')).limit(1).count() == 0,
            'unrecognized seller sex group')
    metrics = seller_metrics(silver['sellers'])
    # Explicit pivot values keep the schema stable even when a sex group has no
    # records in a country. The precondition above makes FIRST unambiguous.
    pivot = (details.groupBy('country').pivot('sex', ['FEMALE', 'MALE'])
             .agg(*(F.first(c).alias(c) for c in metrics)))
    pivot = pivot.select('country', *(F.col(c).alias(c.lower()) for c in pivot.columns if c != 'country'))
    totals = details.groupBy('country').agg(*(F.sum(c).alias(alias) for c, alias in SELLER_TOTALS.items()))
    for noun in ('sold', 'listed', 'bought', 'wished', 'liked'):
        totals = totals.withColumn('mean_products_' + noun + '_per_seller',
            F.round(F.col('total_products_' + noun) / F.col('seller_count_sales_or_listings'), 4).cast(T.DecimalType(18, 4)))
    combined = totals.join(pivot, ['country'], 'left')
    assert_unique(combined, ['country'], 'pivoted seller country enrichment')
    tagged_rollups = (tag(rollups, SELLER_POPULATION, 'Comparison-of-Sellers-by-Gender-and-Country.csv', 'global')
                      .withColumn('rollup_key', F.coalesce('sex', F.lit('ALL')))
                      .withColumn('aggregation_level', F.when(F.col('sex').isNull(), 'global_all_sexes')
                                  .otherwise('global_gender')))
    return {
        'country_buyers': tag(silver['buyers'], BUYER_POPULATION, 'Buyers-repartition-by-country.csv', 'country'),
        'seller_country_gender': tag(details, SELLER_POPULATION, 'Comparison-of-Sellers-by-Gender-and-Country.csv', 'country_gender'),
        'country_sellers': tag(combined, SELLER_POPULATION, 'Comparison-of-Sellers-by-Gender-and-Country.csv', 'country'),
        'country_statistics': tag(silver['countries'], COUNTRY_POPULATION, 'Countries-with-Top-Sellers-(Fashion-C2C).csv', 'country'),
        'seller_global_rollups': tagged_rollups,
    }


def country_contexts(aggregates: dict[str, DataFrame]) -> list[DataFrame]:
    definitions = {
        'country_buyers': ('buyers', ['buyers', 'topbuyers', 'femalebuyers', 'malebuyers', 'topfemalebuyers', 'topmalebuyers']),
        'country_statistics': ('countries', ['sellers', 'topsellers', 'femalesellers', 'malesellers', 'topfemalesellers', 'topmalesellers']),
        'country_sellers': ('sellers', [c for c in aggregates['country_sellers'].columns
                                      if c.startswith(('female_', 'male_'))]),
    }
    contexts = []
    for table, (prefix, columns) in definitions.items():
        df = aggregates[table]
        assert_country_detail(df, table)
        assert_unique(df, ['country'], table + ' enrichment')
        population = CONTRACTS[table]['population']
        metadata = {'aggregation_rule': NON_ADDITIVE, 'source_snapshot': HISTORICAL_SNAPSHOT,
                    'source_table': table, 'population_definition': population, 'native_grain': 'country'}
        contexts.append(df.select('country', *(F.col(c).alias(f'Context_original_{prefix}_{c}', metadata=metadata) for c in columns)))
    return contexts


def gold_table(silver: dict[str, DataFrame], aggregates: dict[str, DataFrame]) -> DataFrame:
    """One user row remains one row through every LEFT JOIN."""
    joined = silver['users'].select(*(F.col(c).alias(user_alias(c)) for c in silver['users'].columns))
    for context in country_contexts(aggregates):
        joined = joined.join(context.withColumnRenamed('country', 'Country'), ['Country'], 'left')
    return (joined.withColumn('context_source_snapshot', F.lit(HISTORICAL_SNAPSHOT))
            .withColumn('context_matches_user_snapshot', F.col('Users_source_snapshot') == HISTORICAL_SNAPSHOT)
            .withColumn('context_measure_usage', F.lit('historical country context; do not sum across user rows')))


def validate_gold(silver: dict[str, DataFrame], gold: dict[str, DataFrame]) -> dict:
    """Validate business grain, additive facts, provenance, and pivot fidelity."""
    users, obt = silver['users'], gold['ecom_one_big_table']
    input_count, output_count = users.count(), obt.count()
    require(input_count == output_count == EXPECTED_USERS, 'expected exactly 119,656 user records')
    assert_unique(users, ['source_snapshot', 'identifierhash'], 'Silver users')
    keys = ['Users_source_snapshot', 'Users_identifierHash']
    assert_unique(obt, keys, 'Gold users')
    source_keys = users.select(F.col('source_snapshot').alias(keys[0]), F.col('identifierhash').alias(keys[1]))
    missing = source_keys.join(obt.select(*keys), keys, 'left_anti').count()
    extra = obt.select(*keys).join(source_keys, keys, 'left_anti').count()
    require(missing == extra == 0, 'missing or unexpected user keys')
    # Compare every user attribute, including provenance, beyond just count/keys.
    same_rows(obt.select(*(F.col(user_alias(c)).alias(c) for c in users.columns)), users, 'Gold user attributes')
    checks = {'gold_user_rows': output_count, 'gold_unique_user_keys': output_count,
              'missing_source_users': missing, 'unexpected_gold_users': extra,
              'all_user_attributes_preserved': True}
    products = ['productssold', 'productsbought', 'productswished', 'productslisted', 'socialproductsliked']
    def product_totals(df, user_columns=False):
        return df.agg(*(F.sum(user_alias(c) if user_columns else c).alias(c) for c in products)).first().asDict()
    baseline, actual = product_totals(users), product_totals(obt, True)
    require(actual == baseline, 'changed user-level product totals')
    before_by_version = users.groupBy('source_snapshot').agg(*(F.sum(c).alias(c) for c in products))
    after_by_version = obt.groupBy(F.col('Users_source_snapshot').alias('source_snapshot')).agg(
        *(F.sum(user_alias(c)).alias(c) for c in products))
    same_rows(after_by_version, before_by_version, 'product totals by source version')
    checks['user_product_totals'] = {'silver': baseline, 'gold': actual}
    checks['user_product_totals_by_snapshot'] = [r.asDict() for r in after_by_version.orderBy('source_snapshot').collect()]
    checks['user_product_totals_unchanged'] = True
    checks['unique_country_enrichment_keys'] = {}
    for name in ('country_buyers', 'country_sellers', 'country_statistics'):
        assert_country_detail(gold[name], name)
        assert_unique(gold[name], ['country'], name)
        checks['unique_country_enrichment_keys'][name] = gold[name].count()
    assert_country_detail(gold['seller_country_gender'], 'seller_country_gender')
    assert_unique(gold['seller_country_gender'], ['country', 'sex'], 'seller_country_gender')
    assert_unique(gold['seller_global_rollups'], ['metrics_source_snapshot', 'rollup_key'], 'seller_global_rollups')
    require(gold['seller_global_rollups'].filter(F.col('country').isNotNull()).count() == 0, 'named-country detail in global rollups')
    checks['no_global_rollups_in_detail_tables'] = True
    # Original source attributes must survive in the correctly separated tables.
    details, rollups = seller_parts(silver['sellers'])
    for name, expected in [('country_buyers', silver['buyers']), ('country_statistics', silver['countries']),
                           ('seller_country_gender', details), ('seller_global_rollups', rollups)]:
        same_rows(gold[name], expected, name)
    # Unpivot the stored wide table and verify each complete sex-specific record.
    metrics = seller_metrics(silver['sellers'])
    unpivoted = []
    for sex in ('FEMALE', 'MALE'):
        prefix = sex.lower() + '_'
        unpivoted.append(gold['country_sellers'].filter(F.col(prefix + 'nbsellers').isNotNull()).select(
            'country', F.lit(sex).alias('sex'), *(F.col(prefix + m).alias(m) for m in metrics)))
    same_rows(unpivoted[0].unionByName(unpivoted[1]), details.select('country', 'sex', *metrics), 'seller pivot round trip')
    expected_totals = details.groupBy('country').agg(*(F.sum(c).alias(alias) for c, alias in SELLER_TOTALS.items()))
    same_rows(gold['country_sellers'], expected_totals, 'country seller additive totals')
    for noun in ('sold', 'listed', 'bought', 'wished', 'liked'):
        expected_mean = F.round(F.col('total_products_' + noun) / F.col('seller_count_sales_or_listings'), 4).cast(T.DecimalType(18, 4))
        require(gold['country_sellers'].filter(~F.col('mean_products_' + noun + '_per_seller').eqNullSafe(expected_mean)).count() == 0,
                'seller mean must be recomputed from total and denominator: ' + noun)
    checks['seller_pivot_round_trip'] = True
    checks['seller_country_totals_and_means_reconciled'] = True
    # Confirm these historical summaries retain their observed source definitions.
    original = users.filter(F.col('source_snapshot') == HISTORICAL_SNAPSHOT)
    country_stats = original.groupBy('country').agg(
        F.sum((F.col('productsbought') > 0).cast('long')).alias('expected_buyers'),
        F.sum('productsbought').alias('expected_bought'),
        F.sum((F.col('productssold') > 0).cast('long')).alias('expected_sellers_sold'),
        F.sum('productssold').alias('expected_sold'))
    for table, columns in [('country_buyers', {'buyers': 'expected_buyers', 'totalproductsbought': 'expected_bought'}),
                           ('country_statistics', {'sellers': 'expected_sellers_sold', 'totalproductssold': 'expected_sold'})]:
        joined = gold[table].join(country_stats, ['country'], 'left')
        invalid = reduce(lambda left, right: left | right,
                         (~F.col(c).eqNullSafe(F.col(expected)) for c, expected in columns.items()))
        require(joined.filter(invalid).count() == 0, table + ' population no longer matches original source')
    seller_stats = (original.withColumn('sex', F.upper('gender')).groupBy('country', 'sex').agg(
        F.sum(((F.col('productssold') > 0) | (F.col('productslisted') > 0)).cast('long')).alias('expected_sellers'),
        F.sum('productssold').alias('expected_sold')))
    joined = gold['seller_country_gender'].join(seller_stats, ['country', 'sex'], 'left')
    require(joined.filter(~F.col('nbsellers').eqNullSafe(F.col('expected_sellers')) |
                         ~F.col('totalproductssold').eqNullSafe(F.col('expected_sold'))).count() == 0,
            'seller population must be original-source users with sales or listings')
    checks['historical_summary_population_reconciliation'] = True
    for name, df in gold.items():
        if name != 'ecom_one_big_table':
            require(df.filter(~F.col('metrics_source_snapshot').eqNullSafe(F.lit(HISTORICAL_SNAPSHOT))).count() == 0,
                    name + ' missing historical source label')
            require(df.filter(~F.col('population_definition').eqNullSafe(F.lit(CONTRACTS[name]['population']))).count() == 0,
                    name + ' population definition changed')
    for sex, predicate in [('FEMALE', F.col('sex') == 'FEMALE'), ('MALE', F.col('sex') == 'MALE'), ('ALL', F.lit(True))]:
        expected = details.filter(predicate).agg(*(F.sum(c).alias(c) for c in SELLER_TOTALS)).first().asDict()
        actual = gold['seller_global_rollups'].filter(F.col('rollup_key') == sex).select(*SELLER_TOTALS).first().asDict()
        require(actual == expected, 'global seller rollup does not reconcile: ' + sex)
    checks['global_rollups_reconciled_and_isolated'] = True
    contexts = country_contexts(gold)
    expected_context = users.select('country').distinct()
    for context in contexts:
        expected_context = expected_context.join(context, ['country'], 'left')
    context_columns = [c for c in obt.columns if c.startswith('Context_original_')]
    actual_context = obt.select(F.col('Country').alias('country'), *context_columns).distinct()
    same_rows(actual_context, expected_context, 'historical country context')
    require(all(obt.schema[c].metadata.get('aggregation_rule') == NON_ADDITIVE for c in context_columns),
            'context measures must carry non-additive schema metadata')
    require(obt.filter(~F.col('context_source_snapshot').eqNullSafe(F.lit(HISTORICAL_SNAPSHOT)) |
                       ~F.col('context_matches_user_snapshot').eqNullSafe(F.col('Users_source_snapshot') == HISTORICAL_SNAPSHOT)).count() == 0,
            'user/context source-version label mismatch')
    checks['historical_context_values_and_nonadditive_metadata'] = True
    checks['context_alignment_by_snapshot'] = [r.asDict() for r in obt.groupBy(
        'Users_source_snapshot', 'context_source_snapshot', 'context_matches_user_snapshot').count().orderBy('Users_source_snapshot').collect()]
    checks['gold_null_country_rows'] = obt.filter(F.col('Country').isNull()).count()
    return checks
