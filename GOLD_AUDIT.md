# Gold layer audit — 17 October 2026

The 229,793 rows are exactly what the implemented joins produce. That establishes
SQL/cardinality correctness, but the result is unsuitable as a general-purpose
one-row-per-user analytics table. Seller groups multiply user measures, country
measures repeat across users, and seller rollups are mixed with detail records.
The existing pipeline, notebooks, and Delta tables were left unchanged.

## Intended and actual granularity

The original name `comprehensive_user_table` and its user fields suggest a user
enrichment table, but the notebooks declare no explicit business key. The
recommended user grain is `(source_snapshot, identifierHash)`, with `source_file`
as provenance. All 119,656 such keys are unique in Silver, including 98,913 in the
original source and 20,743 in the newer source. No identifiers overlap between
those two sources; that does not establish distinct real-world people across
datasets or a reliable longitudinal identity mapping.

The actual matched-row grain is **source user record x seller-sex group in that
country**. A user in a country with both seller groups appears once with FEMALE
seller statistics and once with MALE seller statistics, regardless of the user's
own gender. An additional three records are global seller rollups with no user
or country. Consequently the whole table has mixed granularity.

## Join cardinalities

| Stage | Rows | Right-side key behavior |
| --- | ---: | --- |
| Silver users | 119,656 | Unique user/source keys |
| Full outer join countries | 119,656 | 19 rows, unique country |
| Full outer join buyers | 119,656 | 62 rows, unique country |
| Full outer join sellers | 229,793 | 70 country/sex details + 3 global rollups |

There are 200 user countries. All named countries in each summary input have
users, so no named summary-only rows are added. Of the 47 named seller countries,
23 have two seller-sex groups and 24 have one. `(country, sex)` is unique among
the seller input rows, but `country` alone is not.

| Users' seller matches | Unique user records | Gold user rows |
| --- | ---: | ---: |
| Two groups | 110,134 | 220,268 |
| One group | 5,848 | 5,848 |
| No group (outer-join preservation) | 3,674 | 3,674 |
| User subtotal | 119,656 | 229,790 |
| Global seller rollups | — | 3 |
| Gold total | — | 229,793 |

Thus `229,793 = 119,656 + 110,134 + 3`. Every country's actual Gold count matches
its independently calculated join count; there are zero cardinality mismatches.
No source user is missing. There are 110,134 duplicate user-key groups, each with
two rows, but zero exact full-row duplicates and zero duplicate user/sex-group
keys. A whole-row `DISTINCT` would not fix the user multiplication.

Examples: France has 31,194 source user records and 62,388 Gold rows; the US has
23,257 and 46,514; Germany has 8,045 and 16,090.

## Aggregation consequences

The Gold function does not aggregate; it copies measures into the joined rows.
Their original scalar values are retained, but naive downstream sums are wrong.

| Measure | Sum at its source grain | Naive Gold sum |
| --- | ---: | ---: |
| User products sold | 56,299 | 108,833 |
| User products wished | 466,643 | 895,845 |
| User follower counts | 574,436 | 1,104,841 |
| Country buyer counts | 5,419 | 166,746,458 |
| Seller counts from countries summary (19 countries) | 1,892 | 70,173,213 |

For France, the source country buyer count is 1,251. Gold repeats it in each of
62,388 rows, so summing it gives 78,047,388. Even a corrected user-grain table
would repeat country measures across users; those measures would remain
non-additive at user grain. Averaging seller means across Gold rows similarly
weights countries by user population and join multiplicity.

## The three blank-country rows are rollups

Their six additive measures (`nbsellers`, sold, listed, bought, wished, liked)
reconcile exactly with the named-country detail:

| Rollup row | Seller count | Reconciliation |
| --- | ---: | --- |
| FEMALE | 2,127 | Sum of 46 named female groups |
| MALE | 592 | Sum of 24 named male groups |
| All sexes | 2,719 | Sum of all 70 named groups |

This is strong evidence of subtotal/grand-total records rather than missing
country attribution. Summing all 73 source seller rows already gives 8,157
sellers, triple the 2,719 detailed total, before any user join. Preserve these
records separately with an explicit aggregation level; exclude them from detail
joins. Bronze can continue to preserve the original raw records.

## Source-version and seller-definition differences

Independent CSV checks establish the following across every applicable country:

- Buyer counts equal the original user source's `productsBought > 0` count, and
  buyer total purchases equal that source's summed `productsBought`.
- Country-summary seller counts equal the original source's `productsSold > 0`
  count, and country products sold equal that source's sum.
- Seller-comparison counts equal the original source's
  `productsSold > 0 OR productsListed > 0` count by country/sex; its sold totals
  also reconcile with that original source.

The three summary sources therefore reconcile with the **original 98,913 user
records**, while Gold also includes 20,743 records from the newer source.
Original users have 12,027 products sold; newer users add 44,272. A single
current-population interpretation would be unsupported without source-version
and identity rules.

The two seller counts also have different definitions. France has 713 users
with sales, while the seller comparison has 966 users with sales or listings
(733 female + 233 male). This difference is explained by the source definitions;
the two counts should not be merged or renamed as an interchangeable measure.

## Recommended changes, pending implementation

1. Define user Gold as one row per `(source_snapshot, identifierHash)`. Keep the
   source file as lineage. Do not deduplicate across versions without an identity
   mapping and explicit business rule.
2. Separate seller detail (70 country/sex rows) from its three rollup records.
   Keep rollups in an explicitly labeled totals table, outside user enrichment.
3. For a user OBT, pivot seller groups into one row per country with separate
   female/male columns, then LEFT JOIN it and the unique country summaries to
   users. This should yield **119,656 rows** for the current inputs. An explicit
   demographic benchmark requirement could instead justify joining on country
   plus normalized user gender, since that seller key is unique.
4. Prefer separate Gold tables for user records, country measures (200-country
   dimension with sparse summaries), and seller country/sex measures (70 details).
   Give measures explicit source-version and population definitions. Keep older
   context separate from current-version facts unless comparability is verified.
5. Compute sums at their native grain. Recompute combined seller means from
   additive totals and correct denominators; avoid unweighted averages of means
   and avoid `SUM(DISTINCT value)`, which can collapse different countries sharing
   the same count. A user OBT's copied country counts remain contextual fields.
6. Add semantic checks: unique user keys, expected output user count, unique
   enrichment keys before joins, no rollups in detail outputs, preserved additive
   user totals, and documented source-version reconciliation. The existing
   cardinality tests validate the current join implementation; they do not
   establish the intended business grain.

## Audit evidence

The audit read actual Delta snapshots (Silver users/buyers/sellers/countries and
Gold, all version 0) and independently cross-checked raw CSV counts, measures,
and rollups. All 200 country cardinalities matched. Delta versions and hashes of
the working pipeline files/notebooks were unchanged after inspection.
Detailed machine-readable measurements, including every country, are in
`.runtime/gold_audit_report.json`. The temporary read-only audit program is
`.runtime/gold_audit.py`; neither is called by the working pipeline.
