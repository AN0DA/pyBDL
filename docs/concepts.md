# Concepts & Terminology

This page explains BDL domain terms used throughout pyBDL. If you are
unfamiliar with Polish administrative geography or GUS statistics, start here.

## What is GUS / BDL?

**GUS** (Glowny Urzad Statystyczny) is Statistics Poland, the central
government statistics authority. **BDL** (Bank Danych Lokalnych - Local Data
Bank) is GUS's public API and web portal for regional and local statistical
indicators.

- BDL API: <https://api.stat.gov.pl/Home/BdlApi>
- BDL web portal: <https://bdl.stat.gov.pl/bdl/start>

## Administrative units and levels

Poland uses a three-tier administrative division (NUTS):

| Level | `unit_level` | Name (EN) | Name (PL) | Count |
| ----- | ------------ | --------- | --------- | ----- |
| 0 | 0 | Country | Kraj | 1 |
| NUTS-2 | 2 | Voivodeship | Wojewodztwo | 16 |
| NUTS-4 | 4 | County | Powiat | ~380 |
| LAU-1 | 5 | Municipality (gmina) type | Gmina (typ) | - |
| LAU-2 | 6 | Municipality | Gmina | ~2500 |

Use `unit_level` in `get_data_by_variable()` to restrict results to one tier.
Use `bdl.levels.list_levels()` to retrieve the full list with IDs.

## Variables

A **variable** (Polish: *wskaznik*) is a statistical indicator - a measurable
quantity such as "population", "unemployment rate", or "GDP per capita". Each
variable has a unique numeric ID (for example `"3643"`).

Use `bdl.variables.search_variables(name="...")` to find variable IDs by
keyword.

## Subjects

**Subjects** organize variables into a hierarchical category tree (for example
*Demographics > Population > Age structure*). Each subject has a string ID
starting with `"P"` (for example `"P0001"`). Use `bdl.subjects.list_subjects()`
to explore the tree.

## Attributes

**Attributes** describe dimensions or data-quality qualifiers for values. For
example, a variable may include attributes such as total/male/female or flags
for unavailable values. Each row in a data DataFrame includes an `attr_id`
column; use `enrich=["attributes"]` to add human-readable labels.

## Aggregates

**Aggregates** define aggregation variants for data values (for example total,
urban-only, rural-only where applicable). Use
`bdl.aggregates.list_aggregates()` to see available types.

## Measures

**Measures** describe units of measurement for a variable's values (for example
"thousands of persons", "percent", or currency-based units). Use
`bdl.measures.list_measures()` to see available measures.

## Localities

**Localities** (Polish: *miejscowosci statystyczne*) are statistical sub-units
below municipality level. Available via `bdl.units.list_localities()` and the
`get_data_by_variable_locality()` / `get_data_by_unit_locality()` methods.

## API key and registration

You can use pyBDL without an API key (anonymous access), but registered users
receive higher rate limits. Register at <https://api.stat.gov.pl/> to obtain
an `X-ClientId` API key. See [Configuration](config.md) for setup instructions
and [Rate limiting](rate_limiting.md) for quota differences.
