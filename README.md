# dbt Analytics Engineering – NYC Taxi (DuckDB)

This repo is my analytics engineering playground based on the **NYC taxi trip data**. It uses **dbt Core** with the **DuckDB** adapter to build a small but realistic analytics stack:

- Local DuckDB warehouse  
- dbt project: `taxi_rides_ny`  
- Staging models for raw yellow & green trips  
- A fact model `fct_trips` with tests and basic data quality rules  

---

## Project structure

```
dbt-analytics-engineering/
  taxi_rides_ny/
    models/
      staging/
        source.yml          # raw taxi data sources (DuckDB tables)
        stg_green_tripdata.sql
        stg_yellow_tripdata.sql
      marts/
        core/
          fct_trips.sql     # main fact table (one row per trip)
          schema.yml        # tests for fct_trips (dbt_utils + dbt_expectations)
    seeds/
    macros/
    dbt_project.yml


staging/: light cleaning and renaming of raw tables into a consistent schema.

marts/core/fct_trips.sql: main fact table combining yellow & green trips into a single, analytics‑ready model.

marts/core/schema.yml: column‑level tests for fct_trips using dbt core tests, dbt_utils and dbt_expectations.

# Tech stack

dbt Core 1.11.x

DuckDB as the warehouse (local .duckdb file) via dbt-duckdb adapter

GitHub Codespaces + VS Code Desktop for development

dbt packages:

packages:
  - package: dbt-labs/dbt_utils
    version: 1.3.3

  - package: calogica/dbt_expectations
    version: ">=0.10.0"

  - package: dbt-labs/codegen
    version: ">=0.11.0"
dbt_utils – generic tests & macros (surrogate keys, expression tests, etc.).

dbt_expectations – Great Expectations‑style data quality tests.

codegen – helpers to generate model and source YAML files.

Models
Staging models
The models/staging layer:

Defines sources pointing to raw NYC taxi tables in DuckDB via source.yml.

Standardizes column names and types, for example:

vendor_id

rate_code_id

pickup_location_id, dropoff_location_id

pickup_datetime, dropoff_datetime

passenger_count, trip_distance

trip_type, fare_amount, total_amount, payment_type

These staging models are the only place where dbt touches the raw tables; marts build on top of them.

Fact model: fct_trips
The main fact table lives in models/marts/core/fct_trips.sql and is materialized as a view/table in the dev schema.

Key design choices

One row per trip
fct_trips is built from an intermediate union model (int_trips_unioned) that stacks green and yellow taxi trips into a single set. This ensures one row per trip regardless of taxi type.

Surrogate primary key trip_id
trip_id is a deterministic hash of a subset of business keys, implemented without external macros (so it works even without dbt_utils):

md5(
  concat(
    cast(vendor_id as string), ':',
    cast(pickup_datetime as string), ':',
    cast(dropoff_datetime as string), ':',
    cast(pickup_location_id as string), ':',
    cast(dropoff_location_id as string)
  )
) as trip_id
This creates a stable, reproducible primary key for each trip.

Deduplication logic
Full‑row duplicates (same vendor, locations, timestamps, amounts, etc.) are removed with a row_number() window:

row_number() over (
  partition by
    vendor_id,
    rate_code_id,
    pickup_location_id,
    dropoff_location_id,
    pickup_datetime,
    dropoff_datetime,
    store_and_fwd_flag,
    passenger_count,
    trip_distance,
    trip_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    ehail_fee,
    improvement_surcharge,
    total_amount,
    payment_type
  order by pickup_datetime
) as rn
and then keeping where rn = 1. This ensures one row per logical trip, even if the source data contains exact duplicates.

Payment type enrichment
The numeric payment_type codes (1–5) are mapped to human‑readable descriptions using the official NYC TLC data dictionary:

case cast(payment_type as int)
  when 1 then 'Credit card'
  when 2 then 'Cash'
  when 3 then 'No charge'
  when 4 then 'Dispute'
  when 5 then 'Unknown'
  else 'Other'
end as payment_type_description
This makes downstream analysis and BI more readable without losing the original numeric code.

 # Data quality tests
Data tests are defined in models/marts/core/schema.yml using:

dbt core built‑in tests (not_null, unique)

dbt_utils.expression_is_true

dbt_expectations tests

Example schema extract:

version: 2

models:
  - name: fct_trips
    description: "One row per taxi trip (yellow+green union)"
    columns:
      - name: trip_id
        description: "Surrogate primary key for trips"
        tests:
          - not_null
          # - unique  # can be heavy on DuckDB; optional if dedup logic is trusted

      - name: pickup_datetime
        tests:
          - not_null

      - name: dropoff_datetime
        tests:
          - not_null

      - name: total_amount
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: "total_amount >= 0"

      - name: fare_amount
        tests:
          - dbt_utils.expression_is_true:
              expression: "fare_amount >= 0"

      - name: payment_type
        tests:
          - dbt_expectations.expect_column_values_to_be_in_set:
              value_set:[1][2][3][4][5]

      - name: payment_type_description
        tests:
          - not_null
          - dbt_expectations.expect_column_values_to_be_in_set:
              value_set:
                - 'Credit card'
                - 'Cash'
                - 'No charge'
                - 'Dispute'
                - 'Unknown'

      - name: trip_distance
        tests:
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0
              max_value: 200
These tests cover:

Schema / PK integrity

trip_id is present and behaves as a primary key.

Required fields

pickup_datetime, dropoff_datetime, total_amount cannot be null.

Basic domain rules

total_amount and fare_amount must be non‑negative.

Categorical consistency

payment_type must be in [1–5], and payment_type_description must be one of the expected labels.

Sanity check on distances

trip_distance must be between 0 and 200 to catch obviously broken records / outliers.

How to run (local DuckDB)
From inside the taxi_rides_ny directory:

Check dbt + DuckDB connectivity:


dbt debug
Build all models:


dbt run
Or build only the main fact model:


dbt run -s fct_trips
Run tests for the fact model:


dbt test -s fct_trips
The DuckDB profile is defined in ~/.dbt/profiles.yml and points to a local DuckDB file and a dev schema where models like dev.fct_trips are created.

Next steps / ideas
Planned improvements for this project:

Add dimension models (e.g. dim_zones based on the NYC taxi zone lookup table) and join them into fct_trips.

Extend tests with more dbt_expectations checks:

Distribution/quantile checks for fares and tips

Null ratio thresholds for selected columns

Freshness tests for incremental data loads

Connect this dbt project to a BI tool (e.g. Metabase, Looker Studio, or Superset) and build dashboards on top of fct_trips.

References
DataTalksClub – Data Engineering Zoomcamp (Analytics Engineering module)

NYC Taxi & Limousine Commission – Trip record data dictionaries

dbt + DuckDB Quickstart and adapter docs

dbt packages:

dbt_utils

dbt_expectations

codegen
