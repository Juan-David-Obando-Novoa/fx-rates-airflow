# FX Rates ELT — Apache Airflow

A small, production-shaped ELT pipeline: Airflow pulls daily foreign-exchange
reference rates from a public API, lands the raw payload in Postgres, cleans and
validates it in a dedicated layer, runs data-quality assertions, and builds two
analytics marts ready for a BI tool.

Built to be run locally with one command.

```
┌──────────────┐   ┌─────────────────┐   ┌──────────────────────────┐   ┌────────────────────┐   ┌─────────────────────────────────┐
│ Frankfurter  │──▶│ raw.fx_rates_raw│──▶│ staging.                 │──▶│ analytics.fx_rates │──▶│ analytics.fx_rates_daily_change │
│ API (ECB)    │   │ (jsonb payload) │   │ fx_rates_validated       │   │ (clean rows only)  │   │ analytics.fx_rates_daily_filled │
└──────────────┘   └─────────────────┘   │ (verdict per row)        │   └────────────────────┘   └─────────────────────────────────┘
    extract            load_raw          └──────────────────────────┘        transform                   build_mart
                                                   clean                          │                     build_filled_mart
                                                                           quality_checks
```

## Stack

| Piece | Choice |
| --- | --- |
| Orchestration | Apache Airflow 3.3 (TaskFlow API, LocalExecutor) |
| Warehouse | PostgreSQL 16 |
| Transformations | SQL, versioned as files under `sql/` |
| Runtime | Docker Compose |
| Source | [Frankfurter](https://frankfurter.dev) — ECB reference rates, no API key |

## Running it

```bash
docker compose up --build
```

Airflow comes up at <http://localhost:8080>. The `standalone` command prints
the generated admin password to the logs on first boot — search for
`Password for user 'admin'`.

Enable the `fx_rates_elt` DAG in the UI. Because `catchup=True` and the DAG
starts on 2026-08-03, Airflow immediately backfills every missed day, one run
at a time (`max_active_runs=1`).

Inspect the result:

```bash
./scripts/query_warehouse.sh
```

Or connect directly — the warehouse is published on port 5433:

```bash
psql -h localhost -p 5433 -U warehouse -d warehouse   # password: warehouse
```

Tear everything down, volumes included:

```bash
docker compose down -v
```

## The DAG

```
create_schema → extract → load_raw → clean → transform → quality_checks ─┬→ build_mart
                                                                         └→ build_filled_mart
```

| Task | What it does |
| --- | --- |
| `create_schema` | Applies `sql/ddl.sql`. Idempotent, runs on every execution. |
| `extract` | Calls the API for the run's logical date. Fails loudly if the response carries no rates. |
| `load_raw` | Upserts the untouched JSON into `raw.fx_rates_raw`, keyed on the logical date. |
| `clean` | Flattens the payload, standardizes it, casts defensively and applies the validation rules. Every row lands in `staging.fx_rates_validated` with a `rejection_reason` (NULL = clean). |
| `transform` | Promotes only the clean rows into `analytics.fx_rates`. |
| `quality_checks` | Fails the run if any expected currency is missing after cleaning, and names the quarantined rows and why. Quarantined rows that do not break coverage (e.g. a currency nobody asked for) only log a warning. |
| `build_mart` | Rebuilds `analytics.fx_rates_daily_change`: day-over-day change with `LAG()` plus a rolling z-score outlier flag. |
| `build_filled_mart` | Rebuilds `analytics.fx_rates_daily_filled`: one row per calendar day per pair, weekend and holiday gaps forward-filled and labelled. |

## Data cleaning

The source is an official central bank feed, so it is usually clean. The
pipeline is built not to trust that.

**Standardize.** Currency codes are trimmed and upper-cased, rates are trimmed,
and casts are defensive: a value that cannot become a date or a number turns
into NULL and is caught by a rule, instead of crashing the run.

**Validate, then quarantine.** Each row gets the first rule it breaks:

| Rule | Catches |
| --- | --- |
| `invalid_currency_code` | Anything that is not a 3-letter ISO code |
| `unexpected_currency` | A currency the pipeline did not request |
| `duplicate_currency` | The same currency twice in one payload (keeps the parseable one) |
| `unexpected_base_currency` | A payload not quoted against USD |
| `invalid_rate_date` | A missing or malformed date |
| `rate_date_in_future` | A rate dated after the day it was requested for |
| `stale_rate_date` | A rate older than `MAX_RATE_AGE_DAYS` (5) |
| `missing_rate` / `non_numeric_rate` / `non_positive_rate` | Empty, unparseable, zero or negative values |

Rejected rows stay in `staging.fx_rates_validated` with their reason, so every
dropped value is auditable. Only clean rows reach `analytics.fx_rates`.

**Fill the calendar gaps, and say so.** The ECB does not publish on weekends or
TARGET holidays. `analytics.fx_rates_daily_filled` gives every calendar day a
row, carrying the last published rate forward (a reference rate stays in force
until the next one). Each row is labelled `observed`, `imputed` or `missing`,
and keeps `source_rate_date`, so an imputed value is never mistaken for a real
one. Rates are not carried forward more than 5 days: past that the row is
`missing` rather than invented.

**Flag outliers, do not delete them.** Each day's move is scored against the
previous 20 moves of the same pair (z-score, current day excluded from its own
baseline so a spike cannot hide itself). `|z| > 4` sets `is_outlier`. The row
stays: in FX a big move can be real news, and that is a call for a human.

## Design decisions

**Everything is idempotent.** Every write is an `INSERT … ON CONFLICT DO
UPDATE` against a real primary key. Re-running a date — a retry, a manual
clear, a backfill — overwrites that date instead of duplicating it. This is the
property that makes a scheduler safe to leave alone.

**Raw is stored before it is parsed.** `raw.fx_rates_raw` keeps the API
response verbatim. When the transform logic changes, the fix is a replay from
raw rather than a re-fetch from a third party that may rate-limit, change its
contract, or revise history.

**The warehouse is keyed on the source's date, not Airflow's.** The ECB
publishes on TARGET business days only, so a Saturday request returns Friday's
rates. Keying `analytics.fx_rates` on the date the API reports means weekend
runs collapse onto the business day they actually describe, instead of
inventing a Saturday observation.

**Retries assume transient failure.** Three attempts with exponential backoff,
which covers the usual network blips without hammering the source. Anything
that survives that is a real problem and should page a human.

**Quality checks fail the run.** A pipeline that silently loads three rows
instead of five is worse than one that stops, because the dashboard downstream
still renders and nobody notices. The checks are cheap and they are blocking.

**Separate metadata and warehouse databases.** Airflow's own state and the
analytical data live in different Postgres instances, as they would in any real
deployment.

## What I would add next

- Move the transformations to dbt so the models get lineage, tests and docs
  instead of hand-rolled `ON CONFLICT` statements.
- Swap Postgres for BigQuery and the Python extract for a `GCSToBigQueryOperator`,
  so the raw landing zone is object storage.
- An `SLA` and an alerting callback on the quality-check task.
- Unit tests for the cleaning rules against a recorded API fixture.
- A robust outlier score (median and MAD instead of mean and standard deviation),
  so one extreme day weighs less on the baseline of the following days.

## Power BI

Point Power BI at the warehouse (`localhost:5433`, database `warehouse`) and use:

- `analytics.fx_rates_daily_filled` for time series (filter or colour by `fill_status`)
- `analytics.fx_rates_daily_change` for moves and the `is_outlier` flag
- `staging.fx_rates_validated` for a data-quality view of quarantined rows

## Notes

The API returns ECB reference rates, which cover roughly 30 currencies. COP is
not among them, so the tracked pairs are USD against CAD, EUR, GBP, BRL and MXN.
