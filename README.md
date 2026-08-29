# FX Rates ELT — Apache Airflow

A small, production-shaped ELT pipeline: Airflow pulls daily foreign-exchange
reference rates from a public API, lands the raw payload in Postgres, models it
with SQL, runs data-quality assertions, and builds a day-over-day mart.

Built to be run locally with one command.

```
┌───────────────┐   ┌─────────────────┐   ┌────────────────────┐   ┌──────────────────────────────┐
│ Frankfurter   │──▶│ raw.fx_rates_raw│──▶│ analytics.fx_rates │──▶│ analytics.                   │
│ API (ECB)     │   │ (jsonb payload) │   │ (one row per pair) │   │ fx_rates_daily_change (mart) │
└───────────────┘   └─────────────────┘   └────────────────────┘   └──────────────────────────────┘
     extract             load_raw              transform                     build_mart
                                                    │
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
create_schema → extract → load_raw → transform → quality_checks → build_mart
```

| Task | What it does |
| --- | --- |
| `create_schema` | Applies `sql/ddl.sql`. Idempotent, runs on every execution. |
| `extract` | Calls the API for the run's logical date. Fails loudly if the response carries no rates. |
| `load_raw` | Upserts the untouched JSON into `raw.fx_rates_raw`, keyed on the logical date. |
| `transform` | Flattens the payload with `jsonb_each_text` into `analytics.fx_rates`. |
| `quality_checks` | Asserts row count, currency coverage, no nulls, no non-positive rates. Raises `AirflowFailException` on any breach. |
| `build_mart` | Rebuilds `analytics.fx_rates_daily_change` using `LAG()` over each currency pair. |

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
- Unit tests for the flattening logic against a recorded API fixture.

## Notes

The API returns ECB reference rates, which cover roughly 30 currencies. COP is
not among them, so the tracked pairs are USD against CAD, EUR, GBP, BRL and MXN.
