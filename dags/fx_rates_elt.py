"""
fx_rates_elt
============

Daily ELT pipeline that lands ECB foreign-exchange reference rates into a
Postgres warehouse, cleans and validates them, and builds two analytics marts.

Flow
----
    create_schema -> extract -> load_raw -> clean -> transform -> quality_checks
                                                                  -> build_mart
                                                                  -> build_filled_mart

Design notes
------------
* Every task is idempotent. Re-running any date overwrites that date's rows
  instead of duplicating them, so backfills and retries are safe.
* The raw JSON payload is stored untouched in `raw.fx_rates_raw` before any
  parsing happens. If the transform logic changes we replay from raw instead
  of re-hitting the API.
* Cleaning happens in its own layer (`staging.fx_rates_validated`): values are
  standardized, cast defensively and checked against validation rules. Rows
  that fail are quarantined with a reason instead of being dropped silently.
  Only clean rows are promoted to `analytics.fx_rates`.
* The API publishes on TARGET business days only. A weekend request returns
  the previous business day's rates, so the warehouse is keyed on the *date
  the API reports*, not on the Airflow logical date. Duplicates collapse via
  the primary key. The gaps are filled for BI in `analytics.fx_rates_daily_filled`
  with forward fill, and every imputed row is labelled as such.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from pathlib import Path

import requests
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

SQL_DIR = Path(__file__).resolve().parents[1] / "sql"
WAREHOUSE_CONN_ID = "warehouse"

API_BASE_URL = "https://api.frankfurter.dev/v1"
BASE_CURRENCY = "USD"
QUOTE_CURRENCIES = ["CAD", "EUR", "GBP", "BRL", "MXN"]

# How long a reference rate stays usable. Covers the longest TARGET closure
# (Good Friday to Easter Monday). Older rates are rejected as stale by `clean`
# and are never carried forward by `build_filled_mart`.
MAX_RATE_AGE_DAYS = 5

REQUEST_TIMEOUT_SECONDS = 30


def read_sql(filename: str) -> str:
    """Load a .sql file from the sibling sql/ directory."""
    return (SQL_DIR / filename).read_text(encoding="utf-8")


@dag(
    dag_id="fx_rates_elt",
    description="Daily FX reference rates: API -> raw -> clean -> analytics -> marts",
    schedule="0 6 * * *",
    start_date=datetime(2026, 8, 3),
    catchup=True,
    max_active_runs=1,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
        "retry_exponential_backoff": True,
    },
    tags=["elt", "fx", "postgres"],
    doc_md=__doc__,
)
def fx_rates_elt():

    @task
    def create_schema() -> None:
        """Create schemas and tables if they do not exist yet."""
        PostgresHook(postgres_conn_id=WAREHOUSE_CONN_ID).run(read_sql("ddl.sql"))

    @task
    def extract(target_date: str) -> dict:
        """Fetch the reference rates published for `target_date`."""
        url = f"{API_BASE_URL}/{target_date}"
        params = {"base": BASE_CURRENCY, "symbols": ",".join(QUOTE_CURRENCIES)}

        response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
        payload = response.json()

        if not payload.get("rates"):
            raise AirflowFailException(f"API returned no rates for {target_date}: {payload}")

        log.info("Requested %s, API reported rates for %s", target_date, payload["date"])
        return payload

    @task
    def load_raw(payload: dict, target_date: str) -> None:
        """Store the untouched JSON payload, keyed on the logical date."""
        import json

        PostgresHook(postgres_conn_id=WAREHOUSE_CONN_ID).run(
            read_sql("load_raw.sql"),
            parameters={"logical_date": target_date, "payload": json.dumps(payload)},
        )

    @task
    def clean(target_date: str) -> None:
        """Standardize, validate and quarantine the raw rows for `target_date`."""
        PostgresHook(postgres_conn_id=WAREHOUSE_CONN_ID).run(
            read_sql("clean.sql"),
            parameters={
                "logical_date": target_date,
                "base_currency": BASE_CURRENCY,
                "expected_quotes": QUOTE_CURRENCIES,
                "max_rate_age_days": MAX_RATE_AGE_DAYS,
            },
            # clean.sql is DELETE + INSERT. psycopg 3 sends parameterized SQL as
            # a prepared statement, which takes one command at a time. The hook
            # runs both on one connection and commits once, so it stays atomic.
            split_statements=True,
        )

    @task
    def transform(target_date: str) -> None:
        """Promote the rows that passed cleaning into analytics.fx_rates."""
        PostgresHook(postgres_conn_id=WAREHOUSE_CONN_ID).run(
            read_sql("transform.sql"),
            parameters={"logical_date": target_date},
        )

    @task
    def quality_checks(target_date: str) -> None:
        """Fail the run if a currency the business needs did not make it through."""
        hook = PostgresHook(postgres_conn_id=WAREHOUSE_CONN_ID)
        staged_rows, rejected_rows, covered_quotes, rejection_summary = hook.get_first(
            read_sql("quality_checks.sql"),
            parameters={"logical_date": target_date, "expected_quotes": QUOTE_CURRENCIES},
        )
        missing = sorted(set(QUOTE_CURRENCIES) - set(covered_quotes or []))

        failures = []
        if staged_rows == 0:
            failures.append("the payload produced no rows")
        if missing:
            failures.append(f"missing currencies: {', '.join(missing)}")

        if failures:
            detail = f" | quarantined: {rejection_summary}" if rejection_summary else ""
            raise AirflowFailException(
                f"Data quality checks failed for {target_date}: " + "; ".join(failures) + detail
            )

        if rejected_rows:
            # Not fatal: e.g. the API sent a currency nobody asked for.
            log.warning(
                "%s row(s) quarantined for %s: %s", rejected_rows, target_date, rejection_summary
            )

        log.info(
            "Quality checks passed for %s: %s currencies loaded, %s row(s) quarantined",
            target_date,
            len(QUOTE_CURRENCIES),
            rejected_rows,
        )

    @task
    def build_mart() -> None:
        """Rebuild the day-over-day change mart, flagging outlier moves."""
        PostgresHook(postgres_conn_id=WAREHOUSE_CONN_ID).run(read_sql("mart.sql"))

    @task
    def build_filled_mart() -> None:
        """Rebuild the calendar-complete series (forward fill, labelled)."""
        PostgresHook(postgres_conn_id=WAREHOUSE_CONN_ID).run(
            read_sql("mart_filled.sql"),
            parameters={"max_rate_age_days": MAX_RATE_AGE_DAYS},
        )

    # The logical date of each run, rendered by Airflow at execution time.
    target_date = "{{ ds }}"

    payload = extract(target_date)
    checks = quality_checks(target_date)

    (
        create_schema()
        >> payload
        >> load_raw(payload, target_date)
        >> clean(target_date)
        >> transform(target_date)
        >> checks
    )
    checks >> [build_mart(), build_filled_mart()]


fx_rates_elt()
