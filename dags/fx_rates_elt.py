"""
fx_rates_elt
============

Daily ELT pipeline that lands ECB foreign-exchange reference rates into a
Postgres warehouse and builds a small analytics mart on top of them.

Flow
----
    create_schema -> extract -> load_raw -> transform -> quality_checks -> build_mart

Design notes
------------
* Every task is idempotent. Re-running any date overwrites that date's rows
  instead of duplicating them, so backfills and retries are safe.
* The raw JSON payload is stored untouched in `raw.fx_rates_raw` before any
  parsing happens. If the transform logic changes we replay from raw instead
  of re-hitting the API.
* The API publishes on TARGET business days only. A weekend request returns
  the previous business day's rates, so the warehouse is keyed on the *date
  the API reports*, not on the Airflow logical date. Duplicates collapse via
  the primary key.
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

REQUEST_TIMEOUT_SECONDS = 30
MIN_EXPECTED_ROWS = len(QUOTE_CURRENCIES)


def read_sql(filename: str) -> str:
    """Load a .sql file from the sibling sql/ directory."""
    return (SQL_DIR / filename).read_text(encoding="utf-8")


@dag(
    dag_id="fx_rates_elt",
    description="Daily FX reference rates: API -> raw -> analytics -> mart",
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
    def transform(target_date: str) -> None:
        """Flatten the JSON payload into the analytics.fx_rates table."""
        PostgresHook(postgres_conn_id=WAREHOUSE_CONN_ID).run(
            read_sql("transform.sql"),
            parameters={"logical_date": target_date},
        )

    @task
    def quality_checks(target_date: str) -> None:
        """Fail the run if the loaded data does not meet expectations."""
        hook = PostgresHook(postgres_conn_id=WAREHOUSE_CONN_ID)
        row = hook.get_first(
            read_sql("quality_checks.sql"),
            parameters={"logical_date": target_date},
        )
        row_count, null_rates, non_positive, distinct_quotes = row

        failures = []
        if row_count < MIN_EXPECTED_ROWS:
            failures.append(f"expected at least {MIN_EXPECTED_ROWS} rows, found {row_count}")
        if null_rates:
            failures.append(f"{null_rates} null rate(s)")
        if non_positive:
            failures.append(f"{non_positive} non-positive rate(s)")
        if distinct_quotes < len(QUOTE_CURRENCIES):
            failures.append(
                f"expected {len(QUOTE_CURRENCIES)} currencies, found {distinct_quotes}"
            )

        if failures:
            raise AirflowFailException(
                f"Data quality checks failed for {target_date}: " + "; ".join(failures)
            )

        log.info(
            "Quality checks passed for %s: %s rows, %s currencies",
            target_date,
            row_count,
            distinct_quotes,
        )

    @task
    def build_mart() -> None:
        """Rebuild the day-over-day change mart with a window function."""
        PostgresHook(postgres_conn_id=WAREHOUSE_CONN_ID).run(read_sql("mart.sql"))

    # The logical date of each run, rendered by Airflow at execution time.
    target_date = "{{ ds }}"

    payload = extract(target_date)

    (
        create_schema()
        >> payload
        >> load_raw(payload, target_date)
        >> transform(target_date)
        >> quality_checks(target_date)
        >> build_mart()
    )


fx_rates_elt()
