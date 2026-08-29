-- Schemas and tables for the FX rates pipeline. Safe to run on every DAG run.

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS analytics;

-- Landing zone: the API response exactly as it arrived.
CREATE TABLE IF NOT EXISTS raw.fx_rates_raw (
    logical_date date        PRIMARY KEY,
    payload      jsonb       NOT NULL,
    ingested_at  timestamptz NOT NULL DEFAULT now()
);

-- Modelled layer: one row per date / currency pair.
CREATE TABLE IF NOT EXISTS analytics.fx_rates (
    rate_date      date          NOT NULL,
    base_currency  text          NOT NULL,
    quote_currency text          NOT NULL,
    rate           numeric(18,6) NOT NULL CHECK (rate > 0),
    loaded_at      timestamptz   NOT NULL DEFAULT now(),
    PRIMARY KEY (rate_date, base_currency, quote_currency)
);

-- Mart: day-over-day movement, the table a BI tool would sit on.
CREATE TABLE IF NOT EXISTS analytics.fx_rates_daily_change (
    rate_date      date          NOT NULL,
    base_currency  text          NOT NULL,
    quote_currency text          NOT NULL,
    rate           numeric(18,6) NOT NULL,
    prev_rate      numeric(18,6),
    pct_change     numeric(10,4),
    PRIMARY KEY (rate_date, base_currency, quote_currency)
);
