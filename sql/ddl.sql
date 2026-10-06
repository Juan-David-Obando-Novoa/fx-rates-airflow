-- Schemas and tables for the FX rates pipeline. Safe to run on every DAG run.
--
-- Layers (medallion):
--   raw        bronze  API response exactly as it arrived
--   staging    silver  every parsed row with its cleaning verdict (audit trail)
--   analytics  silver  clean, validated rates only
--   analytics  gold    marts (facts) and dimensions that a BI tool sits on

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;

-- Landing zone: the API response exactly as it arrived.
CREATE TABLE IF NOT EXISTS raw.fx_rates_raw (
    logical_date date        PRIMARY KEY,
    payload      jsonb       NOT NULL,
    ingested_at  timestamptz NOT NULL DEFAULT now()
);

-- Cleaning layer: one row per currency found in a payload, standardized, with
-- the verdict of the validation rules. A NULL rejection_reason means the row
-- passed every rule and was promoted to analytics.fx_rates. Rejected rows stay
-- here (quarantine) so nothing is dropped silently.
CREATE TABLE IF NOT EXISTS staging.fx_rates_validated (
    logical_date     date        NOT NULL,
    rate_date        date,
    base_currency    text,
    quote_currency   text,
    rate_raw         text,
    rate             numeric,
    rejection_reason text,
    validated_at     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS fx_rates_validated_logical_date_idx
    ON staging.fx_rates_validated (logical_date);

-- Modelled layer: one row per date / currency pair.
CREATE TABLE IF NOT EXISTS analytics.fx_rates (
    rate_date      date          NOT NULL,
    base_currency  text          NOT NULL,
    quote_currency text          NOT NULL,
    rate           numeric(18,6) NOT NULL CHECK (rate > 0),
    loaded_at      timestamptz   NOT NULL DEFAULT now(),
    PRIMARY KEY (rate_date, base_currency, quote_currency)
);

-- Mart: day-over-day movement with an outlier flag.
CREATE TABLE IF NOT EXISTS analytics.fx_rates_daily_change (
    rate_date      date          NOT NULL,
    base_currency  text          NOT NULL,
    quote_currency text          NOT NULL,
    rate           numeric(18,6) NOT NULL,
    prev_rate      numeric(18,6),
    pct_change     numeric(10,4),
    zscore         numeric(10,4),
    is_outlier     boolean,
    PRIMARY KEY (rate_date, base_currency, quote_currency)
);

-- Existing warehouses created before the outlier columns existed.
ALTER TABLE analytics.fx_rates_daily_change
    ADD COLUMN IF NOT EXISTS zscore     numeric(10,4),
    ADD COLUMN IF NOT EXISTS is_outlier boolean;

-- Mart: calendar-complete daily series (weekend and holiday gaps filled).
CREATE TABLE IF NOT EXISTS analytics.fx_rates_daily_filled (
    calendar_date    date          NOT NULL,
    base_currency    text          NOT NULL,
    quote_currency   text          NOT NULL,
    rate             numeric(18,6),
    source_rate_date date,
    fill_status      text          NOT NULL
                     CHECK (fill_status IN ('observed', 'imputed', 'missing')),
    PRIMARY KEY (calendar_date, base_currency, quote_currency)
);

-- Dimension: one row per tracked quote currency (reference data).
CREATE TABLE IF NOT EXISTS analytics.dim_currency (
    currency_code text PRIMARY KEY,
    currency_name text NOT NULL,
    region        text NOT NULL
);

-- Dimension: one row per calendar day covered by the marts.
CREATE TABLE IF NOT EXISTS analytics.dim_date (
    date_day        date    PRIMARY KEY,
    year            int     NOT NULL,
    quarter         int     NOT NULL,
    month           int     NOT NULL,
    month_label     text    NOT NULL,   -- 'YYYY-MM', sorts correctly as text
    iso_week        int     NOT NULL,
    iso_day_of_week int     NOT NULL,   -- 1 = Monday ... 7 = Sunday
    day_name        text    NOT NULL,
    is_weekend      boolean NOT NULL
);
