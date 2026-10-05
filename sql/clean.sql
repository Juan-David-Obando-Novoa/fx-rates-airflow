-- Cleaning layer (raw -> staging).
--
-- Parses this run's raw payload, standardizes it and gives every row a verdict.
-- A row that breaks a rule is kept in staging with the reason (quarantine)
-- instead of being dropped silently or crashing the whole run.
--
-- Rebuilt per logical date with delete + insert, so re-running is idempotent.

DELETE FROM staging.fx_rates_validated
WHERE logical_date = %(logical_date)s::date;

INSERT INTO staging.fx_rates_validated
    (logical_date, rate_date, base_currency, quote_currency, rate_raw, rate, rejection_reason)
WITH parsed AS (
    -- One row per currency in the payload. Everything is still text.
    SELECT
        r.logical_date,
        r.payload ->> 'date' AS rate_date_raw,
        r.payload ->> 'base' AS base_raw,
        q.key                AS quote_raw,
        q.value              AS rate_raw
    FROM raw.fx_rates_raw AS r,
         LATERAL jsonb_each_text(r.payload -> 'rates') AS q(key, value)
    WHERE r.logical_date = %(logical_date)s::date
),
standardized AS (
    -- Normalize formats and cast defensively: a value that cannot be cast
    -- becomes NULL and is caught by the rules below, instead of erroring.
    -- Dates need two checks: the regex enforces the YYYY-MM-DD shape and
    -- pg_input_is_valid that the day exists ('2026-02-31' has the shape).
    SELECT
        logical_date,
        quote_raw,
        CASE WHEN trim(rate_date_raw) ~ '^\d{4}-\d{2}-\d{2}$'
              AND pg_input_is_valid(trim(rate_date_raw), 'date')
             THEN trim(rate_date_raw)::date
        END                              AS rate_date,
        upper(trim(base_raw))            AS base_currency,
        upper(trim(quote_raw))           AS quote_currency,
        nullif(trim(rate_raw), '')       AS rate_raw,
        CASE WHEN trim(rate_raw) ~ '^-?\d+(\.\d+)?$'
             THEN trim(rate_raw)::numeric
        END                              AS rate
    FROM parsed
),
ranked AS (
    -- Duplicates inside one payload (e.g. 'GBP' and ' gbp ' after cleanup):
    -- keep one, preferring a parseable rate, and flag the rest.
    SELECT
        *,
        row_number() OVER (
            PARTITION BY quote_currency
            ORDER BY rate IS NULL, quote_raw
        ) AS occurrence
    FROM standardized
)
SELECT
    logical_date,
    rate_date,
    base_currency,
    quote_currency,
    rate_raw,
    rate,
    -- The first rule that fails wins. NULL means the row is clean.
    CASE
        WHEN quote_currency !~ '^[A-Z]{3}$'                     THEN 'invalid_currency_code'
        WHEN NOT quote_currency = ANY(%(expected_quotes)s)      THEN 'unexpected_currency'
        WHEN occurrence > 1                                     THEN 'duplicate_currency'
        WHEN base_currency IS DISTINCT FROM %(base_currency)s   THEN 'unexpected_base_currency'
        WHEN rate_date IS NULL                                  THEN 'invalid_rate_date'
        WHEN rate_date > logical_date                           THEN 'rate_date_in_future'
        WHEN rate_date < logical_date - %(max_rate_age_days)s   THEN 'stale_rate_date'
        WHEN rate_raw IS NULL                                   THEN 'missing_rate'
        WHEN rate IS NULL                                       THEN 'non_numeric_rate'
        WHEN rate <= 0                                          THEN 'non_positive_rate'
    END AS rejection_reason
FROM ranked;
