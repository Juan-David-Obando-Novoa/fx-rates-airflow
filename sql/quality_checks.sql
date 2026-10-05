-- One row of assertions for the date this run just loaded.
WITH staged AS (
    SELECT *
    FROM staging.fx_rates_validated
    WHERE logical_date = %(logical_date)s::date
),
loaded AS (
    -- What actually landed in the warehouse for the date(s) this run cleaned.
    SELECT f.quote_currency
    FROM analytics.fx_rates AS f
    WHERE f.rate_date IN (SELECT rate_date FROM staged WHERE rejection_reason IS NULL)
      AND f.quote_currency = ANY(%(expected_quotes)s)
)
SELECT
    (SELECT count(*) FROM staged)                                   AS staged_rows,
    (SELECT count(*) FROM staged WHERE rejection_reason IS NOT NULL) AS rejected_rows,
    (SELECT array_agg(DISTINCT quote_currency) FROM loaded)          AS covered_quotes,
    (SELECT string_agg(coalesce(quote_currency, '?') || ' ' || rejection_reason, ', '
                       ORDER BY quote_currency)
     FROM staged
     WHERE rejection_reason IS NOT NULL)                             AS rejection_summary;
