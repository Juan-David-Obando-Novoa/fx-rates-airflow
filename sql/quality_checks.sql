-- One row of assertions for the date this run just loaded.
WITH target AS (
    SELECT (payload ->> 'date')::date AS rate_date
    FROM raw.fx_rates_raw
    WHERE logical_date = %(logical_date)s::date
)
SELECT
    count(*)                                     AS row_count,
    count(*) FILTER (WHERE f.rate IS NULL)       AS null_rates,
    count(*) FILTER (WHERE f.rate <= 0)          AS non_positive_rates,
    count(DISTINCT f.quote_currency)             AS distinct_quotes
FROM analytics.fx_rates AS f
JOIN target AS t ON f.rate_date = t.rate_date;
