-- Flatten the JSON payload into one row per currency pair.
-- Keyed on the date the API reports, not the Airflow logical date:
-- on weekends and holidays the API returns the previous business day.
INSERT INTO analytics.fx_rates (rate_date, base_currency, quote_currency, rate)
SELECT
    (r.payload ->> 'date')::date AS rate_date,
    r.payload ->> 'base'         AS base_currency,
    q.key                        AS quote_currency,
    q.value::numeric             AS rate
FROM raw.fx_rates_raw AS r,
     LATERAL jsonb_each_text(r.payload -> 'rates') AS q(key, value)
WHERE r.logical_date = %(logical_date)s::date
ON CONFLICT (rate_date, base_currency, quote_currency) DO UPDATE
    SET rate      = EXCLUDED.rate,
        loaded_at = now();
