-- Promote clean rows (staging -> analytics). Only rows that passed every
-- cleaning rule reach the modelled layer.
--
-- Keyed on the date the API reports, not the Airflow logical date:
-- on weekends and holidays the API returns the previous business day, so
-- those runs collapse onto the row that already exists instead of duplicating it.
INSERT INTO analytics.fx_rates (rate_date, base_currency, quote_currency, rate)
SELECT rate_date, base_currency, quote_currency, rate
FROM staging.fx_rates_validated
WHERE logical_date = %(logical_date)s::date
  AND rejection_reason IS NULL
ON CONFLICT (rate_date, base_currency, quote_currency) DO UPDATE
    SET rate      = EXCLUDED.rate,
        loaded_at = now();
