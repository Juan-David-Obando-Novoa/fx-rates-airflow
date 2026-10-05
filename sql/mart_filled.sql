-- Calendar-complete daily series: the table Power BI sits on.
--
-- The ECB does not publish on weekends or TARGET holidays, so analytics.fx_rates
-- has gaps. A chart over that table silently skips those days, and any average
-- by calendar day is biased toward business days. Here every calendar day gets
-- one row per pair, labelled so imputed values are never mistaken for real ones:
--   observed  the rate was published that day
--   imputed   carried forward from the last published rate (forward fill),
--             because a reference rate stays in force until the next one
--   missing   nothing published within max_rate_age_days, so nothing is invented
INSERT INTO analytics.fx_rates_daily_filled
    (calendar_date, base_currency, quote_currency, rate, source_rate_date, fill_status)
WITH calendar AS (
    SELECT generate_series(
               (SELECT min(rate_date)    FROM analytics.fx_rates),
               (SELECT max(logical_date) FROM raw.fx_rates_raw),
               interval '1 day'
           )::date AS calendar_date
),
pairs AS (
    SELECT DISTINCT base_currency, quote_currency
    FROM analytics.fx_rates
)
SELECT
    c.calendar_date,
    p.base_currency,
    p.quote_currency,
    last_obs.rate,
    last_obs.rate_date AS source_rate_date,
    CASE
        WHEN last_obs.rate_date IS NULL           THEN 'missing'
        WHEN last_obs.rate_date = c.calendar_date THEN 'observed'
        ELSE 'imputed'
    END AS fill_status
FROM calendar AS c
CROSS JOIN pairs AS p
LEFT JOIN LATERAL (
    -- Latest published rate on or before this day, within the age limit.
    SELECT f.rate, f.rate_date
    FROM analytics.fx_rates AS f
    WHERE f.base_currency  = p.base_currency
      AND f.quote_currency = p.quote_currency
      AND f.rate_date <= c.calendar_date
      AND f.rate_date >= c.calendar_date - %(max_rate_age_days)s
    ORDER BY f.rate_date DESC
    LIMIT 1
) AS last_obs ON true
ON CONFLICT (calendar_date, base_currency, quote_currency) DO UPDATE
    SET rate             = EXCLUDED.rate,
        source_rate_date = EXCLUDED.source_rate_date,
        fill_status      = EXCLUDED.fill_status;
