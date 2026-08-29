-- Day-over-day movement per currency pair, using a window function.
INSERT INTO analytics.fx_rates_daily_change
    (rate_date, base_currency, quote_currency, rate, prev_rate, pct_change)
SELECT
    rate_date,
    base_currency,
    quote_currency,
    rate,
    prev_rate,
    round(((rate - prev_rate) / prev_rate) * 100, 4) AS pct_change
FROM (
    SELECT
        rate_date,
        base_currency,
        quote_currency,
        rate,
        lag(rate) OVER (
            PARTITION BY base_currency, quote_currency
            ORDER BY rate_date
        ) AS prev_rate
    FROM analytics.fx_rates
) AS windowed
WHERE prev_rate IS NOT NULL
ON CONFLICT (rate_date, base_currency, quote_currency) DO UPDATE
    SET rate       = EXCLUDED.rate,
        prev_rate  = EXCLUDED.prev_rate,
        pct_change = EXCLUDED.pct_change;
