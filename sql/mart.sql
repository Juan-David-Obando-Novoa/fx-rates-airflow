-- Day-over-day movement per currency pair, with an outlier flag.
--
-- pct_change compares each business day with the previous one (LAG).
-- The outlier test is a z-score of the day's move against the previous 20
-- moves of the same pair. The current row is left out of its own baseline,
-- otherwise a big spike would inflate the standard deviation and hide itself.
-- It runs on observed business days only: imputed weekend days would add
-- zero-change rows and shrink the standard deviation artificially.
-- Outliers are flagged, never deleted: in FX a big move can be real news.
INSERT INTO analytics.fx_rates_daily_change
    (rate_date, base_currency, quote_currency, rate, prev_rate, pct_change, zscore, is_outlier)
WITH with_prev AS (
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
),
with_change AS (
    SELECT
        *,
        round(((rate - prev_rate) / prev_rate) * 100, 4) AS pct_change
    FROM with_prev
    WHERE prev_rate IS NOT NULL
),
with_baseline AS (
    SELECT
        *,
        avg(pct_change)         OVER prior_moves AS baseline_mean,
        stddev_samp(pct_change) OVER prior_moves AS baseline_std,
        count(pct_change)       OVER prior_moves AS baseline_n
    FROM with_change
    WINDOW prior_moves AS (
        PARTITION BY base_currency, quote_currency
        ORDER BY rate_date
        ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
    )
),
scored AS (
    SELECT
        *,
        -- Needs at least 10 prior moves to be meaningful; NULL until then.
        CASE WHEN baseline_n >= 10 AND baseline_std > 0
             THEN round((pct_change - baseline_mean) / baseline_std, 4)
        END AS zscore
    FROM with_baseline
)
SELECT
    rate_date,
    base_currency,
    quote_currency,
    rate,
    prev_rate,
    pct_change,
    zscore,
    abs(zscore) > 4 AS is_outlier   -- NULL means not enough history to judge
FROM scored
ON CONFLICT (rate_date, base_currency, quote_currency) DO UPDATE
    SET rate       = EXCLUDED.rate,
        prev_rate  = EXCLUDED.prev_rate,
        pct_change = EXCLUDED.pct_change,
        zscore     = EXCLUDED.zscore,
        is_outlier = EXCLUDED.is_outlier;
