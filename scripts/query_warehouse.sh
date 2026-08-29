#!/usr/bin/env bash
# Quick look at what the pipeline produced.
set -euo pipefail

docker compose exec -T warehouse-db psql -U warehouse -d warehouse <<'SQL'
\echo '--- rows per date ---'
SELECT rate_date, count(*) AS pairs
FROM analytics.fx_rates
GROUP BY rate_date
ORDER BY rate_date DESC
LIMIT 10;

\echo '--- latest rates ---'
SELECT * FROM analytics.fx_rates
ORDER BY rate_date DESC, quote_currency
LIMIT 10;

\echo '--- biggest daily moves ---'
SELECT rate_date, quote_currency, rate, prev_rate, pct_change
FROM analytics.fx_rates_daily_change
ORDER BY abs(pct_change) DESC NULLS LAST
LIMIT 10;
SQL
