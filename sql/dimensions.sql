-- Conformed dimensions for the BI layer (gold).
--
-- Built in the warehouse rather than in the BI tool, so every consumer
-- (Power BI, a notebook, an ad hoc query) slices by the same attributes.
-- Both tables are shared by all the marts: one currency list, one calendar.

-- Currency: reference data. A new tracked currency needs a row here as well
-- as an entry in QUOTE_CURRENCIES in the DAG.
INSERT INTO analytics.dim_currency (currency_code, currency_name, region)
VALUES
    ('BRL', 'Brazilian real',  'Latin America'),
    ('CAD', 'Canadian dollar', 'North America'),
    ('EUR', 'Euro',            'Europe'),
    ('GBP', 'Pound sterling',  'Europe'),
    ('MXN', 'Mexican peso',    'Latin America')
ON CONFLICT (currency_code) DO UPDATE
    SET currency_name = EXCLUDED.currency_name,
        region        = EXCLUDED.region;

-- Date: every calendar day from the first published rate to the latest run,
-- the same span as analytics.fx_rates_daily_filled. Grows with each run.
INSERT INTO analytics.dim_date
    (date_day, year, quarter, month, month_label, iso_week, iso_day_of_week, day_name, is_weekend)
SELECT
    d::date,
    extract(year    FROM d)::int,
    extract(quarter FROM d)::int,
    extract(month   FROM d)::int,
    to_char(d, 'YYYY-MM'),
    extract(week    FROM d)::int,
    extract(isodow  FROM d)::int,
    to_char(d, 'Dy'),
    extract(isodow  FROM d) >= 6
FROM generate_series(
         (SELECT min(rate_date)    FROM analytics.fx_rates),
         (SELECT max(logical_date) FROM raw.fx_rates_raw),
         interval '1 day'
     ) AS d
ON CONFLICT (date_day) DO NOTHING;
