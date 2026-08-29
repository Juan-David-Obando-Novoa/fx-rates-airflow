-- Idempotent landing. Re-running a date replaces its payload.
INSERT INTO raw.fx_rates_raw (logical_date, payload, ingested_at)
VALUES (%(logical_date)s::date, %(payload)s::jsonb, now())
ON CONFLICT (logical_date) DO UPDATE
    SET payload     = EXCLUDED.payload,
        ingested_at = EXCLUDED.ingested_at;
