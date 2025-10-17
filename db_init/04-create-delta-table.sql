-- db_init/020_features.sql
-- Minute features from existing ohlcv_1m (no new Kafka topics)

CREATE OR REPLACE VIEW minute_features_stage AS
SELECT
    "SYMBOL_VALUE"                           AS symbol,
    "WINDOW_START"                           AS ts,           -- BIGINT ms
    "OPEN"                                   AS open,
    "HIGH"                                   AS high,
    "LOW"                                    AS low,
    "CLOSE"                                  AS close,
    "VWAP"                                   AS vwap,
    "TRADES"                                 AS trades,
    LAG("HIGH")  OVER w                      AS high_prev,
    LAG("LOW")   OVER w                      AS low_prev,
    LAG("CLOSE") OVER w                      AS close_prev,
    CASE
      WHEN LAG("CLOSE") OVER w IS NULL THEN NULL
      ELSE (("CLOSE" - LAG("CLOSE") OVER w) / LAG("CLOSE") OVER w) * 1e4
    END                                       AS price_1m_delta_bps
FROM public.ohlcv_1m
WINDOW w AS (PARTITION BY "SYMBOL_VALUE" ORDER BY "WINDOW_START");

-- Helpful index for fresh-minute lookups
CREATE INDEX IF NOT EXISTS idx_ohlcv_symbol_ts
  ON ohlcv_1m ("SYMBOL_VALUE","WINDOW_START");
