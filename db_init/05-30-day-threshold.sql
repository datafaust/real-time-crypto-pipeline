-- =============================================================================
-- Threshold Primer: minimal tables & views to support energy gating (z/percentile)
-- Requires existing:
--   - ohlcv_1m ("SYMBOL_VALUE" text, "WINDOW_START" bigint ms, "CLOSE"/"HIGH"/"LOW"/...)
--   - predictions_1m ("SYMBOL_VALUE" text, "WINDOW_START" timestamptz, "PREDICTED_VOLUME" double)
--   - minute_features_stage (created earlier; has symbol, ts(ms), prev levels, delta bps)
-- =============================================================================

-- 0) Helpful indexes (cheap, improves joins/lookups)
CREATE INDEX IF NOT EXISTS idx_pred_symbol_ts
  ON predictions_1m ("SYMBOL_VALUE","WINDOW_START");
CREATE INDEX IF NOT EXISTS idx_ohlcv_symbol_ts
  ON ohlcv_1m ("SYMBOL_VALUE","WINDOW_START");

-- 1) Recent 30d predictions (view)
CREATE OR REPLACE VIEW predictions_recent_30d AS
SELECT
  p."SYMBOL_VALUE" AS symbol,
  p."WINDOW_START" AS ts,             -- timestamptz (minute aligned)
  p."PREDICTED_VOLUME" AS predicted_volume
FROM predictions_1m p
WHERE p."WINDOW_START" >= NOW() - INTERVAL '30 days';

-- 2) Per-symbol 30d stats (materialized view; refresh when you want)
--    Mean/Std for z; plus a few fixed cutoffs so you can use percentiles without heavy windows.
CREATE MATERIALIZED VIEW IF NOT EXISTS pred_stats_30d_mv AS
SELECT
  symbol,
  COUNT(*)                               AS n_obs,
  AVG(predicted_volume)                  AS mean_pred,
  STDDEV_SAMP(predicted_volume)          AS std_pred,
  PERCENTILE_DISC(0.80) WITHIN GROUP (ORDER BY predicted_volume) AS p80,
  PERCENTILE_DISC(0.85) WITHIN GROUP (ORDER BY predicted_volume) AS p85,
  PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY predicted_volume) AS p90,
  PERCENTILE_DISC(0.95) WITHIN GROUP (ORDER BY predicted_volume) AS p95,
  MAX(ts)                                AS asof_ts
FROM predictions_recent_30d
GROUP BY symbol;

-- Make symbol lookups fast
CREATE INDEX IF NOT EXISTS idx_pred_stats_30d_mv_symbol
  ON pred_stats_30d_mv (symbol);

-- 3) Full percentile rank (dynamic, by symbol) for the last 30 days (view)
--    Use this when you want the exact percentile of a particular minute.
--    For a small symbol set this is fine; if it ever gets heavy, swap to a materialized view.
CREATE OR REPLACE VIEW predictions_recent_30d_ranked AS
WITH base AS (
  SELECT
    symbol,
    ts,
    predicted_volume,
    PERCENT_RANK() OVER (PARTITION BY symbol ORDER BY predicted_volume) AS pred_percentile_30d
  FROM predictions_recent_30d
)
SELECT * FROM base;

-- 4) Enriched “energy” view for each minute you already store
--    Joins:
--      - minute_features_stage (ms epoch)
--      - predictions (same minute, timestamptz)
--      - per-symbol stats for z-score
--      - percentile from ranked view
--    This gives you, for any minute, its z and percentile alongside your prev-bar features.
CREATE OR REPLACE VIEW minute_energy_enriched AS
SELECT
  m.symbol,
  m.ts,                                 -- bigint ms (minute start)
  m.open, m.high, m.low, m.close, m.vwap, m.trades,
  m.high_prev, m.low_prev, m.close_prev, m.price_1m_delta_bps,
  r.predicted_volume,
  s.mean_pred,
  s.std_pred,
  CASE
    WHEN s.std_pred IS NULL OR s.std_pred = 0 THEN NULL
    ELSE (r.predicted_volume - s.mean_pred) / s.std_pred
  END AS pred_z,
  -- percentile in [0,1]; multiply by 100 in the app if you want %
  rr.pred_percentile_30d
FROM minute_features_stage m
LEFT JOIN predictions_1m p
  ON p."SYMBOL_VALUE" = m.symbol
 AND p."WINDOW_START" = TO_TIMESTAMP(m.ts/1000.0)
LEFT JOIN predictions_recent_30d r
  ON r.symbol = p."SYMBOL_VALUE" AND r.ts = p."WINDOW_START"
LEFT JOIN pred_stats_30d_mv s
  ON s.symbol = m.symbol
LEFT JOIN predictions_recent_30d_ranked rr
  ON rr.symbol = r.symbol AND rr.ts = r.ts;

-- 5) Strategy config (store thresholds used live; one row per symbol)
CREATE TABLE IF NOT EXISTS strategy_config (
  symbol           TEXT PRIMARY KEY,
  z_threshold      DOUBLE PRECISION DEFAULT 1.0,
  percentile_min   DOUBLE PRECISION DEFAULT 90.0, -- use as percent (90 = top 10%)
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 6) Venue meta (fees & tick/lot); fill rows manually once per venue/symbol
CREATE TABLE IF NOT EXISTS venue_meta (
  symbol     TEXT PRIMARY KEY,
  maker_bps  DOUBLE PRECISION NOT NULL,  -- e.g. 1.0 = 1 bp = 0.01%
  taker_bps  DOUBLE PRECISION NOT NULL,
  tick_size  DOUBLE PRECISION NOT NULL,
  lot_size   DOUBLE PRECISION NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Convenience: a skinny latest snapshot for the UI (optional)
CREATE OR REPLACE VIEW minute_energy_latest AS
SELECT DISTINCT ON (m.symbol)
  m.symbol,
  m.ts,
  m.close,
  m.high_prev, m.low_prev, m.close_prev, m.price_1m_delta_bps,
  m.predicted_volume, m.pred_z, m.pred_percentile_30d
FROM minute_energy_enriched m
ORDER BY m.symbol, m.ts DESC;


-- for refresh materialized view
CREATE UNIQUE INDEX IF NOT EXISTS ux_pred_stats_30d_mv_symbol
  ON pred_stats_30d_mv (symbol);
