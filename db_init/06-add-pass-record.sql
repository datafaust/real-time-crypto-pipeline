CREATE TABLE IF NOT EXISTS pass_events (
  symbol            TEXT    NOT NULL,
  ts                BIGINT  NOT NULL,  -- ms since epoch (minute start)
  side              TEXT    NOT NULL CHECK (side IN ('LONG','SHORT')),
  armed_px          DOUBLE PRECISION NOT NULL,
  -- snapshots
  close             DOUBLE PRECISION NOT NULL,
  high_prev         DOUBLE PRECISION,
  low_prev          DOUBLE PRECISION,
  pred_z            DOUBLE PRECISION,
  pred_percentile_30d DOUBLE PRECISION,
  close_minus_ema_bps DOUBLE PRECISION,
  atr14_1m_bps      DOUBLE PRECISION,
  z_threshold       DOUBLE PRECISION,
  percentile_min    DOUBLE PRECISION,
  reason            TEXT,   -- e.g. "energy+breakout+trend"
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (symbol, ts)
);

CREATE INDEX IF NOT EXISTS idx_pass_created_at ON pass_events (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_pass_symbol_ts ON pass_events (symbol, ts DESC);
