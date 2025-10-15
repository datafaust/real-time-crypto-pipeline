-- =============================================================================
-- ksqlDB: Real-time OHLCV with Late-Data Tolerance (Grace Period)
-- =============================================================================

SET 'auto.offset.reset' = 'earliest';

-- 1) Source stream over RAW_TICKS (JSON), using event-time from TRADE_TIME
CREATE STREAM IF NOT EXISTS RAW_TICKS_SRC (
  SOURCE         STRING,
  EVENT_TYPE     STRING,
  EVENT_TIME     BIGINT,
  SYMBOL         STRING,
  AGG_ID         BIGINT,
  PRICE          DOUBLE,
  QTY            DOUBLE,
  TRADE_TIME     BIGINT,   -- event time in ms from Binance
  IS_BUYER_MAKER BOOLEAN,
  INGEST_TS      BIGINT
) WITH (
  KAFKA_TOPIC = 'raw_ticks',
  VALUE_FORMAT = 'JSON',
  KEY_FORMAT   = 'KAFKA',
  TIMESTAMP    = 'TRADE_TIME'  -- explicit identifier casing
);

-- 2) Cleaned stream, re-keyed by SYMBOL, and explicitly set TIMESTAMP column.
CREATE OR REPLACE STREAM CLEAN_TICKS
WITH (
  KAFKA_TOPIC  = 'clean_ticks',
  VALUE_FORMAT = 'JSON',
  TIMESTAMP    = 'TS_MS'
) AS
SELECT
  SYMBOL,
  CAST(PRICE AS DOUBLE) AS PRICE,
  CAST(QTY   AS DOUBLE) AS QTY,
  TRADE_TIME            AS TS_MS
FROM RAW_TICKS_SRC
PARTITION BY SYMBOL
EMIT CHANGES;

-- 3) 1-min OHLCV TABLE (windowed) with GRACE PERIOD for late data tolerance.
CREATE OR REPLACE TABLE OHLCV_1M
WITH (
  KAFKA_TOPIC  = 'ohlcv_1m',
  VALUE_FORMAT = 'JSON',
  PARTITIONS   = 1,
  REPLICAS     = 1,
  RETENTION_MS = 604800000   -- 7 days
) AS
SELECT
  SYMBOL,
  WINDOWSTART AS WINDOW_START,
  WINDOWEND   AS WINDOW_END,
  EARLIEST_BY_OFFSET(PRICE) AS OPEN,
  MAX(PRICE)                AS HIGH,
  MIN(PRICE)                AS LOW,
  LATEST_BY_OFFSET(PRICE)   AS CLOSE,
  SUM(QTY)                  AS VOLUME,
  SUM(PRICE * QTY) / NULLIF(SUM(QTY), CAST(0.0 AS DOUBLE)) AS VWAP,
  COUNT(*)                  AS TRADES
FROM CLEAN_TICKS
WINDOW TUMBLING (SIZE 1 MINUTE, GRACE PERIOD 30 SECONDS)
GROUP BY SYMBOL
EMIT CHANGES;

-- 4) Changelog STREAM over the table’s topic (declare windowed key!)
CREATE STREAM IF NOT EXISTS OHLCV_1M_CHANGELOG (
  SYMBOL        STRING KEY,
  WINDOW_START  BIGINT,
  WINDOW_END    BIGINT,
  OPEN          DOUBLE,
  HIGH          DOUBLE,
  LOW           DOUBLE,
  CLOSE         DOUBLE,
  VOLUME        DOUBLE,
  VWAP          DOUBLE,
  TRADES        BIGINT
) WITH (
  KAFKA_TOPIC  = 'ohlcv_1m',
  KEY_FORMAT   = 'KAFKA',
  VALUE_FORMAT = 'JSON',
  WINDOW_TYPE  = 'TUMBLING',
  WINDOW_SIZE  = '1 MINUTE'   -- singular literal
);

-- 5) AVRO sink stream for JDBC (preserve key + copy key into value)
CREATE OR REPLACE STREAM OHLCV_1M_OUT_AVRO
WITH (
  KAFKA_TOPIC  = 'ohlcv_1m_out_avro',
  VALUE_FORMAT = 'AVRO',
  PARTITIONS   = 1
) AS
SELECT
  SYMBOL,                            -- keep the KEY column in the projection
  AS_VALUE(SYMBOL) AS SYMBOL_VALUE,  -- copy key into the VALUE for JDBC PK
  WINDOW_START,
  WINDOW_END,
  OPEN,
  HIGH,
  LOW,
  CLOSE,
  VOLUME,
  VWAP,
  TRADES
FROM OHLCV_1M_CHANGELOG
EMIT CHANGES;


-- -- =============================================================================
-- -- ksqlDB: Real-time OHLCV with Late-Data Tolerance (Grace Period)
-- -- -----------------------------------------------------------------------------
-- -- WHY: In streaming systems, some events arrive late (network jitter, broker
-- -- retries, out-of-order delivery). Without tolerance, late ticks for a past
-- -- minute would be dropped and your candle would be incomplete.
-- --
-- -- WHAT: Adding a GRACE PERIOD keeps each 1-minute window “re-openable” for a
-- -- short time (e.g., 30s). If late ticks arrive within that grace, ksqlDB will
-- -- UPDATE the same window/candle (and your JDBC sink will UPSERT the same row).
-- --
-- -- EFFECT:
-- --   - EMIT CHANGES: you'll see multiple updates to the same candle until it
-- --     finalizes after window_end + grace (more write churn, live accuracy).
-- --   - (Optional) EMIT FINAL: emit only once per candle after grace closes
-- --     (less write churn, finalized values only).
-- -- =============================================================================

-- -- Read earliest on first run
-- SET 'auto.offset.reset' = 'earliest';

-- -- 1) Source stream over RAW_TICKS (JSON), using event-time from TRADE_TIME
-- CREATE STREAM IF NOT EXISTS RAW_TICKS_SRC (
--   SOURCE         STRING,
--   EVENT_TYPE     STRING,
--   EVENT_TIME     BIGINT,
--   SYMBOL         STRING,
--   AGG_ID         BIGINT,
--   PRICE          DOUBLE,
--   QTY            DOUBLE,
--   TRADE_TIME     BIGINT,   -- event time in ms from Binance
--   IS_BUYER_MAKER BOOLEAN,
--   INGEST_TS      BIGINT
-- ) WITH (
--   KAFKA_TOPIC = 'raw_ticks',
--   VALUE_FORMAT = 'JSON',
--   KEY_FORMAT   = 'KAFKA',
--   TIMESTAMP    = 'trade_time'  -- use event-time semantics (not processing-time)
-- );

-- -- 2) Cleaned stream, re-keyed by SYMBOL, and explicitly set TIMESTAMP column.
-- --    We project TRADE_TIME as TS_MS and tell ksqlDB to use it as event-time.
-- CREATE OR REPLACE STREAM CLEAN_TICKS
-- WITH (
--   KAFKA_TOPIC  = 'clean_ticks',
--   VALUE_FORMAT = 'JSON',
--   TIMESTAMP    = 'TS_MS'       -- keep event-time after projection/rename
-- ) AS
-- SELECT
--   SYMBOL,
--   CAST(PRICE AS DOUBLE) AS PRICE,
--   CAST(QTY   AS DOUBLE) AS QTY,
--   TRADE_TIME            AS TS_MS
-- FROM RAW_TICKS_SRC
-- PARTITION BY SYMBOL
-- EMIT CHANGES;

-- -- 3) 1-min OHLCV TABLE (windowed) with GRACE PERIOD for late data tolerance.
-- --    GRACE PERIOD 30 SECONDS means this window remains updateable for 30s
-- --    after WINDOW_END; late ticks within that bound will revise the candle.
-- CREATE OR REPLACE TABLE OHLCV_1M
-- WITH (
--   KAFKA_TOPIC  = 'ohlcv_1m',
--   VALUE_FORMAT = 'JSON',
--   PARTITIONS   = 1,
--   REPLICAS     = 1,
--   RETENTION_MS = 604800000   -- 7 days
-- ) AS
-- SELECT
--   SYMBOL,
--   WINDOWSTART AS WINDOW_START,
--   WINDOWEND   AS WINDOW_END,
--   EARLIEST_BY_OFFSET(PRICE) AS OPEN,
--   MAX(PRICE)                AS HIGH,
--   MIN(PRICE)                AS LOW,
--   LATEST_BY_OFFSET(PRICE)   AS CLOSE,
--   SUM(QTY)                  AS VOLUME,
--   SUM(PRICE * QTY) / NULLIF(SUM(QTY), CAST(0.0 AS DOUBLE)) AS VWAP,
--   COUNT(*)                  AS TRADES
-- FROM CLEAN_TICKS
-- WINDOW TUMBLING (SIZE 1 MINUTE, GRACE PERIOD 30 SECONDS)  -- <-- late-data tolerance
-- GROUP BY SYMBOL
-- EMIT CHANGES;  -- live-updating candles (multiple upserts per minute possible)

-- -- (Optional) If you prefer one final record per candle after grace closes, use:
-- -- CREATE OR REPLACE TABLE OHLCV_1M_FINAL AS
-- -- SELECT ... (same SELECT as above)
-- -- FROM CLEAN_TICKS
-- -- WINDOW TUMBLING (SIZE 1 MINUTE, GRACE PERIOD 30 SECONDS)
-- -- GROUP BY SYMBOL
-- -- EMIT FINAL;

-- -- 4) Changelog STREAM over the table’s topic (declare windowed key!)
-- --    This exposes the table updates (including late-data revisions) as a stream.
-- CREATE STREAM IF NOT EXISTS OHLCV_1M_CHANGELOG (
--   SYMBOL        STRING KEY,
--   WINDOW_START  BIGINT,
--   WINDOW_END    BIGINT,
--   OPEN          DOUBLE,
--   HIGH          DOUBLE,
--   LOW           DOUBLE,
--   CLOSE         DOUBLE,
--   VOLUME        DOUBLE,
--   VWAP          DOUBLE,
--   TRADES        BIGINT
-- ) WITH (
--   KAFKA_TOPIC  = 'ohlcv_1m',
--   KEY_FORMAT   = 'KAFKA',
--   VALUE_FORMAT = 'JSON',
--   WINDOW_TYPE  = 'TUMBLING',
--   WINDOW_SIZE  = '1 MINUTES'
-- );

-- -- 5) AVRO sink stream for JDBC (preserve key + copy key into value)
-- --    JDBC upserts the same PK (SYMBOL_VALUE, WINDOW_START) as late data revises.
-- CREATE OR REPLACE STREAM OHLCV_1M_OUT_AVRO
-- WITH (
--   KAFKA_TOPIC  = 'ohlcv_1m_out_avro',
--   VALUE_FORMAT = 'AVRO',
--   PARTITIONS   = 1
-- ) AS
-- SELECT
--   SYMBOL,                            -- keep the KEY column in the projection
--   AS_VALUE(SYMBOL) AS SYMBOL_VALUE,  -- copy key into the VALUE for JDBC PK
--   WINDOW_START,
--   WINDOW_END,
--   OPEN,
--   HIGH,
--   LOW,
--   CLOSE,
--   VOLUME,
--   VWAP,
--   TRADES
-- FROM OHLCV_1M_CHANGELOG
-- EMIT CHANGES;

-- -- =============================================================================
-- -- NOTES:
-- -- - With GRACE, the same (SYMBOL, WINDOW_START) row may be updated multiple
-- --   times within ~30s after the minute ends; your JDBC sink should use upsert
-- --   mode with PK=(SYMBOL_VALUE, WINDOW_START) to absorb revisions.
-- -- - If you switch to EMIT FINAL, point the JDBC sink to the final output topic
-- --   (from OHLCV_1M_FINAL) to write only once per finalized candle.
-- -- =============================================================================
