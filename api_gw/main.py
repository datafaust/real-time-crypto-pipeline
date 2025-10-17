# api_gw/main.py
import os, json, asyncio, time
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import asyncpg

# Rolling EMA/ATR state
from features_state import FeatureState

app = FastAPI(title="Crypto API Gateway")

# ---------- CORS ----------
def _csv(name: str, default: str = ""):
    raw = os.getenv(name, default)
    return [x.strip() for x in raw.split(",") if x.strip()]

ALLOW_ORIGINS = _csv("CORS_ALLOW_ORIGINS")
ALLOW_ORIGIN_REGEX = os.getenv("CORS_ALLOW_ORIGIN_REGEX") or None
if not ALLOW_ORIGINS and not ALLOW_ORIGIN_REGEX:
    ALLOW_ORIGINS = ["http://localhost:3000"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOW_ORIGINS,
    allow_origin_regex=ALLOW_ORIGIN_REGEX,
    allow_credentials=False,
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["*"],
    max_age=3600,
)

# ---------- Security headers ----------
@app.middleware("http")
async def add_security_headers(request: Request, call_next):
    resp: Response = await call_next(request)
    resp.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains; preload"
    resp.headers["X-Content-Type-Options"] = "nosniff"
    resp.headers["Referrer-Policy"] = "no-referrer"
    resp.headers["X-Frame-Options"] = "DENY"
    resp.headers["Permissions-Policy"] = "geolocation=(), microphone=(), camera=()"
    if ALLOW_ORIGINS and "*" not in ALLOW_ORIGINS:
        vary = resp.headers.get("Vary", "")
        resp.headers["Vary"] = (vary + ", Origin").lstrip(", ")
    return resp

# ---------- DB ----------
_POOL = None
async def get_pool():
    global _POOL
    if _POOL is None:
        dsn = os.getenv("DATABASE_URL")
        if not dsn:
            raise RuntimeError("DATABASE_URL is not set")
        _POOL = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=5, command_timeout=10)
    return _POOL

@app.get("/healthz")
async def healthz():
    return {"ok": True}

# ---------- API (existing) ----------
@app.get("/api/symbols")
async def list_symbols():
    pool = await get_pool()
    rows = await pool.fetch('SELECT DISTINCT "SYMBOL_VALUE" AS symbol FROM "ohlcv_1m" ORDER BY 1')
    return [r["symbol"] for r in rows]

@app.get("/api/ohlcv/latest")
async def ohlcv_latest(symbol: str):
    pool = await get_pool()
    row = await pool.fetchrow(
        '''
        SELECT "SYMBOL_VALUE","WINDOW_START","OPEN","HIGH","LOW","CLOSE","VOLUME","VWAP","TRADES"
        FROM "ohlcv_1m"
        WHERE "SYMBOL_VALUE" = $1
        ORDER BY "WINDOW_START" DESC
        LIMIT 1
        ''', symbol
    )
    if not row:
        raise HTTPException(status_code=404, detail="symbol not found")
    return dict(row)

@app.get("/api/predictions/latest")
async def predictions_latest(symbol: str):
    pool = await get_pool()
    row = await pool.fetchrow(
        '''
        SELECT "SYMBOL_VALUE","WINDOW_START","PREDICTED_VOLUME","MODEL_VERSION"
        FROM "predictions_1m"
        WHERE "SYMBOL_VALUE" = $1
        ORDER BY "WINDOW_START" DESC
        LIMIT 1
        ''', symbol
    )
    if not row:
        raise HTTPException(status_code=404, detail="prediction not found")
    return dict(row)

@app.get("/api/alignment/range")
async def alignment_range(symbol: str, minutes: int = 60):
    pool = await get_pool()
    rows = await pool.fetch(
        '''
        WITH preds AS (
          SELECT "SYMBOL_VALUE","WINDOW_START" AS p_ts,"PREDICTED_VOLUME"
          FROM "predictions_1m"
          WHERE "SYMBOL_VALUE" = $1
            AND "WINDOW_START" >= NOW() - ($2::text || ' minutes')::interval
        ),
        ohlc AS (
          SELECT "SYMBOL_VALUE", TO_TIMESTAMP("WINDOW_START"/1000.0) AS o_ts, "VOLUME"
          FROM "ohlcv_1m"
          WHERE "SYMBOL_VALUE" = $1
            AND "WINDOW_START" >= EXTRACT(EPOCH FROM (NOW() - ($2::text || ' minutes')::interval))*1000
        )
        SELECT ohlc."SYMBOL_VALUE" AS symbol,
               ohlc.o_ts           AS window_start,
               ohlc."VOLUME"       AS actual_volume,
               preds."PREDICTED_VOLUME" AS predicted_volume
        FROM ohlc
        LEFT JOIN preds
          ON preds."SYMBOL_VALUE" = ohlc."SYMBOL_VALUE"
         AND preds.p_ts = ohlc.o_ts
        ORDER BY window_start ASC
        ''', symbol, str(minutes)
    )
    return [dict(r) for r in rows]

@app.get("/api/price/range")
async def price_range(symbol: str, minutes: int = 120):
    """
    Returns minute closes for the past `minutes` for a symbol.
    """
    pool = await get_pool()
    rows = await pool.fetch(
        """
        SELECT
          TO_TIMESTAMP(o."WINDOW_START"/1000.0) AS ts,
          o."CLOSE" AS close
        FROM "ohlcv_1m" o
        WHERE o."SYMBOL_VALUE" = $1
          AND o."WINDOW_START" >= EXTRACT(EPOCH FROM (NOW() - ($2::text || ' minutes')::interval))*1000
        ORDER BY o."WINDOW_START" ASC
        """,
        symbol, str(minutes)
    )
    return [ {"timestamp": r["ts"].isoformat(), "close": r["close"]} for r in rows ]

# ---------- NEW: strategy config (thresholds) ----------
@app.get("/api/strategy/config")
async def get_strategy_config(symbol: str):
    """
    Returns the strategy thresholds for a symbol.
    If no row exists, sensible defaults are returned (z=1.0, percentile=90).
    """
    pool = await get_pool()
    row = await pool.fetchrow(
        'SELECT z_threshold, percentile_min FROM public.strategy_config WHERE symbol = $1',
        symbol
    )
    if not row:
        return {"symbol": symbol, "z_threshold": 1.0, "percentile_min": 90.0}
    return {"symbol": symbol, "z_threshold": row["z_threshold"], "percentile_min": row["percentile_min"]}

# ---------- SSE: joined OHLCV + prediction ----------
@app.get("/sse/stream")
async def sse_stream(symbol: str):
    async def event_gen():
        pool = await get_pool()
        last_ts = None
        last_pred = None

        sql = '''
            SELECT
              o."SYMBOL_VALUE",
              TO_TIMESTAMP(o."WINDOW_START"/1000.0) AS "WINDOW_START",
              o."CLOSE"  AS "CLOSE",
              o."VOLUME" AS "VOLUME",
              o."VOLUME" AS "ACTUAL_VOLUME",
              o."TRADES" AS "TRADES",
              p."PREDICTED_VOLUME",
              p."MODEL_VERSION"
            FROM "ohlcv_1m" o
            LEFT JOIN "predictions_1m" p
              ON p."SYMBOL_VALUE" = o."SYMBOL_VALUE"
             AND p."WINDOW_START" = TO_TIMESTAMP(o."WINDOW_START"/1000.0)
            WHERE o."SYMBOL_VALUE" = $1
            ORDER BY o."WINDOW_START" DESC
            LIMIT 1;
        '''

        while True:
            row = await pool.fetchrow(sql, symbol)
            if row:
                payload = dict(row)
                ts_dt = payload.get("WINDOW_START")
                ts_iso = ts_dt.isoformat().replace("+00:00", "Z") if ts_dt else None
                payload["WINDOW_START"] = ts_iso

                pred = payload.get("PREDICTED_VOLUME")
                should_emit = (ts_iso != last_ts) or (ts_iso == last_ts and last_pred is None and pred is not None)

                if should_emit:
                    yield f"data: {json.dumps(payload)}\n\n"
                    last_ts = ts_iso
                    last_pred = pred

            await asyncio.sleep(1)
    return StreamingResponse(event_gen(), media_type="text/event-stream")

# ---------- NEW: SSE for 1B features (+ energy) ----------
_feat_state = FeatureState()

async def _seed_ema_atr(pool):
    """
    Warm up EMA/ATR state using last ~60 minutes per symbol so restart is smooth.
    """
    async with pool.acquire() as con:
        rows = await con.fetch("""
          WITH latest AS (
            SELECT symbol, ts, open, high, low, close,
                   LAG(close) OVER (PARTITION BY symbol ORDER BY ts) AS close_prev
            FROM public.minute_features_stage
            WHERE ts >= (EXTRACT(EPOCH FROM NOW())*1000 - 60*60*1000)
          )
          SELECT * FROM latest ORDER BY symbol, ts;
        """)
    for r in rows:
        _feat_state.update(
            r["symbol"], r["close"], r["high"], r["low"], r["close_prev"]
        )

@app.get("/sse/features")
async def sse_features(symbol: str):
    """
    Streams per-minute features for `symbol` with an AS-OF prediction:
      - prior-minute rails & delta (from minute_features_stage)
      - ema20/atr14 (rolling in-process)
      - countdown to bar close
      - energy fields computed using the most-recent prediction <= bar start:
          predicted_volume, pred_z (via pred_stats_30d_mv), pred_percentile_30d (via ranked view)
    """
    pool = await get_pool()
    await _seed_ema_atr(pool)
    last_ts = None

    async def gen():
        nonlocal last_ts
        # AS-OF join:
        #   m = latest minute from minute_features_stage
        #   r = most recent prediction at or before m.ts
        #   s = 30d stats to compute z
        #   rr = percentile for the r.p_ts prediction
        sql = """
          WITH m AS (
            SELECT symbol, ts, open, high, low, close, vwap, trades,
                   high_prev, low_prev, close_prev, price_1m_delta_bps
            FROM public.minute_features_stage
            WHERE symbol = $1
            ORDER BY ts DESC
            LIMIT 1
          ),
          r AS (
            SELECT p."SYMBOL_VALUE" AS symbol,
                   p."WINDOW_START" AS p_ts,
                   p."PREDICTED_VOLUME" AS predicted_volume
            FROM public.predictions_1m p
            JOIN m ON m.symbol = p."SYMBOL_VALUE"
            WHERE p."WINDOW_START" <= TO_TIMESTAMP(m.ts/1000.0)
            ORDER BY p."WINDOW_START" DESC
            LIMIT 1
          ),
          s AS (
            SELECT ps.symbol, ps.mean_pred, ps.std_pred
            FROM public.pred_stats_30d_mv ps
            JOIN m ON m.symbol = ps.symbol
          ),
          rr AS (
            SELECT pr.symbol, pr.ts AS p_ts, pr.pred_percentile_30d
            FROM public.predictions_recent_30d_ranked pr
            JOIN r ON r.symbol = pr.symbol AND r.p_ts = pr.ts
          )
          SELECT
            m.symbol,
            m.ts,
            m.open, m.high, m.low, m.close, m.vwap, m.trades,
            m.high_prev, m.low_prev, m.close_prev, m.price_1m_delta_bps,
            r.predicted_volume,
            CASE
              WHEN s.std_pred IS NULL OR s.std_pred = 0 OR r.predicted_volume IS NULL THEN NULL
              ELSE (r.predicted_volume - s.mean_pred) / s.std_pred
            END AS pred_z,
            rr.pred_percentile_30d
          FROM m
          LEFT JOIN r  ON TRUE
          LEFT JOIN s  ON TRUE
          LEFT JOIN rr ON TRUE;
        """

        while True:
            row = await (await get_pool()).fetchrow(sql, symbol)
            if row:
                ts = row["ts"]  # bigint ms (minute start)
                if ts != last_ts:
                    # Update rolling EMA/ATR from minute OHLC
                    st = _feat_state.update(
                        row["symbol"], row["close"], row["high"], row["low"], row["close_prev"]
                    )
                    ema20 = st.ema20
                    close_minus_ema_bps = None
                    atr14_1m_bps = None
                    if ema20:
                        close_minus_ema_bps = ((row["close"] - ema20) / ema20) * 1e4
                    if st.atr14 and row["close"]:
                        atr14_1m_bps = (st.atr14 / row["close"]) * 1e4

                    # countdown to bar close (ts is WINDOW_START in ms)
                    now_ms = int(time.time() * 1000)
                    bar_close_ms = ts + 60_000
                    countdown_ms = max(0, bar_close_ms - now_ms)

                    payload = {
                        "symbol": row["symbol"],
                        "ts": ts,
                        "close": row["close"],
                        "high_prev": row["high_prev"],
                        "low_prev": row["low_prev"],
                        "close_prev": row["close_prev"],
                        "price_1m_delta_bps": row["price_1m_delta_bps"],
                        "ema20": ema20,
                        "close_minus_ema_bps": close_minus_ema_bps,
                        "atr14_1m_bps": atr14_1m_bps,
                        "bar_close_countdown_ms": countdown_ms,
                        # energy (AS-OF)
                        "predicted_volume": row["predicted_volume"],
                        "pred_z": row["pred_z"],
                        "pred_percentile_30d": row["pred_percentile_30d"],
                    }
                    yield f"data: {json.dumps(payload)}\n\n"
                    last_ts = ts
            await asyncio.sleep(1)

    return StreamingResponse(gen(), media_type="text/event-stream")
