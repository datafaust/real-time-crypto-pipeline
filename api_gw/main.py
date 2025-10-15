# api_gw/main.py
import os, json, asyncio, re
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import asyncpg

app = FastAPI(title="Crypto API Gateway")

# --- CORS config ---
# Supports:
#   CORS_ALLOW_ORIGINS="http://localhost:3000,http://10.2.1.50:3000,https://your-vercel-app.vercel.app"
#   CORS_ALLOW_ORIGIN="http://localhost:3000"  (legacy single)
#   CORS_ALLOW_ORIGIN_REGEX="https://.*\\.vercel\\.app" (optional)
raw_list = os.getenv("CORS_ALLOW_ORIGINS", "")
single = os.getenv("CORS_ALLOW_ORIGIN", "")
regex_pat = os.getenv("CORS_ALLOW_ORIGIN_REGEX", "")

allow_origins = [o.strip() for o in raw_list.split(",") if o.strip()]
if single and single not in allow_origins:
    allow_origins.append(single)

allow_origin_regex = regex_pat or None

app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins or ["http://localhost:3000"],
    allow_origin_regex=allow_origin_regex,
    allow_credentials=False,           # set True only if you use cookies/auth
    allow_methods=["GET", "OPTIONS"],  # include OPTIONS for preflight
    allow_headers=["*"],
    max_age=600,
)

_POOL = None
async def get_pool():
    global _POOL
    if _POOL is None:
        dsn = os.getenv("DATABASE_URL")  # e.g. postgresql://readonly:readonly@postgres:5432/crypto
        if not dsn:
            raise RuntimeError("DATABASE_URL is not set")
        _POOL = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=5, command_timeout=10)
    return _POOL

@app.get("/healthz")
async def healthz():
    return {"ok": True}

# ---------- Basics ----------

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
        ''',
        symbol,
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
        ''',
        symbol,
    )
    if not row:
        raise HTTPException(status_code=404, detail="prediction not found")
    return dict(row)

# ---------- Chart backfill (actual vs predicted) ----------

@app.get("/api/alignment/range")
async def alignment_range(symbol: str, minutes: int = 60):
    """
    Returns aligned actual vs predicted for the last `minutes`.
    ohlcv_1m stores ms epoch; predictions_1m stores timestamp.
    """
    pool = await get_pool()
    rows = await pool.fetch(
        '''
        WITH preds AS (
          SELECT "SYMBOL_VALUE",
                 "WINDOW_START" AS p_ts,
                 "PREDICTED_VOLUME"
          FROM "predictions_1m"
          WHERE "SYMBOL_VALUE" = $1
            AND "WINDOW_START" >= NOW() - ($2::text || ' minutes')::interval
        ),
        ohlc AS (
          SELECT "SYMBOL_VALUE",
                 TO_TIMESTAMP("WINDOW_START"/1000.0) AS o_ts,
                 "VOLUME"
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
        ''',
        symbol,
        str(minutes),
    )
    # Convert records to plain dicts; asyncpg returns datetime directly JSON-serializable via FastAPI
    return [dict(r) for r in rows]

# ---------- Live SSE (latest-only inline join; no DB view) ----------

@app.get("/sse/stream")
async def sse_stream(symbol: str):
    """
    Emits the latest joined OHLCV + prediction for `symbol` once per second.
    Lightweight query that only touches the newest minute.
    """

    async def event_gen():
        last_ts = None
        pool = await get_pool()
        sql = '''
            WITH p AS (
              SELECT "SYMBOL_VALUE",
                     "WINDOW_START" AS p_ts,
                     "PREDICTED_VOLUME",
                     "MODEL_VERSION"
              FROM "predictions_1m"
              WHERE "SYMBOL_VALUE" = $1
              ORDER BY "WINDOW_START" DESC
              LIMIT 1
            )
            SELECT
              o."SYMBOL_VALUE",
              TO_TIMESTAMP(o."WINDOW_START"/1000.0) AS "WINDOW_START",
              o."VOLUME" AS "ACTUAL_VOLUME",
              p."PREDICTED_VOLUME",
              p."MODEL_VERSION"
            FROM "ohlcv_1m" o
            LEFT JOIN p
              ON p."SYMBOL_VALUE" = o."SYMBOL_VALUE"
             AND o."WINDOW_START" = EXTRACT(EPOCH FROM p.p_ts)*1000
            WHERE o."SYMBOL_VALUE" = $1
            ORDER BY o."WINDOW_START" DESC
            LIMIT 1;
        '''
        while True:
            row = await pool.fetchrow(sql, symbol)
            if row:
                payload = dict(row)
                # ensure timestamp is a string for SSE
                if payload.get("WINDOW_START") is not None:
                    payload["WINDOW_START"] = str(payload["WINDOW_START"])
                ts_key = payload.get("WINDOW_START")
                if ts_key != last_ts:
                    yield f"data: {json.dumps(payload)}\n\n"
                    last_ts = ts_key
            await asyncio.sleep(1)

    return StreamingResponse(event_gen(), media_type="text/event-stream")
