# api_gw/main.py
import os, json, asyncio
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import asyncpg

app = FastAPI(title="Crypto API Gateway")

# ---------- CORS ----------
# Supports:
#   CORS_ALLOW_ORIGINS="http://localhost:3000,https://crypto-dashboard-one-xi.vercel.app/"
#   CORS_ALLOW_ORIGIN_REGEX="https://.*\\.vercel\\.app"
def _csv(name: str, default: str = ""):
    raw = os.getenv(name, default)
    return [x.strip() for x in raw.split(",") if x.strip()]

ALLOW_ORIGINS = _csv("CORS_ALLOW_ORIGINS")
ALLOW_ORIGIN_REGEX = os.getenv("CORS_ALLOW_ORIGIN_REGEX") or None

# If nothing set, allow localhost during dev (and you can add your Vercel origin below)
if not ALLOW_ORIGINS and not ALLOW_ORIGIN_REGEX:
    ALLOW_ORIGINS = ["http://localhost:3000"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOW_ORIGINS,
    allow_origin_regex=ALLOW_ORIGIN_REGEX,
    allow_credentials=False,           # keep False if using "*" or no cookies
    allow_methods=["GET", "OPTIONS"],  # include OPTIONS for preflight
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
    # Make caches vary by Origin when not wildcard
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

# ---------- API ----------
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

@app.get("/sse/stream")
async def sse_stream(symbol: str):
    async def event_gen():
        pool = await get_pool()
        last_ts = None
        last_pred = None

        # Latest OHLCV minute for the symbol, joined to the prediction for the SAME minute
        sql = '''
            SELECT
              o."SYMBOL_VALUE",
              TO_TIMESTAMP(o."WINDOW_START"/1000.0) AS "WINDOW_START",
              o."VOLUME" AS "ACTUAL_VOLUME",
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
                # format payload
                payload = dict(row)
                ts_dt = payload.get("WINDOW_START")
                ts_iso = ts_dt.isoformat().replace("+00:00", "Z") if ts_dt else None
                payload["WINDOW_START"] = ts_iso

                pred = payload.get("PREDICTED_VOLUME")
                # Emit when: new minute OR same minute but prediction flipped from null -> value
                should_emit = (ts_iso != last_ts) or (ts_iso == last_ts and last_pred is None and pred is not None)

                if should_emit:
                    yield f"data: {json.dumps(payload)}\n\n"
                    last_ts = ts_iso
                    last_pred = pred

            await asyncio.sleep(1)
    return StreamingResponse(event_gen(), media_type="text/event-stream")
