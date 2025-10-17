import os, asyncio, asyncpg, time
from math import isnan

PG_DSN = os.getenv("PG_DSN", "postgresql://postgres:postgres@postgres:5432/crypto")
SYMBOLS_CSV = os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT")
POLL_MS = int(os.getenv("POLL_MS", "1000"))

class FeatureState:
    """EMA20 and ATR14 updated per *closed* bar (1m)."""
    def __init__(self, ema_alpha=2/(20+1), atr_n=14):
        self.alpha = ema_alpha
        self.atr_n = atr_n
        self.ema = None
        self.atr = None
        self.prev_close = None
        self.seeded = False
        self._atr_buf = []

    def update(self, close, high, low, close_prev):
        # TR for this bar
        candidates = []
        if high is not None and low is not None:
            candidates.append(high - low)
        if close_prev is not None and high is not None:
            candidates.append(abs(high - close_prev))
        if close_prev is not None and low is not None:
            candidates.append(abs(low - close_prev))
        tr = max([c for c in candidates if c is not None], default=None)
        if tr is None or close is None:
            return self.ema, self.atr

        # EMA20
        if self.ema is None:
            self.ema = close
        else:
            self.ema = self.alpha * close + (1 - self.alpha) * self.ema

        # ATR14 (Wilder)
        if self.atr is None:
            self._atr_buf.append(tr)
            if len(self._atr_buf) >= self.atr_n:
                self.atr = sum(self._atr_buf[-self.atr_n:]) / self.atr_n
        else:
            self.atr = (self.atr * (self.atr_n - 1) + tr) / self.atr_n

        return self.ema, self.atr

class Watcher:
    def __init__(self, dsn, symbols):
        self.dsn = dsn
        self.symbols = symbols
        self.states = {s: FeatureState() for s in symbols}
        self.last_ts = {s: None for s in symbols}

    async def _seed_state(self, con, symbol):
        # warm up ~60m for smooth EMA/ATR
        rows = await con.fetch("""
          WITH x AS (
            SELECT symbol, ts, open, high, low, close,
                   LAG(close) OVER (PARTITION BY symbol ORDER BY ts) AS close_prev
            FROM public.minute_features_stage
            WHERE symbol = $1
              AND ts >= (EXTRACT(EPOCH FROM NOW())*1000 - 60*60*1000)
          )
          SELECT * FROM x ORDER BY ts ASC;
        """, symbol)
        st = self.states[symbol]
        for r in rows:
            st.update(r["close"], r["high"], r["low"], r["close_prev"])

    async def run(self):
        pool = await asyncpg.create_pool(dsn=PG_DSN, min_size=1, max_size=3, command_timeout=10)
        async with pool.acquire() as con:
            for s in self.symbols:
                await self._seed_state(con, s)

        while True:
            try:
                async with pool.acquire() as con:
                    for s in self.symbols:
                        await self._tick_symbol(con, s)
            except Exception as e:
                print(f"[pass-watcher] error loop: {e}")
            await asyncio.sleep(POLL_MS / 1000)

    async def _tick_symbol(self, con, symbol):
        row = await con.fetchrow("""
          SELECT symbol, ts, close, high_prev, low_prev, close_prev,
                 predicted_volume, pred_z, pred_percentile_30d
          FROM public.minute_energy_enriched
          WHERE symbol = $1
          ORDER BY ts DESC
          LIMIT 1;
        """, symbol)
        if not row:
            return

        ts = row["ts"]
        if self.last_ts[symbol] == ts:
            return

        st = self.states[symbol]
        ema, atr = st.update(row["close"], row["high_prev"], row["low_prev"], row["close_prev"])
        close_minus_ema_bps = None
        atr14_1m_bps = None
        if ema:
            close_minus_ema_bps = ((row["close"] - ema) / ema) * 1e4
        if atr and row["close"]:
            atr14_1m_bps = (atr / row["close"]) * 1e4

        cfg = await con.fetchrow("""
          SELECT z_threshold, percentile_min
          FROM public.strategy_config
          WHERE symbol = $1
        """, symbol)
        z_thr = cfg["z_threshold"] if cfg else 1.0
        pct_min = cfg["percentile_min"] if cfg else 90.0

        pred_z = row["pred_z"]
        pred_pct = row["pred_percentile_30d"]
        energy_ok = False
        if pred_z is not None and not isnan(pred_z) and pred_z >= z_thr:
            energy_ok = True
        if pred_pct is not None and not isnan(pred_pct) and (pred_pct*100.0) >= pct_min:
            energy_ok = True

        close = row["close"]
        high_prev = row["high_prev"]
        low_prev = row["low_prev"]

        # Breakout & trend guards
        long_ok = False
        short_ok = False
        if high_prev is not None and close is not None and close_minus_ema_bps is not None:
            long_ok = (close >= high_prev) and (close_minus_ema_bps >= 0)
        if low_prev is not None and close is not None and close_minus_ema_bps is not None:
            short_ok = (close <= low_prev) and (close_minus_ema_bps <= 0)

        if energy_ok and (long_ok or short_ok):
            side = "LONG" if long_ok else "SHORT"
            # tick size: quick default 0.01 if no venue_meta row
            vm = await con.fetchrow("""SELECT tick_size FROM public.venue_meta WHERE symbol=$1""", symbol)
            tick = vm["tick_size"] if vm and vm["tick_size"] else 0.01
            armed_px = (high_prev + tick) if side == "LONG" else (low_prev - tick)

            reason_parts = ["energy"]
            if long_ok or short_ok:
                reason_parts.append("breakout")
                reason_parts.append("trend")
            reason = "+".join(reason_parts)

            await con.execute("""
              INSERT INTO public.pass_events (
                symbol, ts, side, armed_px,
                close, high_prev, low_prev,
                pred_z, pred_percentile_30d,
                close_minus_ema_bps, atr14_1m_bps,
                z_threshold, percentile_min, reason
              ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
              ON CONFLICT (symbol, ts) DO NOTHING;
            """,
            symbol, ts, side, armed_px,
            close, high_prev, low_prev,
            pred_z, pred_pct,
            close_minus_ema_bps, atr14_1m_bps,
            z_thr, pct_min, reason)

        self.last_ts[symbol] = ts

async def main():
    symbols = [s.strip().upper() for s in SYMBOLS_CSV.split(",") if s.strip()]
    print(f"[pass-watcher] starting for symbols: {symbols}, poll={POLL_MS}ms")
    await Watcher(PG_DSN, symbols).run()

if __name__ == "__main__":
    asyncio.run(main())
