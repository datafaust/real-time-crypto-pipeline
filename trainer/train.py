# /trainer/train.py
import os
import pickle
import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.exc import ProgrammingError
from sklearn.linear_model import Ridge

PG_URL = os.getenv("PG_URL", "postgresql+psycopg2://postgres:postgres@postgres:5432/crypto")
MODEL_PATH = os.getenv("MODEL_PATH", "/models/model.pkl")
SOURCE_TABLE = os.getenv("SOURCE_TABLE", "ohlcv_1m")

# Fast bootstrap window
LOOKBACK_MINUTES = os.getenv("LOOKBACK_MINUTES")       # e.g. "7"
LOOKBACK_DAYS = os.getenv("LOOKBACK_DAYS", "7")

# Small thresholds
MAX_LAGS = int(os.getenv("MAX_LAGS", "3"))             # max lags per symbol
MIN_ROWS_PER_SYMBOL = int(os.getenv("MIN_ROWS_PER_SYMBOL", "3"))

def table_exists(engine, table_name: str) -> bool:
    q = text("""
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = :t
        LIMIT 1
    """)
    with engine.connect() as conn:
        return conn.execute(q, {"t": table_name}).first() is not None

def build_symbol_lagged(g: pd.DataFrame, max_lags: int):
    """
    Return (lagged_df, k) where k is the number of lags actually created.
    """
    g = g.sort_values("WINDOW_START").copy()
    n = len(g)
    if n < 2:
        return pd.DataFrame(), 0
    k = max(1, min(max_lags, n - 1))
    for j in range(1, k + 1):
        g[f"lag{j}"] = g["VOLUME"].shift(j)
    g["y"] = g["VOLUME"]
    g = g.dropna()
    return g, k

def main():
    eng = create_engine(PG_URL)

    if not table_exists(eng, SOURCE_TABLE):
        print(f"[trainer] Skip: table '{SOURCE_TABLE}' not found yet.")
        return

    if LOOKBACK_MINUTES:
        try:
            minutes = float(LOOKBACK_MINUTES)
        except ValueError:
            minutes = 7.0
        where = f"NOW() - INTERVAL '{minutes} minutes'"
        window_human = f"~{minutes:.1f} min"
    else:
        days = float(LOOKBACK_DAYS)
        where = f"NOW() - INTERVAL '{days} days'"
        window_human = f"~{days:.2f} days"

    q = f"""
        SELECT "SYMBOL_VALUE","WINDOW_START","VOLUME"
        FROM "{SOURCE_TABLE}"
        WHERE "WINDOW_START" >= (EXTRACT(EPOCH FROM {where})*1000)
    """

    try:
        df = pd.read_sql(q, eng)
    except ProgrammingError as e:
        print(f"[trainer] Skip: query failed: {e}")
        return

    if df.empty:
        print(f"[trainer] Skip: no rows in '{SOURCE_TABLE}' for the last {window_human}.")
        return

    print(f"[trainer] Pulled {len(df)} rows from '{SOURCE_TABLE}' ({window_human}).")

    per_symbol_models = {}
    n_features_map = {}
    stats = []

    for sym, g in df.groupby("SYMBOL_VALUE"):
        lagged, k = build_symbol_lagged(g[["SYMBOL_VALUE", "WINDOW_START", "VOLUME"]], MAX_LAGS)
        n_src, n_train = len(g), len(lagged)
        stats.append(f"{sym}:n={n_src},k={k},train_rows={n_train}")

        # Need at least k+1 rows after shifting to have k features reliably
        if k == 0 or n_train < max(MIN_ROWS_PER_SYMBOL, k + 1):
            continue

        feature_cols = [f"lag{j}" for j in range(1, k + 1)]
        X = lagged[feature_cols].values
        y = lagged["y"].values

        mdl = Ridge(alpha=0.5)
        mdl.fit(X, y)
        per_symbol_models[sym] = mdl
        n_features_map[sym] = k   # <- authoritative

    if stats:
        print("[trainer] Per-symbol stats -> " + " | ".join(stats))

    if not per_symbol_models:
        print("[trainer] Skip: not enough samples per symbol yet.")
        return

    model_blob = {
        "version": "v" + datetime.utcnow().strftime("%Y%m%d_%H%M"),
        "per_symbol": per_symbol_models,
        "n_features": n_features_map,  # <- inference must use this
        "sklearn": "ridge",            # just a hint for debugging
    }

    os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)
    with open(MODEL_PATH, "wb") as f:
        pickle.dump(model_blob, f)

    print(f"[trainer] ✅ Wrote {MODEL_PATH} with {len(per_symbol_models)} symbols "
          f"(n_features={n_features_map})")

if __name__ == "__main__":
    main()
