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

# Lookback: use minutes if set, else days (sliding window)
LOOKBACK_MINUTES = os.getenv("LOOKBACK_MINUTES")  # e.g., "120"
LOOKBACK_DAYS = os.getenv("LOOKBACK_DAYS", "7")

# Controls
MAX_LAGS = int(os.getenv("MAX_LAGS", "10"))
MIN_ROWS_PER_SYMBOL = int(os.getenv("MIN_ROWS_PER_SYMBOL", "200"))
ROWS_PER_SYMBOL_CAP = int(os.getenv("ROWS_PER_SYMBOL_CAP", "12000"))  # hard ceiling
RIDGE_ALPHA_ENV = os.getenv("RIDGE_ALPHA", "0.5")  # e.g. "0.1,0.5,1.0,2.0"
VALIDATION_FRACTION = float(os.getenv("VALIDATION_FRACTION", "0.1"))  # last 10% minutes
ENABLE_HOD_FEATURES = os.getenv("ENABLE_HOD_FEATURES", "0") == "1"    # cheap seasonality

def table_exists(engine, table_name: str) -> bool:
    q = text("""
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = :t
        LIMIT 1
    """)
    with engine.connect() as conn:
        return conn.execute(q, {"t": table_name}).first() is not None

def build_symbol_lagged(g: pd.DataFrame, max_lags: int, enable_hod: bool):
    """
    Returns (lagged_df, k) where k is the number of lags actually created.
    g must have: SYMBOL_VALUE, WINDOW_START (int ms), VOLUME (float)
    """
    g = g.sort_values("WINDOW_START").copy()
    n = len(g)
    if n < 2:
        return pd.DataFrame(), 0

    # optional: cheap intraday seasonality features (sin/cos of minute-of-day)
    if enable_hod:
        mins = (g["WINDOW_START"] // 60000) % 1440
        g["hod_sin"] = np.sin(2 * np.pi * mins / 1440).astype("float32")
        g["hod_cos"] = np.cos(2 * np.pi * mins / 1440).astype("float32")

    k = max(1, min(max_lags, n - 1))
    for j in range(1, k + 1):
        g[f"lag{j}"] = g["VOLUME"].shift(j)

    g["y"] = g["VOLUME"]
    g = g.dropna()
    return g, k

def mape(y_true, y_pred):
    denom = np.clip(np.asarray(y_true, dtype=np.float64), 1e-9, None)
    return float(np.mean(np.abs((y_true - y_pred) / denom)) * 100.0)

def main():
    eng = create_engine(PG_URL)

    if not table_exists(eng, SOURCE_TABLE):
        print(f"[trainer] Skip: table '{SOURCE_TABLE}' not found yet.")
        return

    # Build lookback fragment
    if LOOKBACK_MINUTES:
        try:
            minutes = float(LOOKBACK_MINUTES)
        except ValueError:
            minutes = 120.0
        lookback_sql = f"NOW() - INTERVAL '{minutes} minutes'"
        window_human = f"~{minutes:.1f} min"
    else:
        days = float(LOOKBACK_DAYS)
        lookback_sql = f"NOW() - INTERVAL '{days} days'"
        window_human = f"~{days:.2f} days"

    # Sliding window + per-symbol hard cap (keeps RAM bounded)
    q = text(f"""
        WITH cut AS (
          SELECT (EXTRACT(EPOCH FROM ({lookback_sql})) * 1000)::bigint AS since_ms
        ),
        sliced AS (
          SELECT "SYMBOL_VALUE","WINDOW_START","VOLUME",
                 ROW_NUMBER() OVER (
                   PARTITION BY "SYMBOL_VALUE" ORDER BY "WINDOW_START" DESC
                 ) AS rn_desc
          FROM "{SOURCE_TABLE}", cut
          WHERE "WINDOW_START" >= cut.since_ms
        )
        SELECT "SYMBOL_VALUE","WINDOW_START","VOLUME"
        FROM sliced
        WHERE rn_desc <= :cap
        ORDER BY "SYMBOL_VALUE","WINDOW_START" ASC
    """)

    try:
        df = pd.read_sql(q, eng, params={"cap": ROWS_PER_SYMBOL_CAP})
    except ProgrammingError as e:
        print(f"[trainer] Skip: query failed: {e}")
        return

    if df.empty:
        print(f"[trainer] Skip: no rows in '{SOURCE_TABLE}' for the last {window_human}.")
        return

    # Downcast to save RAM
    df["WINDOW_START"] = df["WINDOW_START"].astype("int64")
    df["VOLUME"] = df["VOLUME"].astype("float32")

    print(f"[trainer] Pulled {len(df)} rows from '{SOURCE_TABLE}' ({window_human}); "
          f"cap={ROWS_PER_SYMBOL_CAP} per symbol, MAX_LAGS={MAX_LAGS}, "
          f"MIN_ROWS_PER_SYMBOL={MIN_ROWS_PER_SYMBOL}, HOD={ENABLE_HOD_FEATURES}")

    # Parse alpha list (supports single or comma-list)
    try:
        alpha_grid = [float(a.strip()) for a in RIDGE_ALPHA_ENV.split(",")]
    except Exception:
        alpha_grid = [0.5]

    per_symbol_models = {}
    n_features_map = {}
    stats = []

    for sym, g in df.groupby("SYMBOL_VALUE", sort=False):
        lagged, k = build_symbol_lagged(
            g[["SYMBOL_VALUE", "WINDOW_START", "VOLUME"]], MAX_LAGS, ENABLE_HOD_FEATURES
        )
        n_src, n_train = len(g), len(lagged)
        stats.append(f"{sym}:n={n_src},k={k},train_rows={n_train}")

        # Need at least k+1 rows after shifting and a sanity floor
        min_needed = max(MIN_ROWS_PER_SYMBOL, k + 1)
        if k == 0 or n_train < min_needed:
            continue

        # Feature columns: lags + optional HOD features
        feature_cols = [f"lag{j}" for j in range(1, k + 1)]
        if ENABLE_HOD_FEATURES:
            feature_cols += ["hod_sin", "hod_cos"]

        X = lagged[feature_cols].to_numpy(dtype=np.float32)
        y = lagged["y"].to_numpy(dtype=np.float32)

        # Small time-aware split: last p% for validation
        split_idx = int(len(X) * (1.0 - VALIDATION_FRACTION))
        split_idx = max(split_idx, k + 1)  # avoid degenerate
        X_tr, y_tr = X[:split_idx], y[:split_idx]
        X_v,  y_v  = X[split_idx:], y[split_idx:]

        # Select alpha by lowest validation MAPE (tiny grid keeps CPU very low)
        best_alpha, best_mdl, best_mape = None, None, float("inf")
        for a in alpha_grid:
            mdl = Ridge(alpha=a)
            mdl.fit(X_tr, y_tr)
            if len(X_v) > 0:
                yhat = mdl.predict(X_v)
                cur = mape(y_v, yhat)
            else:
                cur = 0.0  # no val slice (tiny data); accept
            if cur < best_mape:
                best_alpha, best_mdl, best_mape = a, mdl, cur

        per_symbol_models[sym] = best_mdl
        n_features_map[sym] = len(feature_cols)

    if stats:
        print("[trainer] Per-symbol stats -> " + " | ".join(stats))

    if not per_symbol_models:
        print("[trainer] Skip: not enough samples per symbol yet.")
        return

    model_blob = {
        "version": "v" + datetime.utcnow().strftime("%Y%m%d_%H%M"),
        "per_symbol": per_symbol_models,
        "n_features": n_features_map,     # inference must use this
        "sklearn": "ridge",
        "alpha_grid": alpha_grid,
        "enable_hod_features": ENABLE_HOD_FEATURES,
    }

    os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)
    with open(MODEL_PATH, "wb") as f:
        pickle.dump(model_blob, f)

    print(f"[trainer] ✅ Wrote {MODEL_PATH} with {len(per_symbol_models)} symbols "
          f"(n_features={n_features_map})")

if __name__ == "__main__":
    main()



# # /trainer/train.py
# import os
# import pickle
# import pandas as pd
# import numpy as np
# from datetime import datetime
# from sqlalchemy import create_engine, text
# from sqlalchemy.exc import ProgrammingError
# from sklearn.linear_model import Ridge

# PG_URL = os.getenv("PG_URL", "postgresql+psycopg2://postgres:postgres@postgres:5432/crypto")
# MODEL_PATH = os.getenv("MODEL_PATH", "/models/model.pkl")
# SOURCE_TABLE = os.getenv("SOURCE_TABLE", "ohlcv_1m")

# # Fast bootstrap window
# LOOKBACK_MINUTES = os.getenv("LOOKBACK_MINUTES")       # e.g. "7"
# LOOKBACK_DAYS = os.getenv("LOOKBACK_DAYS", "7")

# # Small thresholds
# MAX_LAGS = int(os.getenv("MAX_LAGS", "3"))             # max lags per symbol
# MIN_ROWS_PER_SYMBOL = int(os.getenv("MIN_ROWS_PER_SYMBOL", "3"))

# def table_exists(engine, table_name: str) -> bool:
#     q = text("""
#         SELECT 1
#         FROM information_schema.tables
#         WHERE table_schema = 'public' AND table_name = :t
#         LIMIT 1
#     """)
#     with engine.connect() as conn:
#         return conn.execute(q, {"t": table_name}).first() is not None

# def build_symbol_lagged(g: pd.DataFrame, max_lags: int):
#     """
#     Return (lagged_df, k) where k is the number of lags actually created.
#     """
#     g = g.sort_values("WINDOW_START").copy()
#     n = len(g)
#     if n < 2:
#         return pd.DataFrame(), 0
#     k = max(1, min(max_lags, n - 1))
#     for j in range(1, k + 1):
#         g[f"lag{j}"] = g["VOLUME"].shift(j)
#     g["y"] = g["VOLUME"]
#     g = g.dropna()
#     return g, k

# def main():
#     eng = create_engine(PG_URL)

#     if not table_exists(eng, SOURCE_TABLE):
#         print(f"[trainer] Skip: table '{SOURCE_TABLE}' not found yet.")
#         return

#     if LOOKBACK_MINUTES:
#         try:
#             minutes = float(LOOKBACK_MINUTES)
#         except ValueError:
#             minutes = 7.0
#         where = f"NOW() - INTERVAL '{minutes} minutes'"
#         window_human = f"~{minutes:.1f} min"
#     else:
#         days = float(LOOKBACK_DAYS)
#         where = f"NOW() - INTERVAL '{days} days'"
#         window_human = f"~{days:.2f} days"

#     q = f"""
#         SELECT "SYMBOL_VALUE","WINDOW_START","VOLUME"
#         FROM "{SOURCE_TABLE}"
#         WHERE "WINDOW_START" >= (EXTRACT(EPOCH FROM {where})*1000)
#     """

#     try:
#         df = pd.read_sql(q, eng)
#     except ProgrammingError as e:
#         print(f"[trainer] Skip: query failed: {e}")
#         return

#     if df.empty:
#         print(f"[trainer] Skip: no rows in '{SOURCE_TABLE}' for the last {window_human}.")
#         return

#     print(f"[trainer] Pulled {len(df)} rows from '{SOURCE_TABLE}' ({window_human}).")

#     per_symbol_models = {}
#     n_features_map = {}
#     stats = []

#     for sym, g in df.groupby("SYMBOL_VALUE"):
#         lagged, k = build_symbol_lagged(g[["SYMBOL_VALUE", "WINDOW_START", "VOLUME"]], MAX_LAGS)
#         n_src, n_train = len(g), len(lagged)
#         stats.append(f"{sym}:n={n_src},k={k},train_rows={n_train}")

#         # Need at least k+1 rows after shifting to have k features reliably
#         if k == 0 or n_train < max(MIN_ROWS_PER_SYMBOL, k + 1):
#             continue

#         feature_cols = [f"lag{j}" for j in range(1, k + 1)]
#         X = lagged[feature_cols].values
#         y = lagged["y"].values

#         mdl = Ridge(alpha=0.5)
#         mdl.fit(X, y)
#         per_symbol_models[sym] = mdl
#         n_features_map[sym] = k   # <- authoritative

#     if stats:
#         print("[trainer] Per-symbol stats -> " + " | ".join(stats))

#     if not per_symbol_models:
#         print("[trainer] Skip: not enough samples per symbol yet.")
#         return

#     model_blob = {
#         "version": "v" + datetime.utcnow().strftime("%Y%m%d_%H%M"),
#         "per_symbol": per_symbol_models,
#         "n_features": n_features_map,  # <- inference must use this
#         "sklearn": "ridge",            # just a hint for debugging
#     }

#     os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)
#     with open(MODEL_PATH, "wb") as f:
#         pickle.dump(model_blob, f)

#     print(f"[trainer] ✅ Wrote {MODEL_PATH} with {len(per_symbol_models)} symbols "
#           f"(n_features={n_features_map})")

# if __name__ == "__main__":
#     main()
