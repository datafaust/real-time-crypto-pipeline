# api_gw/features_state.py
from dataclasses import dataclass, field
from typing import Optional, Dict, List

@dataclass
class RollingState:
    ema20: Optional[float] = None
    atr14: Optional[float] = None
    last_close: Optional[float] = None
    tr_warmup: List[float] = field(default_factory=list)

class FeatureState:
    def __init__(self):
        self.by_symbol: Dict[str, RollingState] = {}

    def update(self, symbol: str, close: float, high: float, low: float, close_prev: Optional[float]):
        st = self.by_symbol.setdefault(symbol, RollingState())

        # --- EMA20 (alpha = 2/(20+1)) ---
        alpha = 2.0 / 21.0
        if st.ema20 is None:
            st.ema20 = close  # simple seed (or SMA20 if you prefer during seeding)
        else:
            st.ema20 = alpha * close + (1 - alpha) * st.ema20

        # --- ATR14 (Wilder RMA) ---
        tr = None
        if close_prev is not None:
            tr = max(high - low, abs(high - close_prev), abs(low - close_prev))
        if st.atr14 is None:
            if tr is not None:
                st.tr_warmup.append(tr)
                if len(st.tr_warmup) == 14:
                    st.atr14 = sum(st.tr_warmup) / 14.0
        else:
            # If tr is None on very first bar, treat as 0
            st.atr14 = ((st.atr14 * (14 - 1)) + (tr or 0.0)) / 14.0

        st.last_close = close
        return st
