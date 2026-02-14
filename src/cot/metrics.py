# src/cot/metrics.py
from __future__ import annotations

import numpy as np
import pandas as pd


### this function calculates the percentile rank of the latest value in a rolling window.
### ie. “Within the past N weeks, what fraction of values are ≤ the current week’s value?”

def _rolling_percentile_last(x: np.ndarray) -> float:
    """
    Percentile rank of the last value within the rolling window.
    Returns value in [0, 1]. Caller can scale to 0-100.
    """
    if len(x) == 0 or np.isnan(x[-1]):
        return np.nan
    # simple percentile: proportion <= last
    return float(np.sum(x <= x[-1]) / len(x))

### this function calculates a min–max oscillator of the latest value in the rolling window, scaled 0–1 (then later 0–100)
### ie. “Where does the current value sit between the minimum and maximum of the last N weeks?”
### 0 = at recent lows (extreme short relative to window)
### 100 = at recent highs (extreme long relative to window)

def _rolling_minmax_last(x: np.ndarray) -> float:
    """
    Min-max oscillator of the last value within the rolling window.
    Returns value in [0, 1]. Caller can scale to 0-100.
    """
    if len(x) == 0 or np.isnan(x[-1]):
        return np.nan
    xmin = np.nanmin(x)
    xmax = np.nanmax(x)
    if not np.isfinite(xmin) or not np.isfinite(xmax) or xmax == xmin:
        return np.nan
    return float((x[-1] - xmin) / (xmax - xmin))


def add_position_metrics(
    df: pd.DataFrame,
    lookbacks_weeks: dict[str, int] | None = None,
    min_periods: int = 52,
    compute_for: list[str] | None = None,
    include_score_changes: bool = True,
) -> pd.DataFrame:
    
    out = df.copy()
    out = out.sort_values(["dataset", "group", "cftc_code", "date"])

    if lookbacks_weeks is None:
        lookbacks_weeks = {
            "3y": 156,
            "5y": 260,
            # "max" uses expanding window
        }

    if compute_for is None:
        # What users want to express/score on
        compute_for = ["net", "pct_oi_net"]

    g = out.groupby(["dataset", "group", "cftc_code"], group_keys=False)

    
    ### Changes metrics (WoW/MoM/13w) 
    ### For each contract (grouped by dataset, group, cftc_code), it computes differences:
    ### 1w change (WoW): diff(1)
    ### 4w change (MoM approx): diff(4)
    ### 13w change (quarter-ish): diff(13)
    ### for net, long, short, spreading, %OI 
  
    base_change_cols = ["long", "short", "spreading", "net", "open_interest"]
    for col in base_change_cols:
        if col in out.columns:
            out[f"{col}_chg_1w"] = g[col].diff(1)
            out[f"{col}_chg_4w"] = g[col].diff(4)
            out[f"{col}_chg_13w"] = g[col].diff(13)

    # %OI-based changes 
    pct_cols = ["pct_oi_net", "pct_oi_long", "pct_oi_short"]
    for col in pct_cols:
        if col in out.columns:
            out[f"{col}_chg_1w"] = g[col].diff(1)
            out[f"{col}_chg_4w"] = g[col].diff(4)
            out[f"{col}_chg_13w"] = g[col].diff(13)

    
    ### Rolling percentile score (3y/5y/max)

    def roll_pct(s, w):
        s = pd.to_numeric(s, errors="coerce")

        return (
            s.rolling(window=w, min_periods=w)
             .apply(lambda x: pd.Series(x).rank(pct=True).iloc[-1] * 100)
        )


    ### Rolling min-max oscillator (3y/5y/max) 

    def roll_minmax(s, w):
        s = pd.to_numeric(s, errors="coerce")

        roll = s.rolling(window=w, min_periods=w)

        mn = roll.min()
        mx = roll.max()

        denom = (mx - mn)

        mm = (s - mn) / denom.where(denom != 0) * 100

        return mm


    ### Rolling z-score (3y/5y/max)
    ### tells how many standard deviations from mean 

    def roll_z(s, w):
        s = pd.to_numeric(s, errors="coerce")

        roll = s.rolling(window=w, min_periods=w)

        mean = roll.mean()
        std = roll.std()

        # Avoid divide-by-zero
        z = (s - mean) / std.where(std != 0)

        return z


    for expr in compute_for:
        if expr not in out.columns:
            continue

        # fixed lookbacks (3y/5y etc)
        for tag, w in lookbacks_weeks.items():
            pct01 = g[expr].apply(lambda s: roll_pct(s, w))
            mm01 = g[expr].apply(lambda s: roll_minmax(s, w))
            z = g[expr].apply(lambda s: roll_z(s, w))

            out[f"{expr}_pctile_{tag}"] = pct01 
            out[f"{expr}_minmax_{tag}"] = mm01 
            out[f"{expr}_z_{tag}"] = z

            if include_score_changes:
                out[f"{expr}_pctile_{tag}_chg_1w"] = g[f"{expr}_pctile_{tag}"].diff(1)
                out[f"{expr}_pctile_{tag}_chg_4w"] = g[f"{expr}_pctile_{tag}"].diff(4)
                out[f"{expr}_pctile_{tag}_chg_13w"] = g[f"{expr}_pctile_{tag}"].diff(13)

        def expanding_pct(s: pd.Series) -> pd.Series:
            return s.expanding(min_periods=min_periods).apply(
            lambda x: _rolling_percentile_last(x.to_numpy(dtype=float)), raw=False,
            )

        def expanding_minmax(s: pd.Series) -> pd.Series:
            return s.expanding(min_periods=min_periods).apply(
                lambda x: _rolling_minmax_last(x.to_numpy(dtype=float)), raw=False,
            )

        def expanding_z(s: pd.Series) -> pd.Series:
            m = s.expanding(min_periods=min_periods).mean()
            sd = s.expanding(min_periods=min_periods).std(ddof=0)
            return (s - m) / sd

        out[f"{expr}_pctile_max"] = g[expr].apply(expanding_pct) * 100.0
        out[f"{expr}_minmax_max"] = g[expr].apply(expanding_minmax) * 100.0
        out[f"{expr}_z_max"] = g[expr].apply(expanding_z)
        
    return out

