# src/cot/transform.py
from __future__ import annotations
import pandas as pd


def _to_datetime(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce")


def _to_numeric(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def standardize_tff_group(df_raw: pd.DataFrame, group: str = "lev_money") -> pd.DataFrame:
    df = df_raw.copy()

    date_col = "report_date_as_yyyy_mm_dd"
    oi_col = "open_interest_all"
    market_col = "market_and_exchange_names"
    code_col = "cftc_contract_market_code"
    name_col = "contract_market_name"   

    group_map = {
        "dealer": ("dealer_positions_long_all", "dealer_positions_short_all", "dealer_positions_spread_all"),
        "asset_mgr": ("asset_mgr_positions_long_all", "asset_mgr_positions_short_all", "asset_mgr_positions_spread_all"),
        "lev_money": ("lev_money_positions_long", "lev_money_positions_short", "lev_money_positions_spread"),
    }

    if group not in group_map:
        raise ValueError(f"Unknown group={group}. Use one of: {list(group_map.keys())}")

    long_col, short_col, spread_col = group_map[group]

    needed = [
        name_col,                
        market_col,
        code_col,
        date_col,
        oi_col,
        long_col,
        short_col,
        spread_col
    ]

    missing = [c for c in needed if c not in df.columns]
    if missing:
        raise KeyError(f"Missing columns for TFF group '{group}': {missing}")

    out = df[needed].rename(columns={
        name_col: "contract_name",   
        market_col: "market",
        code_col: "cftc_code",
        date_col: "date",
        oi_col: "open_interest",
        long_col: "long",
        short_col: "short",
        spread_col: "spreading",
    })

    out["date"] = _to_datetime(out["date"])
    for c in ["open_interest", "long", "short", "spreading"]:
        out[c] = _to_numeric(out[c])

    out["net"] = out["long"] - out["short"]

    oi = out["open_interest"]
    out["pct_oi_net"] = out["net"] / oi.where(oi > 0)
    out["pct_oi_long"] = out["long"] / oi.where(oi > 0)
    out["pct_oi_short"] = out["short"] / oi.where(oi > 0)

    out["dataset"] = "TFF"
    out["group"] = "leveraged_funds"

    out = (
        out
        .sort_values(["cftc_code", "date"])
        .drop_duplicates(["cftc_code", "date"], keep="last")
    )

    return out.reset_index(drop=True)


def standardize_dis_managed_money(df_raw: pd.DataFrame) -> pd.DataFrame:
    df = df_raw.copy()

    date_col = "report_date_as_yyyy_mm_dd"
    oi_col = "open_interest_all"
    market_col = "market_and_exchange_names"
    code_col = "cftc_contract_market_code"
    name_col = "contract_market_name"   

    long_col = "m_money_positions_long_all"
    short_col = "m_money_positions_short_all"
    spread_col = "m_money_positions_spread"

    needed = [
        name_col,              
        market_col,
        code_col,
        date_col,
        oi_col,
        long_col,
        short_col,
        spread_col
    ]

    missing = [c for c in needed if c not in df.columns]
    if missing:
        raise KeyError(f"Missing columns for DIS Managed Money: {missing}")

    out = df[needed].rename(columns={
        name_col: "contract_name",  
        market_col: "market",
        code_col: "cftc_code",
        date_col: "date",
        oi_col: "open_interest",
        long_col: "long",
        short_col: "short",
        spread_col: "spreading",
    })

    out["date"] = _to_datetime(out["date"])
    for c in ["open_interest", "long", "short", "spreading"]:
        out[c] = _to_numeric(out[c])

    out["net"] = out["long"] - out["short"]

    oi = out["open_interest"]
    out["pct_oi_net"] = out["net"] / oi.where(oi > 0)
    out["pct_oi_long"] = out["long"] / oi.where(oi > 0)
    out["pct_oi_short"] = out["short"] / oi.where(oi > 0)

    out["dataset"] = "DIS"
    out["group"] = "managed_money"

    out = (
        out
        .sort_values(["cftc_code", "date"])
        .drop_duplicates(["cftc_code", "date"], keep="last")
    )

    return out.reset_index(drop=True)
