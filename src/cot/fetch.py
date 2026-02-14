### create:
### a generic downloader (soda_download_all)
### a universe downloader specifically for TFF (download_universe_tff)
### raw parquet writer/reader (save_raw, load_raw)

from __future__ import annotations

import os
import time
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd
import requests

from src.cot.config import BASE_TFF


def soda_get(base_url: str, params: Dict[str, Any], timeout: int = 60) -> List[Dict[str, Any]]:
    """
    Single Socrata GET call. Returns list[dict] rows.
    Supports optional app token via env var SODA_APP_TOKEN.
    """
    headers = {}
    token = os.getenv("SODA_APP_TOKEN")
    if token:
        headers["X-App-Token"] = token

    r = requests.get(base_url, params=params, headers=headers, timeout=timeout)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list):
        raise ValueError(f"Unexpected response type: {type(data)}")
    return data


def soda_download_all(
    base_url: str,
    where: Optional[str] = None,
    select: Optional[str] = None,
    order: Optional[str] = None,
    chunk_size: int = 50000,
    pause: float = 0.2,
) -> pd.DataFrame:
    """
    Download all rows with paging using $limit/$offset.
    """
    all_rows: List[Dict[str, Any]] = []
    offset = 0

    while True:
        params: Dict[str, Any] = {"$limit": chunk_size, "$offset": offset}
        if where:
            params["$where"] = where
        if select:
            params["$select"] = select
        if order:
            params["$order"] = order

        rows = soda_get(base_url, params=params)
        if not rows:
            break

        all_rows.extend(rows)
        offset += chunk_size
        time.sleep(pause)

    return pd.DataFrame(all_rows)


import re
from difflib import get_close_matches

def _norm_name(x: str) -> str:
    x = x.upper()
    x = re.sub(r"[^A-Z0-9]+", " ", x)   # punctuation -> space
    x = re.sub(r"\s+", " ", x).strip()
    return x

def build_market_name_map(requested, available: list[str]) -> dict[str, str | None]:
    """
    Map requested universe names -> exact API market_and_exchange_names.
    Uses normalization + closest-match fallback.
    """
    avail_norm = {_norm_name(a): a for a in available}
    avail_keys = list(avail_norm.keys())

    mapping: dict[str, str | None] = {}
    for r in requested:
        rn = _norm_name(r)

        # exact match after normalization
        if rn in avail_norm:
            mapping[r] = avail_norm[rn]
            continue

        # closest match fallback
        matches = get_close_matches(rn, avail_keys, n=1, cutoff=0.75)
        mapping[r] = avail_norm[matches[0]] if matches else None

    return mapping


def download_universe_tff(
    market_names: Iterable[str],
    base_url: str = BASE_TFF,
    pause: float = 0.2,
) -> pd.DataFrame:
    """
    For each market name in UNIVERSE, download all TFF rows.
    We filter on `market_and_exchange_names` (TFF field).
    """
    frames = []
    for mkt in market_names:
        where = f"market_and_exchange_names = '{mkt}'"
        df = soda_download_all(
            base_url=base_url,
            where=where,
            order="report_date_as_yyyy_mm_dd asc",
            pause=pause,
        )
        if not df.empty:
            df["__requested_market__"] = mkt  # helpful for debugging mismatches
        frames.append(df)

    out = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    return out


def save_raw(df: pd.DataFrame, path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_parquet(path, index=False)


def load_raw(path: str) -> pd.DataFrame:
    return pd.read_parquet(path)


from src.cot.config import BASE_DIS

def get_distinct_market_names_tff(base_url: str = BASE_TFF) -> list[str]:
    df = soda_download_all(
        base_url=base_url,
        select="distinct market_and_exchange_names",
        chunk_size=50000,
    )
    if "market_and_exchange_names" not in df.columns:
        return []
    return sorted(df["market_and_exchange_names"].dropna().unique().tolist())

def get_distinct_market_names_dis(base_url: str = BASE_DIS) -> list[str]:
    df = soda_download_all(
        base_url=base_url,
        select="distinct market_and_exchange_names",
        chunk_size=50000,
    )
    if "market_and_exchange_names" not in df.columns:
        return []
    return sorted(df["market_and_exchange_names"].dropna().unique().tolist())


from src.cot.config import DIS_MARKET_MAP

def download_universe_dis(markets, base_url=BASE_DIS, pause=0.2):
    frames = []

    for mkt in markets:
        api_name = DIS_MARKET_MAP.get(mkt)

        if api_name is None:
            print(f"[DIS] No mapping for: {mkt}")
            continue

        where = f"market_and_exchange_names = '{api_name}'"
        df = soda_download_all(
            base_url=base_url,
            where=where,
            order="report_date_as_yyyy_mm_dd asc",
            pause=pause,
        )

        if not df.empty:
            df["__requested_market__"] = mkt
            df["__matched_market__"] = api_name

        frames.append(df)

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

def _chunked(xs: list[str], n: int):
    for i in range(0, len(xs), n):
        yield xs[i:i+n]

def download_tff_by_codes(
    codes: list[str],
    select: Optional[str] = None,
    chunk_size: int = 50000,
    in_clause_batch: int = 50,
) -> pd.DataFrame:
    """
    Download TFF rows for a list of CFTC contract market codes using an IN (...) filter.
    This is MUCH more stable than filtering by market_and_exchange_names.
    """
    codes = [str(c) for c in codes if pd.notna(c)]
    if not codes:
        return pd.DataFrame()

    frames: list[pd.DataFrame] = []

    for batch in _chunked(codes, in_clause_batch):
        quoted = ",".join([f"'{c}'" for c in batch])
        where = f"cftc_contract_market_code in ({quoted})"

        df = soda_download_all(
            base_url=BASE_TFF,
            where=where,
            select=select,           # None means pull default/all your columns
            chunk_size=chunk_size,
        )
        if df is not None and len(df) > 0:
            frames.append(df)

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def download_dis_by_codes(
    codes: list[str],
    select: Optional[str] = None,
    chunk_size: int = 50000,
    in_clause_batch: int = 50,
) -> pd.DataFrame:
    """
    Download DIS rows for a list of CFTC contract market codes using an IN (...) filter.
    """
    codes = [str(c) for c in codes if pd.notna(c)]
    if not codes:
        return pd.DataFrame()

    frames: list[pd.DataFrame] = []

    for batch in _chunked(codes, in_clause_batch):
        quoted = ",".join([f"'{c}'" for c in batch])
        where = f"cftc_contract_market_code in ({quoted})"

        df = soda_download_all(
            base_url=BASE_DIS,
            where=where,
            select=select,
            chunk_size=chunk_size,
        )
        if df is not None and len(df) > 0:
            frames.append(df)

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
