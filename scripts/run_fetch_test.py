
# run_fetch_test.py
import os
import pandas as pd

from src.cot.fetch import download_tff_by_codes, save_raw

UNIVERSE_PATHS = [
    "data/processed/tff_universe_raw.parquet",
    "data/raw/tff_universe_raw.parquet",
]

RAW_OUT_PATH = "data/raw/tff_raw.parquet"  # full-history TFF raw

# limit columns to speed up download
TFF_SELECT = ",".join([
    "report_date_as_yyyy_mm_dd",
    "cftc_contract_market_code",
    "market_and_exchange_names",
    "contract_market_name",
    "open_interest_all",
    "lev_money_positions_long",
    "lev_money_positions_short",
    "lev_money_positions_spread",
])


def _first_existing(paths: list[str]) -> str:
    for p in paths:
        if os.path.exists(p):
            return p
    raise FileNotFoundError(f"Could not find universe parquet. Tried: {paths}")


if __name__ == "__main__":
    universe_path = _first_existing(UNIVERSE_PATHS)
    u = pd.read_parquet(universe_path)

    if "cftc_contract_market_code" not in u.columns:
        raise KeyError(
            "Universe parquet must contain 'cftc_contract_market_code'. "
            f"Found columns: {list(u.columns)}"
        )

    codes = (
        u["cftc_contract_market_code"]
        .dropna()
        .astype(str)
        .unique()
        .tolist()
    )
    codes = sorted(codes)

    print(f"Universe: {universe_path}")
    print(f"Codes to fetch: {len(codes)}")

    # Fetch full history by code (stable across name changes)
    df_tff_raw = download_tff_by_codes(
        codes=codes,
        select=TFF_SELECT,
        chunk_size=50000,
        in_clause_batch=50,
    )

    print("Downloaded rows:", len(df_tff_raw))

    # Save
    save_raw(df_tff_raw, RAW_OUT_PATH)
    print("Saved:", RAW_OUT_PATH)

    # Quick sanity check: min/max dates
    d = pd.to_datetime(df_tff_raw["report_date_as_yyyy_mm_dd"], errors="coerce")
    print("Min report date:", d.min())
    print("Max report date:", d.max())
