import pandas as pd

TFF = "data/processed/tff_levmoney_tidy.parquet"
DIS = "data/processed/dis_managed_money_tidy.parquet"
OUT = "data/processed/cot_tidy.parquet"

ASSET_CLASS_MAP = {
    "Rates": [
        "FED FUNDS",
        "EURODOLLARS-3M",
        "UST 2Y NOTE",
        "UST 5Y NOTE",
        "UST 10Y NOTE",
        "UST BOND",
        "ULTRA UST BOND",
        "ULTRA UST 10Y",
        "MICRO 10 YEAR YIELD",
        "SOFR-1M",
        "SOFR-3M",
        "EURO SHORT TERM RATE",
    ],
    "FX": [
        "EURO FX",
        "JAPANESE YEN",
        "BRITISH POUND",
        "SWISS FRANC",
        "AUSTRALIAN DOLLAR",
        "CANADIAN DOLLAR",
        "NZ DOLLAR",
        "MEXICAN PESO",
        "USD INDEX",
        "BRAZILIAN REAL",
        "SO AFRICAN RAND",
    ],
    "Equities": [
        "E-MINI S&P 500",
        "NASDAQ MINI",
        "NIKKEI STOCK AVERAGE",
        "DJIA x $5",
        "NIKKEI STOCK AVERAGE YEN DENOM",
        "VIX FUTURES",
        "MSCI EAFE",
        "MSCI EM INDEX",
    ],
    "Commodities": [
        "GOLD",
        "SILVER",
        "GASOLINE RBOB",
        "CRUDE OIL, LIGHT SWEET-WTI",
        "WTI-PHYSICAL",
        "COPPER- #1",
        "PALLADIUM",
        "PLATINUM",
        "CORN",
        "OATS",
        "COCOA",
        "COFFEE C",
        "SUGAR NO. 11",
        "SOYBEANS",
    ],
    "Crypto": [
        "BITCOIN",
        "DOGECOIN",
        "SOL",
    ],
}


def infer_asset_class_from_market(market: str) -> str:
    """
    market looks like: 'CORN - CHICAGO BOARD OF TRADE'
    We match using the base name before ' - '.
    """
    base = market.split(" - ")[0].strip()
    for cls, names in ASSET_CLASS_MAP.items():
        if base in names:
            return cls
    return "Other"


if __name__ == "__main__":
    df_tff = pd.read_parquet(TFF)
    df_dis = pd.read_parquet(DIS)

    df = pd.concat([df_tff, df_dis], ignore_index=True)

    # Add asset class label used by Streamlit filtering
    df["asset_class"] = df["market"].map(infer_asset_class_from_market)

    df.to_parquet(OUT, index=False)

    print("combined shape:", df.shape)
    print("asset_class counts:\n", df["asset_class"].value_counts(dropna=False))
    print("saved:", OUT)
