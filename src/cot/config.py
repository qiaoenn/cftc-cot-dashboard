# src/cot/config.py
from __future__ import annotations


# CFTC API endpoints


# Traders in Financial Futures (rates, FX, equities, crypto)
BASE_TFF = "https://publicreporting.cftc.gov/resource/gpe5-46if.json"

# Disaggregated (physical commodities)
BASE_DIS = "https://publicreporting.cftc.gov/resource/72hh-3qpy.json"


# TFF universe (financial futures)


UNIVERSE_TFF = {

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

    "Crypto": [
        "BITCOIN",
        "DOGECOIN",
        "SOL",
    ],
}


# DIS universe (commodities)


UNIVERSE_DIS = {

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
    ]
}

# =========================
# Explicit DIS market mapping
# =========================
# Disaggregated COT uses verbose exchange-qualified names.
# We map our short universe names to the official CFTC names.

DIS_MARKET_MAP = {
    "GOLD": "GOLD - COMMODITY EXCHANGE INC.",
    "SILVER": "SILVER - COMMODITY EXCHANGE INC.",
    "GASOLINE RBOB": "GASOLINE RBOB - NEW YORK MERCANTILE EXCHANGE",
    "CRUDE OIL, LIGHT SWEET-WTI": "CRUDE OIL, LIGHT SWEET - NEW YORK MERCANTILE EXCHANGE",
    "WTI-PHYSICAL": "WTI CRUDE OIL - PHYSICAL",
    "COPPER- #1": "COPPER GRADE #1 - COMMODITY EXCHANGE INC.",
    "PALLADIUM": "PALLADIUM - NEW YORK MERCANTILE EXCHANGE",
    "PLATINUM": "PLATINUM - NEW YORK MERCANTILE EXCHANGE",
    "CORN": "CORN - CHICAGO BOARD OF TRADE",
    "OATS": "OATS - CHICAGO BOARD OF TRADE",
    "COCOA": "COCOA - ICE FUTURES U.S.",
    "COFFEE C": "COFFEE C - ICE FUTURES U.S.",
    "SUGAR NO. 11": "SUGAR NO. 11 - ICE FUTURES U.S.",
    "SOYBEANS": "SOYBEANS - CHICAGO BOARD OF TRADE",
}



# Helpers

def flatten_universe(universe: dict[str, list[str]]) -> list[str]:
    """
    Flatten a {category: [contracts]} universe into a unique list of contract names.
    Order is preserved.
    """
    out: list[str] = []
    for _, items in universe.items():
        out.extend(items)

    # dedupe while preserving order
    seen = set()
    uniq = []
    for x in out:
        if x not in seen:
            uniq.append(x)
            seen.add(x)

    return uniq


