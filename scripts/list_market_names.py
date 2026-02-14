# scripts/list_market_names.py
from src.cot.fetch import soda_download_all
from src.cot.config import BASE_TFF

df = soda_download_all(
    base_url=BASE_TFF,
    select="distinct market_and_exchange_names",
    chunk_size=50000,
)
print("count:", len(df))
print(df.head(50).to_string(index=False))
