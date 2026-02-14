import pandas as pd
from src.cot.fetch import soda_download_all
from src.cot.config import BASE_TFF

df = soda_download_all(
    base_url=BASE_TFF,
    select="market_and_exchange_names",
    chunk_size=5000,
)
print("cols:", df.columns.tolist())
print(df.head(10))
