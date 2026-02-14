from src.cot.config import flatten_universe, UNIVERSE_DIS
from src.cot.fetch import download_universe_dis, save_raw

RAW_PATH = "data/raw/dis_universe_raw.parquet"

if __name__ == "__main__":
    markets = flatten_universe(UNIVERSE_DIS)
    df_raw = download_universe_dis(markets)
    print("raw shape:", df_raw.shape)
    print("columns:", df_raw.columns.tolist()[:20])
    save_raw(df_raw, RAW_PATH)
    print("saved to:", RAW_PATH)
