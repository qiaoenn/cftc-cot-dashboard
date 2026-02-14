
from src.cot.fetch import load_raw
from src.cot.transform import standardize_tff_group

RAW_PATH = "data/raw/tff_universe_raw.parquet"
OUT_PATH = "data/processed/tff_levmoney_tidy.parquet"

if __name__ == "__main__":
    df_raw = load_raw(RAW_PATH)
    df_tidy = standardize_tff_group(df_raw, group="lev_money")

    print("tidy shape:", df_tidy.shape)
    print(df_tidy.head())
    df_tidy.to_parquet(OUT_PATH, index=False)
    print("saved:", OUT_PATH)
