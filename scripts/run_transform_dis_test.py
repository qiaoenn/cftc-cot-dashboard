from src.cot.fetch import load_raw
from src.cot.transform import standardize_dis_managed_money

RAW_PATH = "data/raw/dis_universe_raw.parquet"
OUT_PATH = "data/processed/dis_managed_money_tidy.parquet"

if __name__ == "__main__":
    df_raw = load_raw(RAW_PATH)
    df_tidy = standardize_dis_managed_money(df_raw)

    print("tidy shape:", df_tidy.shape)
    print(df_tidy.head())
    df_tidy.to_parquet(OUT_PATH, index=False)
    print("saved:", OUT_PATH)
