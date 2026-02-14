# scripts/run_metrics.py
import pandas as pd

from src.cot.metrics import add_position_metrics

TIDY_PATH = "data/processed/cot_tidy.parquet"
METRICS_PATH = "data/processed/cot_metrics.parquet"
SNAPSHOT_PATH = "data/processed/cot_latest_snapshot.parquet"

if __name__ == "__main__":
    df = pd.read_parquet(TIDY_PATH)

    dfm = add_position_metrics(df)
    dfm.to_parquet(METRICS_PATH, index=False)

    # Latest row per (dataset, group, cftc_code)
    latest = (
        dfm.sort_values("date")
           .groupby(["dataset", "group", "cftc_code"], as_index=False)
           .tail(1)
    )

    # A default ranking (you’ll let users change this in Streamlit)
    latest = latest.sort_values("pct_oi_net_pctile_5y", ascending=False)
    latest.to_parquet(SNAPSHOT_PATH, index=False)

    print("metrics saved:", METRICS_PATH, "shape:", dfm.shape)
    print("snapshot saved:", SNAPSHOT_PATH, "shape:", latest.shape)
    print(latest[["dataset","group","market","cftc_code","date",
                  "net","net_pctile_5y","pct_oi_net","pct_oi_net_pctile_5y",
                  "net_chg_1w","pct_oi_net_chg_1w"]].head(10))
