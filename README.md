# CFTC CoT Positioning Dashboard

A Streamlit dashboard that visualizes CFTC Commitments of Traders (CoT) positioning across asset classes (Rates, FX, Equities, Commodities, Crypto).  
It focuses on speculative positioning:
- **Leveraged Funds** for financial futures (TFF)
- **Managed Money** for commodity futures (Disaggregated)

## What this dashboard does

### 1) Positioning & Score Over Time
Plots a positioning series (choose **Net contracts** or **% of open interest**) over time and overlays a “crowdedness/extremeness” score (choose **percentile**, **z-score**, or **min-max oscillator**) computed over a selectable lookback window (3y / 5y / max).

### 2) Weekly Change Decomposition
Breaks weekly net positioning change into components:
- Long build
- Short covering/build
Helps distinguish whether changes are driven by fresh long demand vs short covering.

### 3) Cross-Asset Positioning Map
Scatter plot for the selected asset class:
- X-axis: how extreme positioning is (score)
- Y-axis: how quickly positioning is changing (Δ over 1w/4w/13w)
Used to spot crowded + still-building trades vs crowded + unwinding trades.

### 4) Cross-Asset Screener
A ranked table that complements the scatter by showing exact values (market, net, %OI net, score, change).

---

## Data sources
- CFTC CoT data pulled from CFTC’s public endpoints.
- Two datasets are used:
  - **TFF (Traders in Financial Futures)** for financial futures (Leveraged Funds)
  - **Disaggregated (commodities)** for commodity futures (Managed Money)

---

## Project structure
cftc-cot-dashboard/
app.py
requirements.txt
runtime.txt (optional)
data/
raw/
processed/
scripts/
run_fetch_test.py
run_transform.py
run_metrics.py
run_snapshot.py
src/
cot/
fetch.py
transform.py
metrics.py

---

## Setup (local)

### 1) Create + activate virtual environment
```bash
python -m venv .venv
source .venv/bin/activate
```

### 2) Install dependencies
```bash
pip install -r requirements.txt
```

### 3) Run the pipeline
Fetch raw data:

```bash
python -m scripts.run_fetch_test
```

Transform into tidy format:
```bash
python -m scripts.run_transform
```

Compute metrics (scores + changes):
```bash
python -m scripts.run_metrics
```

Build latest snapshot:
```bash
python -m scripts.run_snapshot
```

### 4) Launch the app
```bash
streamlit run app.py
```

### Deployment (Streamlit Community Cloud)
1. Push this repo to GitHub.
2. Ensure you have:
- app.py at repo root (or point Streamlit to the correct path)
- a minimal requirements.txt
- optional runtime.txt for Python version pinning
3. In Streamlit Community Cloud:
- Choose your GitHub repo
- Set the main file path to app.py
- Deploy
