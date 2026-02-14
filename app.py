# app.py
import streamlit as st
import pandas as pd
import altair as alt

st.set_page_config(
    page_title="CFTC CoT Dashboard",
    layout="wide"
)

@st.cache_data
def load_data():
    metrics = pd.read_parquet("data/processed/cot_metrics.parquet")
    latest = pd.read_parquet("data/processed/cot_latest_snapshot.parquet")
    return metrics, latest

metrics, latest = load_data()

# --- Ensure types are correct for plotting ---
metrics["date"] = pd.to_datetime(metrics["date"], errors="coerce")
latest["date"] = pd.to_datetime(latest["date"], errors="coerce")

# --- Altair: avoid max_rows issues ---
alt.data_transformers.disable_max_rows()


st.title("CFTC CoT Dashboard")
st.caption(
    "Speculative positioning using Leveraged Funds (financials) "
    "and Managed Money (commodities)"
)

# -------------------------
# Sidebar controls
# -------------------------
st.sidebar.header("Controls")

asset_class = st.sidebar.selectbox(
    "Asset Class",
    options=["FX", "Equities", "Commodities", "Crypto", "Rates"]
)

markets = (
    latest
    .query("asset_class == @asset_class")
    .sort_values("market")["market"]
    .dropna()
    .unique()
)

market = st.sidebar.selectbox("Market", markets)

expression = st.sidebar.selectbox(
    "Expression",
    options=[
        ("Net contracts", "net"),
        ("% of open interest", "pct_oi_net")
    ],
    format_func=lambda x: x[0]
)[1]

score_type = st.sidebar.selectbox(
    "Score type",
    options=["percentile", "z", "minmax"]
)

lookback = st.sidebar.selectbox(
    "Lookback window",
    options=["3y", "5y", "max"]
)

change_horizon = st.sidebar.selectbox(
    "Change horizon",
    options=[
        ("1 week", "1w"),
        ("4 weeks", "4w"),
        ("13 weeks", "13w")
    ],
    format_func=lambda x: x[0]
)[1]

# map UI lookback → metrics suffix used in parquet (common: weeks)
LOOKBACK_SUFFIX = {"3y": "156w", "5y": "260w", "max": "max"}
lb = LOOKBACK_SUFFIX.get(lookback, lookback)

score_col = {
    "percentile": f"{expression}_pctile_{lookback}",
    "z": f"{expression}_z_{lookback}",
    "minmax": f"{expression}_minmax_{lookback}",
}[score_type]

chg_col = f"{expression}_chg_{change_horizon}"

# -------------------------
# Filter data (NOW hist exists)
# -------------------------
row_latest = latest.query(
    "asset_class == @asset_class and market == @market"
)

if row_latest.empty:
    st.error("No latest snapshot available for this selection.")
    st.stop()

row_latest = row_latest.iloc[0]

hist = (
    metrics
    .query("asset_class == @asset_class and market == @market")
    .sort_values("date")
)

if hist.empty:
    st.error("No historical metrics available for this selection.")
    st.stop()

# -------------------------
# Contract name display (NOW safe)
# -------------------------
def short_market_name(s: str) -> str:
    return s.split(" - ")[0].strip()

# show contract name if available (only after you rebuild parquets)
contract_name = row_latest.get("contract_name", None)
if pd.notna(contract_name):
    st.markdown(f"**Contract:** {contract_name}")

# -------------------------
# Current Snapshot KPIs
# -------------------------
st.subheader("Positioning Summary")

row = row_latest  

c1, c2, c3, c4 = st.columns(4)
c1.metric("Score", f"{row[score_col]:.1f}" if pd.notna(row[score_col]) else "—")
c2.metric("Net (contracts)", f"{row['net']:,.0f}" if pd.notna(row["net"]) else "—")
c3.metric(f"Δ {change_horizon}", f"{row[chg_col]:+.0f}" if pd.notna(row[chg_col]) else "—")
c4.metric("%OI Net", f"{row['pct_oi_net']:.2%}" if pd.notna(row["pct_oi_net"]) else "—")

# flags
flags = []
score = row.get(score_col)
chg = row.get(chg_col)

if pd.notna(score):
    if score_type in ["percentile", "minmax"]:
        # 0–100 scale
        if score >= 90:
            flags.append("Extreme long")
        elif score <= 10:
            flags.append("Extreme short")

    elif score_type == "z":
        # z-score scale (std devs from mean)
        if score >= 2:
            flags.append("Extreme long (>= +2σ)")
        elif score <= -2:
            flags.append("Extreme short (<= -2σ)")

if pd.notna(chg) and abs(chg) >= 0.02 and expression == "pct_oi_net":
    flags.append("⚡ Big weekly move (%OI)")

if flags:
    st.info(" | ".join(flags))


drivers_tbl = pd.DataFrame({
    "metric": ["Δ long (1w)", "Δ short (1w)", "Implied Δ net"],
    "value": [
        row["long_chg_1w"],
        row["short_chg_1w"],
        (row["long_chg_1w"] - row["short_chg_1w"]),
    ]
})

st.caption(
    "Driver breakdown highlights whether the weekly shift was driven by fresh long build or short covering.\n\n"
    "Implied Δ net = Δ long − Δ short"
)

st.dataframe(drivers_tbl, use_container_width=True)


# -------------------------
# Chart 1: Positioning & Score Over Time (dual-axis + TradingView-like pan/zoom)
# -------------------------
st.subheader("Positioning & Score Over Time")

level_col = expression  # "net" or "pct_oi_net"
needed_cols = ["date", level_col]
if score_col in hist.columns:
    needed_cols.append(score_col)

base = hist[needed_cols].copy()

# Force correct dtypes
base["date"] = pd.to_datetime(base["date"], errors="coerce")
base[level_col] = pd.to_numeric(base[level_col], errors="coerce")
if score_col in base.columns:
    base[score_col] = pd.to_numeric(base[score_col], errors="coerce")

# Require date + level
base = base.dropna(subset=["date", level_col])

if base.empty:
    st.warning("No time-series data available for this selection.")
else:
    # Build plot series + labels
    if level_col == "pct_oi_net":
        base["level_plot"] = base[level_col] * 100
        level_title = "% OI Net"
        level_fmt = ".2f"
    else:
        base["level_plot"] = base[level_col]
        level_title = "Net (contracts)"
        level_fmt = ",.0f"

    # Make sure Altair doesn't silently choke on long series
    alt.data_transformers.disable_max_rows()

    # Hover tooltip (more sensitive)
    nearest = alt.selection_point(nearest=True, on="mouseover", fields=["date"], empty=False)

    # Base chart
    chart_base = alt.Chart(base)

    # Solid: level (left axis)
    level_line = chart_base.mark_line().encode(
        x=alt.X("date:T", title="Date"),
        y=alt.Y("level_plot:Q", title=level_title),
    )

    layers = [level_line]

    # Dotted: score (right axis) if available
    has_score = (score_col in base.columns) and base[score_col].notna().any()
    if has_score:
        score_line = chart_base.mark_line(strokeDash=[4, 2]).encode(
            x="date:T",
            y=alt.Y(f"{score_col}:Q", title="Score"),
        )
        layers.append(score_line)

    # Tooltip rule
    tooltip_fields = [
        alt.Tooltip("date:T", title="Date"),
        alt.Tooltip("level_plot:Q", title=level_title, format=level_fmt),
    ]
    if has_score:
        tooltip_fields.append(alt.Tooltip(f"{score_col}:Q", title="Score", format=".2f"))

    rule = chart_base.mark_rule().encode(
        x="date:T",
        opacity=alt.condition(nearest, alt.value(0.35), alt.value(0)),
        tooltip=tooltip_fields,
    ).add_params(nearest)

    # Combine layers (dual-axis) + enable TradingView-like pan/zoom
    chart = (
        alt.layer(*layers, rule)
        .resolve_scale(y="independent")
        .properties(height=320)
        # key line: pan/zoom on x-axis (scroll to zoom, drag to pan)
        .interactive(bind_y=False)
    )

      # Legend + short interpretation
    st.caption(
        f"Legend — Solid: {level_title} | "
        f"Dotted: {score_type} score ({lookback} lookback)"
        if has_score else
        f"Legend — Solid: {level_title} (score not available for this selection)"
    )

    st.caption(
        "Solid line shows how positioning is building/unwinding over time. "
        "Dotted score standardises today’s positioning vs its own history, helping to spot crowded extremes."
        if has_score else
        "Solid line shows how positioning is building/unwinding over time."
    )


    st.altair_chart(chart, use_container_width=True)



# -------------------------
# Chart 2: Weekly Change Decomposition
# -------------------------
st.subheader("Weekly Change Decomposition")

st.caption(
    "Breaks down weekly change in net positioning into long build and short covering, "
    "helping distinguish fresh directional conviction from mechanical short covering.\n\n"
    "Net ≈ ΔLong − ΔShort"
)

drivers = pd.DataFrame({
    "Component": ["Longs", "Shorts"],
    "Change": [
        row_latest["long_chg_1w"],
        -row_latest["short_chg_1w"]
    ]
})
drivers.loc[len(drivers)] = ["Net", drivers["Change"].sum()]

st.bar_chart(drivers.set_index("Component"))


# -------------------------
# Chart 3: Cross-asset scatter
# -------------------------
st.subheader("Cross-Asset Positioning Map")

st.caption(
    "Each dot represents one market. \n\n"
    "X-axis shows how extreme current positioning is relative to its history "
    f"({score_type} score, {lookback} lookback). "
    "Higher values = more crowded long; lower values = more crowded short. \n\n"
    "Y-axis shows recent positioning momentum "
    f"(Δ {change_horizon}). "
    "Above zero = positioning is building; below zero = positioning is unwinding. \n\n"
    "Top-right = crowded and still building; "
    "top-left = extreme but reversing; "
    "bottom-right = crowded and unwinding; "
    "bottom-left = depressed and still selling."
)


scatter_df = latest.query("asset_class == @asset_class")

scatter = scatter_df[
    ["market", score_col, chg_col]
].dropna()

chart = alt.Chart(scatter).mark_circle(size=120).encode(
    x=alt.X(f"{score_col}:Q", title=score_col),
    y=alt.Y(f"{chg_col}:Q", title=chg_col),
    tooltip=[
        alt.Tooltip("market:N", title="Market"),
        alt.Tooltip(f"{score_col}:Q", title=score_col, format=".2f"),
        alt.Tooltip(f"{chg_col}:Q", title=chg_col, format=",.0f"),
    ]
).interactive()

st.altair_chart(chart, use_container_width=True)

# -------------------------
# Screener
# -------------------------
st.subheader("Cross-Asset Screener")

screener_cols = ["market", "date", "net", "pct_oi_net", score_col, chg_col]

tbl = (
    latest.query("asset_class == @asset_class")[screener_cols]
    .dropna(subset=[score_col])
    .copy()
)

sort_mode = st.sidebar.radio(
    "Screener sort",
    options=["Most extreme", "Biggest change"],
)

if sort_mode == "Most extreme":
    tbl = tbl.sort_values(score_col, ascending=False)
else:
    tbl = tbl.sort_values(chg_col, ascending=False, key=lambda s: s.abs())

# -------------------------
# Clean formatting
# -------------------------

# Convert %OI to percentage
if "pct_oi_net" in tbl.columns:
    tbl["pct_oi_net"] = tbl["pct_oi_net"] * 100

# Round numeric columns nicely
round_cols = [score_col, "pct_oi_net"]
for col in round_cols:
    if col in tbl.columns:
        tbl[col] = tbl[col].round(2)

# Rename for clean display
tbl = tbl.rename(columns={
    "market": "Market",
    "date": "Date",
    "net": "Net (contracts)",
    "pct_oi_net": "%OI Net",
    score_col: "Score",
    chg_col: "Δ 1w"
})

# Reset index and add Rank column
tbl = tbl.reset_index(drop=True)
tbl.insert(0, "Rank", tbl.index + 1)

#caption
st.caption(
    "This table complements the Cross-Asset Positioning Map by providing precise values "
    "behind each plotted point for deeper comparison and ranking."
)

# Display
st.dataframe(tbl, use_container_width=True, hide_index=True)

# footer
max_date = pd.to_datetime(latest["date"]).max()
st.caption(f"As-of report date: {max_date.date()} (positions as of Tuesday; published Friday)")
