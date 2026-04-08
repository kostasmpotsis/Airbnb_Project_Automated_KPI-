# -*- coding: utf-8 -*-
"""
╔══════════════════════════════════════════════════════╗
║   KOSTAS ACROPOLIS STUDIOS — KPI DASHBOARD v3        ║
║   + Date Range Filter · Comparison Tab · st.metric  ║
╚══════════════════════════════════════════════════════╝
    python -m streamlit run airbnb_dashboard_v3.py
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import snowflake.connector
import os
from datetime import date

# ─────────────────────────────────────────────────────
# SNOWFLAKE CREDENTIALS
# ─────────────────────────────────────────────────────
SNOWFLAKE_ACCOUNT  = os.environ.get("SNOWFLAKE_ACCOUNT",  "VFHKJRE-GP30607")
SNOWFLAKE_USER     = os.environ.get("SNOWFLAKE_USER",     "KOSTAS")
SNOWFLAKE_PASSWORD = os.environ.get("SNOWFLAKE_PASSWORD", "Mpotsi!1kostas")
SNOWFLAKE_DB       = "AIRBNB_DB"
SNOWFLAKE_SCHEMA   = "RAW"
SNOWFLAKE_WH       = "AIRBNB_WH"

# ─────────────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────────────
st.set_page_config(
    page_title="Kostas Acropolis Studios · KPI Dashboard",
    page_icon="🏛️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────────────
# GLOBAL CSS
# ─────────────────────────────────────────────────────
st.markdown("""
<style>
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
  html, body, [class*="css"] { font-family: 'Inter', sans-serif; }

  .kpi-card {
      background: #ffffff; border: 1px solid #e8eaf0; border-radius: 14px;
      padding: 1.1rem 1.3rem; box-shadow: 0 2px 8px rgba(0,0,0,0.06);
      text-align: center; margin-bottom: 0.4rem;
  }
  .kpi-card .label { font-size: 0.72rem; font-weight: 500; color: #8a94a6; letter-spacing:.04em; text-transform:uppercase; }
  .kpi-card .value { font-size: 1.65rem; font-weight: 700; color: #1a1f36; margin:.15rem 0 0; }
  .kpi-card .delta { font-size: 0.75rem; margin-top:.1rem; }
  .kpi-card .delta.up   { color: #22c55e; }
  .kpi-card .delta.down { color: #ef4444; }
  .kpi-card .delta.neu  { color: #8a94a6; }

  .sec-title {
      font-size: 1rem; font-weight: 600; color: #1a1f36;
      border-left: 4px solid #FF5A5F; padding-left:.6rem; margin: 1.4rem 0 .7rem;
  }

  /* Comparison card */
  .cmp-card {
      background:#fff; border:1px solid #e8eaf0; border-radius:12px;
      padding:.9rem 1rem; margin-bottom:.5rem; text-align:center;
  }
  .cmp-card .cmp-label { font-size:.7rem; color:#8a94a6; text-transform:uppercase; letter-spacing:.04em; }
  .cmp-card .cmp-cur   { font-size:1.4rem; font-weight:700; color:#1a1f36; }
  .cmp-card .cmp-prev  { font-size:.78rem; color:#8a94a6; margin-top:.1rem; }
  .cmp-card .cmp-up    { color:#22c55e; font-weight:600; }
  .cmp-card .cmp-dn    { color:#ef4444; font-weight:600; }
  .cmp-card .cmp-neu   { color:#8a94a6; font-weight:600; }

  .badge-s1 { background:#FF5A5F22; color:#FF5A5F; border-radius:6px; padding:2px 10px; font-size:.78rem; font-weight:600; }
  .badge-s2 { background:#00A69922; color:#00A699; border-radius:6px; padding:2px 10px; font-size:.78rem; font-weight:600; }

  section[data-testid="stSidebar"] { background: #0f172a !important; }
  section[data-testid="stSidebar"] * { color: #e2e8f0 !important; }

  button[data-baseweb="tab"] { font-size:.85rem !important; font-weight:500 !important; }
  button[data-baseweb="tab"][aria-selected="true"] { border-bottom: 3px solid #FF5A5F !important; }

  hr { border: none; border-top: 1px solid #e8eaf0; margin: 1rem 0; }

  /* st.metric delta colours override */
  [data-testid="stMetricDelta"] svg { display:none; }
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────
MONTH_ORDER = ["Jan","Feb","Mar","Apr","May","Jun",
               "Jul","Aug","Sep","Oct","Nov","Dec"]
MONTH_NUM   = {m: i+1 for i, m in enumerate(MONTH_ORDER)}

S1_COLOR = "#FF5A5F"
S2_COLOR = "#00A699"
ACCENT   = "#3B82F6"

STUDIO_VIEWS = {
    "Studio 1": {
        "booked": "BOOKED_DAYS_VIEW_FINAL_1",
        "cancel": "CANCELATION_RATE_1",
        "lead":   "KOSTAS_1_KPI_LEAD_TIME",
        "alos":   "ALOS_METRICS_VIEW_1",
        "color":  S1_COLOR,
    },
    "Studio 2": {
        "booked": "BOOKED_DAYS_VIEW_FINAL_2",
        "cancel": "CANCELATION_RATE_2",
        "lead":   "KOSTAS_2_KPI_LEAD_TIME",
        "alos":   "ALOS_METRICS_VIEW_2",
        "color":  S2_COLOR,
    },
}

# ─────────────────────────────────────────────────────
# SNOWFLAKE
# ─────────────────────────────────────────────────────
@st.cache_resource(show_spinner=False)
def get_conn():
    return snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT, user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD, database=SNOWFLAKE_DB,
        schema=SNOWFLAKE_SCHEMA, warehouse=SNOWFLAKE_WH,
    )

@st.cache_data(ttl=300, show_spinner=False)
def query(view: str) -> pd.DataFrame:
    df = pd.read_sql(f"SELECT * FROM AIRBNB_DB.RAW.{view}", get_conn())
    df.columns = [c.upper() for c in df.columns]
    return df

# ─────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────
def sort_by_month(df: pd.DataFrame, col="MONTH") -> pd.DataFrame:
    if col in df.columns:
        df = df.copy()
        df["_sort"] = pd.Categorical(df[col], categories=MONTH_ORDER, ordered=True)
        df = df.sort_values("_sort").drop(columns="_sort").reset_index(drop=True)
    return df

def kpi_card(label, value, delta_text="", delta_dir="neu"):
    delta_html = f'<div class="delta {delta_dir}">{delta_text}</div>' if delta_text else ""
    return f"""<div class="kpi-card">
      <div class="label">{label}</div>
      <div class="value">{value}</div>
      {delta_html}
    </div>"""

def plot_cfg(fig, h=340):
    fig.update_layout(
        height=h, plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter", size=12), margin=dict(l=10, r=10, t=36, b=10),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    fig.update_xaxes(showgrid=False, linecolor="#e8eaf0")
    fig.update_yaxes(gridcolor="#f1f5f9", zeroline=False)
    return fig

def color_map():
    return {"Studio 1": S1_COLOR, "Studio 2": S2_COLOR}

def pct_delta(cur, prev):
    """Returns (pct_string, direction) for st.metric delta."""
    if prev and prev != 0:
        pct = (cur - prev) / abs(prev) * 100
        sign = "+" if pct >= 0 else ""
        return f"{sign}{pct:.1f}%", pct
    return "N/A", 0

def add_month_num(df: pd.DataFrame) -> pd.DataFrame:
    """Adds _MONTH_NUM integer column from MONTH string."""
    df = df.copy()
    if "MONTH" in df.columns:
        df["_MONTH_NUM"] = df["MONTH"].map(MONTH_NUM)
    return df

# ─────────────────────────────────────────────────────
# LOAD ALL DATA  (always full, filtering done in-memory)
# ─────────────────────────────────────────────────────
with st.spinner("Σύνδεση στο Snowflake…"):
    try:
        data = {}
        for sname, cfg in STUDIO_VIEWS.items():
            data[sname] = {
                "booked": add_month_num(sort_by_month(query(cfg["booked"]))),
                "cancel": query(cfg["cancel"]),
                "lead":   query(cfg["lead"]),
                "alos":   add_month_num(sort_by_month(query(cfg["alos"]))),
            }
        df_total_revenue = sort_by_month(query("KOSTAS_TOTAL_REVENUE"))
        ok = True
    except Exception as e:
        st.error(f"❌ Σφάλμα σύνδεσης Snowflake: {e}")
        ok = False

if not ok:
    st.stop()

# ─────────────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────────────

# Collect all available months from booked views
all_months_set = set()
for sname in STUDIO_VIEWS:
    df_b = data[sname]["booked"]
    if "MONTH" in df_b.columns:
        all_months_set.update(df_b["MONTH"].dropna().unique())

available_months = [m for m in MONTH_ORDER if m in all_months_set]

with st.sidebar:
    st.markdown("## 🏛️ Kostas Acropolis Studios")
    st.markdown("---")

    studio_choice = st.radio(
        "📍 Επιλογή Ακινήτου",
        options=["Και τα 2 Studios", "Studio 1 μόνο", "Studio 2 μόνο"],
        index=0,
    )
    show1 = studio_choice in ["Και τα 2 Studios", "Studio 1 μόνο"]
    show2 = studio_choice in ["Και τα 2 Studios", "Studio 2 μόνο"]

    st.markdown("---")

    # ── DATE RANGE FILTER ──────────────────
    st.markdown("**📅 Date Range Filter**")
    st.caption("Φίλτρο βάσει μήνα check-in")

    if available_months:
        month_start = st.selectbox(
            "Από Μήνα",
            options=available_months,
            index=0,
        )
        # End month must be >= start month
        valid_end = [m for m in available_months
                     if MONTH_NUM.get(m, 0) >= MONTH_NUM.get(month_start, 0)]
        month_end = st.selectbox(
            "Έως Μήνα",
            options=valid_end,
            index=len(valid_end) - 1,
        )
    else:
        month_start = MONTH_ORDER[0]
        month_end   = MONTH_ORDER[-1]

    sel_start_num = MONTH_NUM.get(month_start, 1)
    sel_end_num   = MONTH_NUM.get(month_end, 12)

    st.markdown("---")
    if st.button("🔄 Ανανέωση δεδομένων", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.markdown("---")
    st.caption(f"Εμφάνιση: **{month_start} → {month_end}**\nSnowflake · AIRBNB_DB.RAW")

# ─────────────────────────────────────────────────────
# FILTER DATA by selected month range
# ─────────────────────────────────────────────────────
def filter_booked(df: pd.DataFrame) -> pd.DataFrame:
    if "_MONTH_NUM" in df.columns:
        return df[(df["_MONTH_NUM"] >= sel_start_num) & (df["_MONTH_NUM"] <= sel_end_num)].copy()
    return df

def filter_cancel(df: pd.DataFrame) -> pd.DataFrame:
    """Cancel views use CUSTOM_MONTH_NAME → map to number."""
    df = df.copy()
    col = "CUSTOM_MONTH_NAME" if "CUSTOM_MONTH_NAME" in df.columns else "MONTH"
    if col in df.columns:
        df["_MN"] = df[col].map(MONTH_NUM)
        df = df[(df["_MN"] >= sel_start_num) & (df["_MN"] <= sel_end_num)].drop(columns="_MN")
    return df

def filter_alos(df: pd.DataFrame) -> pd.DataFrame:
    if "_MONTH_NUM" in df.columns:
        return df[(df["_MONTH_NUM"] >= sel_start_num) & (df["_MONTH_NUM"] <= sel_end_num)].copy()
    return df

active_studios = []
if show1: active_studios.append("Studio 1")
if show2: active_studios.append("Studio 2")

# Filtered versions used throughout
fdata = {}
for s in STUDIO_VIEWS:
    fdata[s] = {
        "booked": filter_booked(data[s]["booked"]),
        "cancel": filter_cancel(data[s]["cancel"]),
        "lead":   data[s]["lead"],   # lead time not month-filtered (uses check-in date)
        "alos":   filter_alos(data[s]["alos"]),
    }

# Filter total revenue too
if "MONTH" in df_total_revenue.columns:
    df_total_revenue["_MN"] = df_total_revenue["MONTH"].map(MONTH_NUM)
    df_tr_filtered = df_total_revenue[
        (df_total_revenue["_MN"] >= sel_start_num) &
        (df_total_revenue["_MN"] <= sel_end_num)
    ].drop(columns="_MN").copy()
else:
    df_tr_filtered = df_total_revenue.copy()

# ─────────────────────────────────────────────────────
# TOP SUMMARY KPI ROW  (with MoM delta using st.metric)
# ─────────────────────────────────────────────────────
def agg_booked(studios, df_dict):
    frames = [df_dict[s]["booked"] for s in studios if not df_dict[s]["booked"].empty]
    if not frames: return {}
    df = pd.concat(frames)
    return {
        "gross":  df["PAYMENT_GROSS"].sum()               if "PAYMENT_GROSS"            in df.columns else 0,
        "net":    df["PAYMENT_NET"].sum()                 if "PAYMENT_NET"              in df.columns else 0,
        "occ":    df["OCCUPANCY_RATE"].mean()             if "OCCUPANCY_RATE"           in df.columns else 0,
        "adr":    df["AVERAGE_DAILY_RATE"].mean()         if "AVERAGE_DAILY_RATE"       in df.columns else 0,
        "revpar": df["REVENUE_PER_AVAILABLE_ROOM"].mean() if "REVENUE_PER_AVAILABLE_ROOM" in df.columns else 0,
        "nights": df["TOTAL_BOOKED_DAYS"].sum()           if "TOTAL_BOOKED_DAYS"        in df.columns else 0,
    }

def agg_alos(studios, df_dict):
    frames = [df_dict[s]["alos"] for s in studios if not df_dict[s]["alos"].empty]
    if not frames: return 0
    df = pd.concat(frames)
    return df["ALOS"].mean() if "ALOS" in df.columns else 0

def agg_cancel(studios, df_dict):
    frames = [df_dict[s]["cancel"] for s in studios if not df_dict[s]["cancel"].empty]
    if not frames: return 0
    df = pd.concat(frames)
    return df["CANCELLATION_RATE"].mean() * 100 if "CANCELLATION_RATE" in df.columns else 0

def agg_lead(studios, df_dict):
    frames = [df_dict[s]["lead"] for s in studios if not df_dict[s]["lead"].empty]
    if not frames: return 0
    df = pd.concat(frames)
    return df["GLOBAL_AVG_LEAD_TIME"].iloc[0] if "GLOBAL_AVG_LEAD_TIME" in df.columns and len(df) > 0 else 0

# Build "previous period" dataset: shift month range back by 1
prev_start = max(1, sel_start_num - 1)
prev_end   = max(1, sel_end_num   - 1)

def filter_booked_range(df, s, e):
    if "_MONTH_NUM" in df.columns:
        return df[(df["_MONTH_NUM"] >= s) & (df["_MONTH_NUM"] <= e)].copy()
    return df

pdata = {}
for s in STUDIO_VIEWS:
    pdata[s] = {
        "booked": filter_booked_range(data[s]["booked"], prev_start, prev_end),
        "cancel": data[s]["cancel"],
        "lead":   data[s]["lead"],
        "alos":   filter_booked_range(data[s]["alos"],   prev_start, prev_end),
    }

t_cur  = agg_booked(active_studios, fdata)
t_prev = agg_booked(active_studios, pdata)
alos_cur  = agg_alos(active_studios, fdata)
alos_prev = agg_alos(active_studios, pdata)
cancel_cur = agg_cancel(active_studios, fdata)
lead_cur   = agg_lead(active_studios, fdata)

# ─────────────────────────────────────────────────────
# HEADER
# ─────────────────────────────────────────────────────
st.markdown("# 🏛️ Kostas Acropolis Studios")
col_badge, col_sub = st.columns([3, 7])
with col_badge:
    if show1: st.markdown('<span class="badge-s1">● Studio 1</span>', unsafe_allow_html=True)
    if show2: st.markdown('<span class="badge-s2">● Studio 2</span>', unsafe_allow_html=True)
with col_sub:
    period_label = f"{month_start} – {month_end}" if month_start != month_end else month_start
    st.markdown(
        f"<span style='color:#8a94a6;font-size:.85rem'>"
        f"Athens Hospitality KPI Dashboard · Period: <strong style='color:#1a1f36'>{period_label}</strong> "
        f"· Real-time data from Snowflake</span>",
        unsafe_allow_html=True,
    )
st.markdown("---")

# ── 8 KPI cards — full numbers, no truncation ──────────
def _delta_html(cur_val, prev_val, invert=False):
    """Returns HTML span with % delta arrow + colour."""
    if not prev_val or prev_val == 0:
        return ""
    pct = (cur_val - prev_val) / abs(prev_val) * 100
    if invert:
        pct = -pct
    colour = "#22c55e" if pct >= 0 else "#ef4444"
    arrow  = "▲" if pct >= 0 else "▼"
    sign   = "+" if pct >= 0 else ""
    return (f'<span style="font-size:.72rem;font-weight:600;color:{colour}">'
            f'{arrow} {sign}{pct:.1f}%</span>')

def _metric_card(icon, label, value_str, delta_html):
    return f"""
    <div style="background:#fff;border:1px solid #e8eaf0;border-radius:12px;
                padding:.75rem .9rem;box-shadow:0 2px 6px rgba(0,0,0,0.05);
                text-align:center;height:100%;">
      <div style="font-size:.68rem;font-weight:500;color:#8a94a6;
                  text-transform:uppercase;letter-spacing:.04em;white-space:nowrap;">
        {icon} {label}
      </div>
      <div style="font-size:1.35rem;font-weight:700;color:#1a1f36;
                  margin:.2rem 0 .15rem;white-space:nowrap;overflow:visible;">
        {value_str}
      </div>
      <div style="min-height:1rem;">{delta_html}</div>
    </div>"""

gross_d  = _delta_html(t_cur.get("gross",0),  t_prev.get("gross",0))
net_d    = _delta_html(t_cur.get("net",0),    t_prev.get("net",0))
nights_d = _delta_html(t_cur.get("nights",0), t_prev.get("nights",0))
occ_d    = _delta_html(t_cur.get("occ",0),    t_prev.get("occ",0))
adr_d    = _delta_html(t_cur.get("adr",0),    t_prev.get("adr",0))
revpar_d = _delta_html(t_cur.get("revpar",0), t_prev.get("revpar",0))
alos_d   = _delta_html(alos_cur, alos_prev)
cancel_d = _delta_html(cancel_cur, 0)   # no prev for cancel (single period)

cards_data = [
    ("💰", "Gross Revenue",  f"€{t_cur.get('gross',0):,.0f}",      gross_d),
    ("💵", "Net Revenue",    f"€{t_cur.get('net',0):,.0f}",        net_d),
    ("🛏️", "Booked Nights",  f"{t_cur.get('nights',0):.0f}",       nights_d),
    ("📊", "Avg Occupancy",  f"{t_cur.get('occ',0):.1f}%",         occ_d),
    ("💲", "ADR",            f"€{t_cur.get('adr',0):,.0f}",        adr_d),
    ("📈", "RevPAR",         f"€{t_cur.get('revpar',0):,.0f}",     revpar_d),
    ("⏱️", "ALOS",           f"{alos_cur:.1f} nights",              alos_d),
    ("❌", "Cancel Rate",    f"{cancel_cur:.1f}%",                  cancel_d),
]

card_cols = st.columns(8)
for col, (icon, label, val, dlt) in zip(card_cols, cards_data):
    col.markdown(_metric_card(icon, label, val, dlt), unsafe_allow_html=True)

st.markdown("---")

# ─────────────────────────────────────────────────────
# TABS
# ─────────────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "📈 Revenue & Occupancy",
    "🆚 Comparison",
    "❌ Cancellations",
    "⏳ Lead Time",
    "🛏️ ALOS",
    "📋 Raw Data",
])

# ════════════════════════════════════════════
# TAB 1 — REVENUE & OCCUPANCY
# ════════════════════════════════════════════
with tab1:
    st.markdown('<div class="sec-title">Monthly Revenue (Gross vs Net)</div>', unsafe_allow_html=True)
    rev_rows = []
    for s in active_studios:
        df = fdata[s]["booked"]
        for _, r in df.iterrows():
            rev_rows.append({"Month": r["MONTH"], "Studio": s,
                             "Gross €": r.get("PAYMENT_GROSS", 0),
                             "Net €":   r.get("PAYMENT_NET", 0)})
    if rev_rows:
        df_r = pd.DataFrame(rev_rows)
        df_r["Month"] = pd.Categorical(df_r["Month"], categories=MONTH_ORDER, ordered=True)
        df_r = df_r.sort_values("Month")
        fig = make_subplots(rows=1, cols=2,
                            subplot_titles=("Gross Revenue per Month", "Net Revenue per Month"),
                            shared_yaxes=True, horizontal_spacing=0.08)
        for s in active_studios:
            clr = color_map()[s]
            sub = df_r[df_r["Studio"] == s]
            fig.add_trace(go.Bar(name=f"{s} Gross", x=sub["Month"], y=sub["Gross €"],
                                 marker_color=clr, opacity=0.9), row=1, col=1)
            fig.add_trace(go.Bar(name=f"{s} Net", x=sub["Month"], y=sub["Net €"],
                                 marker_color=clr, opacity=0.5), row=1, col=2)
        plot_cfg(fig, 420)
        fig.update_layout(margin=dict(l=10, r=10, t=70, b=10))
        for ann in fig.layout.annotations:
            ann.y = 1.06
        st.plotly_chart(fig, use_container_width=True)

    c1, c2 = st.columns(2)
    with c1:
        st.markdown('<div class="sec-title">Occupancy Rate %</div>', unsafe_allow_html=True)
        occ_rows = []
        for s in active_studios:
            df = fdata[s]["booked"]
            for _, r in df.iterrows():
                if "OCCUPANCY_RATE" in df.columns:
                    occ_rows.append({"Month": r["MONTH"], "Studio": s, "Occupancy %": r["OCCUPANCY_RATE"]})
        if occ_rows:
            df_o = pd.DataFrame(occ_rows)
            df_o["Month"] = pd.Categorical(df_o["Month"], categories=MONTH_ORDER, ordered=True)
            df_o = df_o.sort_values("Month")
            fig = px.line(df_o, x="Month", y="Occupancy %", color="Studio",
                          markers=True, color_discrete_map=color_map())
            fig.add_hrect(y0=80, y1=100, fillcolor="#22c55e", opacity=0.05, line_width=0)
            fig.add_hline(y=80, line_dash="dot", line_color="#22c55e",
                          annotation_text="Target 80%", annotation_font_color="#22c55e")
            plot_cfg(fig, 320)
            st.plotly_chart(fig, use_container_width=True)

    with c2:
        st.markdown('<div class="sec-title">ADR vs RevPAR</div>', unsafe_allow_html=True)
        pr_rows = []
        for s in active_studios:
            df = fdata[s]["booked"]
            for _, r in df.iterrows():
                pr_rows.append({"Month": r["MONTH"], "Studio": s,
                                "ADR": r.get("AVERAGE_DAILY_RATE", 0),
                                "RevPAR": r.get("REVENUE_PER_AVAILABLE_ROOM", 0)})
        if pr_rows:
            df_p = pd.DataFrame(pr_rows)
            df_p["Month"] = pd.Categorical(df_p["Month"], categories=MONTH_ORDER, ordered=True)
            df_p = df_p.sort_values("Month")
            fig = go.Figure()
            for s in active_studios:
                clr = color_map()[s]
                sub = df_p[df_p["Studio"] == s]
                fig.add_trace(go.Scatter(name=f"{s} ADR", x=sub["Month"], y=sub["ADR"],
                                         mode="lines+markers", line=dict(color=clr, width=2.5)))
                fig.add_trace(go.Scatter(name=f"{s} RevPAR", x=sub["Month"], y=sub["RevPAR"],
                                         mode="lines+markers", line=dict(color=clr, width=1.5, dash="dot")))
            plot_cfg(fig, 320)
            st.plotly_chart(fig, use_container_width=True)

    st.markdown('<div class="sec-title">Booked Days vs Days in Month</div>', unsafe_allow_html=True)
    bd_rows = []
    for s in active_studios:
        df = fdata[s]["booked"]
        for _, r in df.iterrows():
            bd_rows.append({"Month": r["MONTH"], "Studio": s,
                            "Booked": r.get("TOTAL_BOOKED_DAYS", 0),
                            "Available": r.get("DAYS", 30)})
    if bd_rows:
        df_bd = pd.DataFrame(bd_rows)
        df_bd["Month"] = pd.Categorical(df_bd["Month"], categories=MONTH_ORDER, ordered=True)
        df_bd = df_bd.sort_values("Month")
        booked_color = S1_COLOR if show1 and not show2 else S2_COLOR if show2 and not show1 else ACCENT
        fig = px.bar(df_bd, x="Month", y=["Available","Booked"],
                     barmode="overlay", opacity=0.85,
                     facet_col="Studio" if len(active_studios) > 1 else None,
                     color_discrete_map={"Available": "#e2e8f0", "Booked": booked_color},
                     labels={"value": "Days", "variable": "Type"})
        plot_cfg(fig, 300)
        st.plotly_chart(fig, use_container_width=True)

    st.markdown('<div class="sec-title">📦 Combined Net Revenue — Both Studios (TRevPAR)</div>', unsafe_allow_html=True)
    if not df_tr_filtered.empty and "TOTAL_NET_REVENUE" in df_tr_filtered.columns:
        df_tr = df_tr_filtered.copy()
        if "MONTH" not in df_tr.columns and "CUSTOM_MONTH_NAME" in df_tr.columns:
            df_tr = df_tr.rename(columns={"CUSTOM_MONTH_NAME": "MONTH"})
        if "MONTH" in df_tr.columns:
            df_tr["Month"] = pd.Categorical(df_tr["MONTH"], categories=MONTH_ORDER, ordered=True)
            df_tr = df_tr.sort_values("Month")
        fig_tr = go.Figure()
        fig_tr.add_trace(go.Bar(
            name="Total Net Revenue", x=df_tr.get("Month", df_tr.get("MONTH")),
            y=df_tr["TOTAL_NET_REVENUE"],
            marker=dict(color=df_tr["TOTAL_NET_REVENUE"],
                        colorscale=[[0,"#fca5a5"],[0.5,"#f97316"],[1,"#16a34a"]], showscale=False),
            text=df_tr["TOTAL_NET_REVENUE"].map(lambda v: f"€{v:,.0f}"),
            textposition="outside",
        ))
        if "TREVPAR" in df_tr.columns:
            fig_tr.add_trace(go.Scatter(
                name="TRevPAR", x=df_tr.get("Month", df_tr.get("MONTH")),
                y=df_tr["TREVPAR"], mode="lines+markers",
                line=dict(color="#6366f1", width=2.5), marker=dict(size=7), yaxis="y2",
            ))
        fig_tr.update_layout(
            title="Combined Net Revenue & TRevPAR per Month",
            yaxis=dict(title="Net Revenue (€)", gridcolor="#f1f5f9"),
            yaxis2=dict(title="TRevPAR (€)", overlaying="y", side="right", showgrid=False, zeroline=False),
        )
        plot_cfg(fig_tr, 380)
        fig_tr.update_layout(margin=dict(l=10, r=60, t=60, b=10))
        st.plotly_chart(fig_tr, use_container_width=True)

# ════════════════════════════════════════════
# TAB 2 — COMPARISON (NEW)
# ════════════════════════════════════════════
with tab2:
    st.markdown('<div class="sec-title">Month-over-Month KPI Comparison</div>', unsafe_allow_html=True)
    st.caption(
        "Για κάθε μήνα στο επιλεγμένο range, συγκρίνεται με τον **ακριβώς προηγούμενο μήνα**. "
        "🟢 = βελτίωση · 🔴 = χειροτέρευση"
    )

    # Build month list within filter range
    months_in_range = [m for m in MONTH_ORDER
                       if sel_start_num <= MONTH_NUM[m] <= sel_end_num]

    if len(months_in_range) < 1:
        st.info("Επέλεξε τουλάχιστον 1 μήνα από το sidebar.")
    else:
        KPI_DEFS = [
            ("Payment Gross",   "PAYMENT_GROSS",            "€{:,.0f}", False),
            ("Payment Net",     "PAYMENT_NET",              "€{:,.0f}", False),
            ("Booked Days",     "TOTAL_BOOKED_DAYS",        "{:.0f}",   False),
            ("ADR",             "AVERAGE_DAILY_RATE",       "€{:,.0f}", False),
            ("RevPAR",          "REVENUE_PER_AVAILABLE_ROOM","€{:,.0f}",False),
            ("Occupancy Rate",  "OCCUPANCY_RATE",           "{:.1f}%",  False),
        ]

        # For each active studio build a comparison table
        for s in active_studios:
            st.markdown(f"### {s}")
            df_b = data[s]["booked"]  # use full (unfiltered) data for prev month lookup

            # Month selector inside comparison tab
            selected_month = st.selectbox(
                f"Επέλεξε μήνα για σύγκριση — {s}",
                options=months_in_range,
                index=len(months_in_range) - 1,
                key=f"cmp_month_{s}",
            )
            cur_num  = MONTH_NUM[selected_month]
            prev_num = cur_num - 1

            def get_month_row(month_num):
                rows = df_b[df_b["_MONTH_NUM"] == month_num] if "_MONTH_NUM" in df_b.columns else pd.DataFrame()
                if rows.empty:
                    return None
                return rows.iloc[0]

            row_cur  = get_month_row(cur_num)
            row_prev = get_month_row(prev_num)

            prev_label = MONTH_ORDER[prev_num - 1] if 1 <= prev_num <= 12 else "—"

            if row_cur is None:
                st.warning(f"Δεν υπάρχουν δεδομένα για {selected_month}.")
                continue

            # ── st.metric grid ─────────────────────
            mc = st.columns(len(KPI_DEFS))
            for i, (kpi_label, col_key, fmt, invert) in enumerate(KPI_DEFS):
                cur_val  = float(row_cur[col_key])  if (row_cur  is not None and col_key in row_cur.index)  else 0
                prev_val = float(row_prev[col_key]) if (row_prev is not None and col_key in row_prev.index) else None

                delta_str, pct = pct_delta(cur_val, prev_val) if prev_val is not None else ("No prev data", 0)

                # format display value
                if "€" in fmt:
                    disp = f"€{cur_val:,.0f}"
                elif "%" in fmt:
                    disp = f"{cur_val:.1f}%"
                else:
                    disp = fmt.format(cur_val)

                # delta_color: for most KPIs up=good; occupancy/ADR/RevPAR up=good
                delta_color = "normal"  # green for positive, red for negative (Streamlit default)
                if invert:
                    delta_color = "inverse"

                mc[i].metric(
                    label=kpi_label,
                    value=disp,
                    delta=delta_str if delta_str not in ("N/A", "No prev data") else None,
                    delta_color=delta_color,
                    help=f"vs {prev_label}: {prev_val:.1f}" if prev_val is not None else f"No data for {prev_label}",
                )

            # ── Comparison bar chart for selected month ─
            st.markdown(f"<div class='sec-title' style='margin-top:.6rem'>"
                        f"{selected_month} vs {prev_label} — Side-by-Side</div>",
                        unsafe_allow_html=True)

            chart_kpis   = ["PAYMENT_GROSS","PAYMENT_NET","TOTAL_BOOKED_DAYS","AVERAGE_DAILY_RATE","REVENUE_PER_AVAILABLE_ROOM","OCCUPANCY_RATE"]
            chart_labels = ["Gross €","Net €","Booked Days","ADR €","RevPAR €","Occupancy %"]

            cur_vals  = [float(row_cur[k])  if (row_cur  is not None and k in row_cur.index)  else 0 for k in chart_kpis]
            prev_vals = [float(row_prev[k]) if (row_prev is not None and k in row_prev.index) else 0 for k in chart_kpis]

            fig_cmp = go.Figure()
            fig_cmp.add_trace(go.Bar(
                name=selected_month, x=chart_labels, y=cur_vals,
                marker_color=color_map().get(s, ACCENT), opacity=0.9,
                text=[f"{v:,.1f}" for v in cur_vals], textposition="outside",
            ))
            fig_cmp.add_trace(go.Bar(
                name=prev_label, x=chart_labels, y=prev_vals,
                marker_color="#cbd5e1", opacity=0.85,
                text=[f"{v:,.1f}" for v in prev_vals], textposition="outside",
            ))
            fig_cmp.update_layout(barmode="group", title=f"{s} · {selected_month} vs {prev_label}")
            plot_cfg(fig_cmp, 380)
            st.plotly_chart(fig_cmp, use_container_width=True)

            # ── Trend line across all months for each KPI ─
            st.markdown('<div class="sec-title">Trend — Όλοι οι μήνες</div>', unsafe_allow_html=True)
            trend_kpis = [
                ("PAYMENT_GROSS",            "Payment Gross (€)"),
                ("PAYMENT_NET",              "Payment Net (€)"),
                ("OCCUPANCY_RATE",           "Occupancy Rate (%)"),
                ("AVERAGE_DAILY_RATE",       "ADR (€)"),
                ("REVENUE_PER_AVAILABLE_ROOM","RevPAR (€)"),
                ("TOTAL_BOOKED_DAYS",        "Booked Days"),
            ]
            t_c1, t_c2, t_c3 = st.columns(3)
            t_cols = [t_c1, t_c2, t_c3, t_c1, t_c2, t_c3]

            for ti, (tk, tl) in enumerate(trend_kpis):
                if tk not in df_b.columns: continue
                trend_df = df_b[["MONTH", tk, "_MONTH_NUM"]].dropna().sort_values("_MONTH_NUM")
                if trend_df.empty: continue

                fig_t = go.Figure()
                fig_t.add_trace(go.Scatter(
                    x=trend_df["MONTH"], y=trend_df[tk],
                    mode="lines+markers",
                    line=dict(color=color_map().get(s, ACCENT), width=2),
                    marker=dict(size=7),
                    fill="tozeroy", fillcolor=f"{'rgba(255,90,95,0.08)' if s == 'Studio 1' else 'rgba(0,166,153,0.08)'}",
                    name=tl,
                ))
                # Highlight selected month
                if selected_month in trend_df["MONTH"].values:
                    sel_val = float(trend_df.loc[trend_df["MONTH"] == selected_month, tk].iloc[0])
                    fig_t.add_trace(go.Scatter(
                        x=[selected_month], y=[sel_val],
                        mode="markers", marker=dict(size=12, color="#f59e0b", symbol="star"),
                        name="Selected", showlegend=False,
                    ))
                fig_t.update_layout(title=tl, showlegend=False)
                plot_cfg(fig_t, 230)
                fig_t.update_layout(margin=dict(l=5, r=5, t=36, b=5))
                t_cols[ti].plotly_chart(fig_t, use_container_width=True)

            st.markdown("---")

# ════════════════════════════════════════════
# TAB 3 — CANCELLATIONS
# ════════════════════════════════════════════
with tab3:
    st.markdown('<div class="sec-title">Cancellation Rate per Month</div>', unsafe_allow_html=True)

    cr_rows = []
    for s in active_studios:
        df = fdata[s]["cancel"]
        col = "CUSTOM_MONTH_NAME" if "CUSTOM_MONTH_NAME" in df.columns else "MONTH"
        for _, r in df.iterrows():
            cr_rows.append({
                "Month": r[col], "Studio": s,
                "Cancel Rate %": round(r.get("CANCELLATION_RATE", 0) * 100, 2),
                "Canceled":      int(r.get("TOTAL_CANCELED", 0)),
                "Confirmed":     int(r.get("TOTAL_CONFIRMED", 0)),
            })

    if cr_rows:
        df_cr = pd.DataFrame(cr_rows)
        df_cr["Month"] = pd.Categorical(df_cr["Month"], categories=MONTH_ORDER, ordered=True)
        df_cr = df_cr.sort_values("Month")

        fig = px.bar(df_cr, x="Month", y="Cancel Rate %", color="Studio",
                     barmode="group", color_discrete_map=color_map(), text_auto=".1f")
        fig.add_hline(y=10, line_dash="dot", line_color="#ef4444",
                      annotation_text="Warning 10%", annotation_font_color="#ef4444")
        fig.update_traces(textposition="outside")
        plot_cfg(fig, 360)
        st.plotly_chart(fig, use_container_width=True)

        c1, c2 = st.columns(2)
        for i, s in enumerate(active_studios):
            sub = df_cr[df_cr["Studio"] == s]
            fig2 = go.Figure()
            fig2.add_trace(go.Bar(name="Confirmed", x=sub["Month"], y=sub["Confirmed"],
                                  marker_color="#22c55e", opacity=0.9,
                                  text=sub["Confirmed"], textposition="inside"))
            fig2.add_trace(go.Bar(name="Canceled", x=sub["Month"], y=sub["Canceled"],
                                  marker_color="#ef4444", opacity=0.85,
                                  text=sub["Canceled"], textposition="inside"))
            fig2.update_layout(barmode="stack", title=f"{s} · Bookings Breakdown")
            plot_cfg(fig2, 300)
            [c1, c2][i].plotly_chart(fig2, use_container_width=True)

        st.markdown('<div class="sec-title">Cancellation Summary</div>', unsafe_allow_html=True)
        summ_cols = st.columns(len(active_studios))
        for i, s in enumerate(active_studios):
            sub = df_cr[df_cr["Studio"] == s]
            avg_rate = sub["Cancel Rate %"].mean()
            with summ_cols[i]:
                st.markdown(kpi_card(f"{s} · Avg Cancel Rate", f"{avg_rate:.1f}%",
                                     "⚠ High" if avg_rate > 10 else "✓ OK",
                                     "down" if avg_rate > 10 else "up"),
                            unsafe_allow_html=True)
                st.markdown(kpi_card(f"{s} · Total Canceled",  str(sub["Canceled"].sum())),  unsafe_allow_html=True)
                st.markdown(kpi_card(f"{s} · Total Confirmed", str(sub["Confirmed"].sum())), unsafe_allow_html=True)

# ════════════════════════════════════════════
# TAB 4 — LEAD TIME
# ════════════════════════════════════════════
with tab4:
    st.markdown('<div class="sec-title">Booking Lead Time — How Early Guests Book</div>', unsafe_allow_html=True)
    st.caption("Lead Time = days between booking date and check-in date.")

    lt_cols = st.columns(len(active_studios) * 2)
    ci = 0
    for s in active_studios:
        df_lt = fdata[s]["lead"]
        if df_lt.empty: continue
        g_avg = df_lt["GLOBAL_AVG_LEAD_TIME"].iloc[0] if "GLOBAL_AVG_LEAD_TIME" in df_lt.columns else 0
        med   = df_lt["LEAD_TIME_DAYS"].median()       if "LEAD_TIME_DAYS"       in df_lt.columns else 0
        lt_cols[ci].markdown(kpi_card(f"{s} · Global Avg", f"{g_avg:.0f} days"), unsafe_allow_html=True)
        ci += 1
        lt_cols[ci].markdown(kpi_card(f"{s} · Median",     f"{med:.0f} days"),   unsafe_allow_html=True)
        ci += 1

    hist_rows = []
    for s in active_studios:
        df_lt = fdata[s]["lead"]
        if "LEAD_TIME_DAYS" in df_lt.columns:
            for v in df_lt["LEAD_TIME_DAYS"].dropna():
                hist_rows.append({"Studio": s, "Lead Time (days)": v})
    if hist_rows:
        fig = px.histogram(pd.DataFrame(hist_rows), x="Lead Time (days)",
                           color="Studio", barmode="overlay", nbins=35, opacity=0.75,
                           color_discrete_map=color_map(), title="Distribution of Lead Times")
        fig.add_vline(x=7,  line_dash="dot", line_color="#f59e0b", annotation_text="7d")
        fig.add_vline(x=30, line_dash="dot", line_color="#3b82f6", annotation_text="30d")
        plot_cfg(fig, 360)
        st.plotly_chart(fig, use_container_width=True)

    c1, c2 = st.columns(2)
    for i, s in enumerate(active_studios):
        df_lt = fdata[s]["lead"]
        if "MONTHLY_AVG_LEAD_TIME" not in df_lt.columns: continue
        df_lt2 = df_lt.copy()
        df_lt2["CHECK_IN_DATE_CONVERTED"] = pd.to_datetime(df_lt2["CHECK_IN_DATE_CONVERTED"], errors="coerce")
        df_lt2["Month"] = df_lt2["CHECK_IN_DATE_CONVERTED"].dt.strftime("%b")
        monthly = (df_lt2.groupby("Month")["MONTHLY_AVG_LEAD_TIME"]
                   .mean().reset_index()
                   .rename(columns={"MONTHLY_AVG_LEAD_TIME": "Avg Lead Time (days)"}))
        monthly["Month"] = pd.Categorical(monthly["Month"], categories=MONTH_ORDER, ordered=True)
        monthly = monthly.sort_values("Month")
        fig = px.bar(monthly, x="Month", y="Avg Lead Time (days)",
                     color_discrete_sequence=[color_map()[s]],
                     title=f"{s} · Monthly Avg Lead Time", text_auto=".0f")
        fig.update_traces(textposition="outside")
        plot_cfg(fig, 300)
        [c1, c2][i].plotly_chart(fig, use_container_width=True)

    sc_rows = []
    for s in active_studios:
        df_lt = fdata[s]["lead"]
        if "LEAD_TIME_DAYS" in df_lt.columns and "CHECK_IN_DATE_CONVERTED" in df_lt.columns:
            tmp = df_lt[["CHECK_IN_DATE_CONVERTED","LEAD_TIME_DAYS","GUEST_NAME"]].copy()
            tmp["Studio"] = s
            sc_rows.append(tmp)
    if sc_rows:
        df_sc = pd.concat(sc_rows)
        df_sc["CHECK_IN_DATE_CONVERTED"] = pd.to_datetime(df_sc["CHECK_IN_DATE_CONVERTED"], errors="coerce")
        df_sc = df_sc.dropna(subset=["CHECK_IN_DATE_CONVERTED","LEAD_TIME_DAYS"])
        fig = px.scatter(df_sc, x="CHECK_IN_DATE_CONVERTED", y="LEAD_TIME_DAYS",
                         color="Studio", hover_data=["GUEST_NAME"],
                         color_discrete_map=color_map(), title="Lead Time per Booking",
                         labels={"CHECK_IN_DATE_CONVERTED":"Check-in Date","LEAD_TIME_DAYS":"Lead Time (days)"})
        fig.update_traces(marker=dict(size=8, opacity=0.75))
        plot_cfg(fig, 380)
        st.plotly_chart(fig, use_container_width=True)

# ════════════════════════════════════════════
# TAB 5 — ALOS
# ════════════════════════════════════════════
with tab5:
    st.markdown('<div class="sec-title">ALOS — Average Length of Stay</div>', unsafe_allow_html=True)
    st.caption("ALOS = Total Nights ÷ Total Bookings.")

    alos_metric_cols = st.columns(len(active_studios) * 2)
    ai = 0
    for s in active_studios:
        df_a = fdata[s]["alos"]
        if df_a.empty: continue
        avg_a = df_a["ALOS"].mean()            if "ALOS"           in df_a.columns else 0
        tot_b = df_a["TOTAL_BOOKINGS"].sum()   if "TOTAL_BOOKINGS" in df_a.columns else 0
        alos_metric_cols[ai].markdown(kpi_card(f"{s} · Avg ALOS", f"{avg_a:.1f} nights",
                                               "Above target" if avg_a >= 3 else "Below 3 nights",
                                               "up" if avg_a >= 3 else "down"), unsafe_allow_html=True)
        ai += 1
        alos_metric_cols[ai].markdown(kpi_card(f"{s} · Total Bookings", str(int(tot_b))), unsafe_allow_html=True)
        ai += 1

    alos_rows = []
    for s in active_studios:
        df_a = fdata[s]["alos"]
        if "ALOS" in df_a.columns:
            for _, r in df_a.iterrows():
                alos_rows.append({"Month": r["MONTH"], "Studio": s,
                                  "ALOS": r["ALOS"],
                                  "Bookings": r.get("TOTAL_BOOKINGS", 0),
                                  "Nights": r.get("TOTAL_NIGHTS_CAPPED", 0)})
    if alos_rows:
        df_al = pd.DataFrame(alos_rows)
        df_al["Month"] = pd.Categorical(df_al["Month"], categories=MONTH_ORDER, ordered=True)
        df_al = df_al.sort_values("Month")

        c1, c2 = st.columns(2)
        with c1:
            st.markdown('<div class="sec-title">ALOS Trend</div>', unsafe_allow_html=True)
            fig = px.line(df_al, x="Month", y="ALOS", color="Studio",
                          markers=True, color_discrete_map=color_map())
            fig.add_hline(y=3, line_dash="dot", line_color="#f59e0b",
                          annotation_text="Min target: 3 nights")
            plot_cfg(fig, 320)
            st.plotly_chart(fig, use_container_width=True)
        with c2:
            st.markdown('<div class="sec-title">Total Bookings per Month</div>', unsafe_allow_html=True)
            fig = px.bar(df_al, x="Month", y="Bookings", color="Studio",
                         barmode="group", color_discrete_map=color_map(), text_auto=True)
            fig.update_traces(textposition="outside")
            plot_cfg(fig, 320)
            st.plotly_chart(fig, use_container_width=True)

        st.markdown('<div class="sec-title">Total Nights Capped per Month</div>', unsafe_allow_html=True)
        fig = px.area(df_al, x="Month", y="Nights", color="Studio",
                      color_discrete_map=color_map())
        plot_cfg(fig, 300)
        st.plotly_chart(fig, use_container_width=True)

# ════════════════════════════════════════════
# TAB 6 — RAW DATA
# ════════════════════════════════════════════
with tab6:
    st.markdown('<div class="sec-title">Raw View Data (filtered period)</div>', unsafe_allow_html=True)

    for s in active_studios:
        st.markdown(f"#### {s}")
        r1, r2, r3, r4 = st.tabs(["Revenue / Occupancy","Cancellations","Lead Time","ALOS"])
        with r1: st.dataframe(fdata[s]["booked"], use_container_width=True, hide_index=True)
        with r2: st.dataframe(fdata[s]["cancel"], use_container_width=True, hide_index=True)
        with r3:
            lt_cols_show = [c for c in ["GUEST_NAME","CHECK_IN_DATE_CONVERTED","BOOKING_DATE",
                                         "LEAD_TIME_DAYS","GLOBAL_AVG_LEAD_TIME","MONTHLY_AVG_LEAD_TIME"]
                            if c in fdata[s]["lead"].columns]
            st.dataframe(fdata[s]["lead"][lt_cols_show], use_container_width=True, hide_index=True)
        with r4: st.dataframe(fdata[s]["alos"], use_container_width=True, hide_index=True)

    st.markdown("---")
    st.markdown('<div class="sec-title">Export</div>', unsafe_allow_html=True)
    exp_cols = st.columns(4)
    for i, s in enumerate(active_studios):
        csv1 = fdata[s]["booked"].to_csv(index=False).encode("utf-8")
        exp_cols[i*2].download_button(f"⬇️ {s} Revenue CSV", csv1, f"{s.replace(' ','_')}_revenue.csv", "text/csv")
        csv2 = fdata[s]["alos"].to_csv(index=False).encode("utf-8")
        exp_cols[i*2+1].download_button(f"⬇️ {s} ALOS CSV",    csv2, f"{s.replace(' ','_')}_alos.csv",    "text/csv")

# ─────────────────────────────────────────────────────
# FOOTER
# ─────────────────────────────────────────────────────
st.markdown("---")
st.markdown(
    "<div style='text-align:center;color:#8a94a6;font-size:.78rem'>"
    "🏛️ Kostas Acropolis Studios · KPI Dashboard v3 · "
    "Data: Snowflake AIRBNB_DB.RAW · Built with Streamlit & Plotly"
    "</div>",
    unsafe_allow_html=True,
)
