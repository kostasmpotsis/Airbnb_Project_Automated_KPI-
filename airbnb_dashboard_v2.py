# -*- coding: utf-8 -*-
"""
╔══════════════════════════════════════════════════════╗
║   KOSTAS ACROPOLIS STUDIOS — KPI DASHBOARD v2        ║
║   Hospitality Analytics · Powered by Snowflake       ║
╚══════════════════════════════════════════════════════╝

Εκτέλεση:
    python -m streamlit run airbnb_dashboard_v2.py
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import snowflake.connector
import os

# ─────────────────────────────────────────────────────
# SNOWFLAKE CREDENTIALS
# Tip: βάλτα σε env variables για ασφάλεια:
#   set SNOWFLAKE_PASSWORD=yourpassword
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
  /* ── Fonts & base ── */
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
  html, body, [class*="css"] { font-family: 'Inter', sans-serif; }

  /* ── KPI card ── */
  .kpi-card {
      background: #ffffff;
      border: 1px solid #e8eaf0;
      border-radius: 14px;
      padding: 1.1rem 1.3rem;
      box-shadow: 0 2px 8px rgba(0,0,0,0.06);
      text-align: center;
      margin-bottom: 0.4rem;
  }
  .kpi-card .label  { font-size: 0.72rem; font-weight: 500; color: #8a94a6; letter-spacing: .04em; text-transform: uppercase; }
  .kpi-card .value  { font-size: 1.65rem; font-weight: 700; color: #1a1f36; margin: .15rem 0 0; }
  .kpi-card .delta  { font-size: 0.75rem; margin-top: .1rem; }
  .kpi-card .delta.up   { color: #22c55e; }
  .kpi-card .delta.down { color: #ef4444; }
  .kpi-card .delta.neu  { color: #8a94a6; }

  /* ── Section title ── */
  .sec-title {
      font-size: 1rem; font-weight: 600; color: #1a1f36;
      border-left: 4px solid #FF5A5F;
      padding-left: .6rem; margin: 1.4rem 0 .7rem;
  }

  /* ── Studio badge ── */
  .badge-s1 { background:#FF5A5F22; color:#FF5A5F; border-radius:6px; padding:2px 10px; font-size:.78rem; font-weight:600; }
  .badge-s2 { background:#00A69922; color:#00A699; border-radius:6px; padding:2px 10px; font-size:.78rem; font-weight:600; }

  /* ── Sidebar ── */
  section[data-testid="stSidebar"] { background: #0f172a !important; }
  section[data-testid="stSidebar"] * { color: #e2e8f0 !important; }
  section[data-testid="stSidebar"] .stSelectbox label,
  section[data-testid="stSidebar"] .stRadio label { color: #94a3b8 !important; font-size:.8rem; }

  /* ── Tab styling ── */
  button[data-baseweb="tab"] { font-size:.85rem !important; font-weight:500 !important; }
  button[data-baseweb="tab"][aria-selected="true"] { border-bottom: 3px solid #FF5A5F !important; }

  /* ── Divider ── */
  hr { border: none; border-top: 1px solid #e8eaf0; margin: 1rem 0; }
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────
MONTH_ORDER = ["Jan","Feb","Mar","Apr","May","Jun",
               "Jul","Aug","Sep","Oct","Nov","Dec"]

S1_COLOR = "#FF5A5F"
S2_COLOR = "#00A699"
ACCENT   = "#3B82F6"

STUDIO_VIEWS = {
    "Studio 1": {
        "booked":   "BOOKED_DAYS_VIEW_FINAL_1",
        "cancel":   "CANCELATION_RATE_1",
        "lead":     "KOSTAS_1_KPI_LEAD_TIME",
        "alos":     "ALOS_METRICS_VIEW_1",
        "color":    S1_COLOR,
        "label":    "KOSTAS ACROPOLIS STUDIO 1",
    },
    "Studio 2": {
        "booked":   "BOOKED_DAYS_VIEW_FINAL_2",
        "cancel":   "CANCELATION_RATE_2",
        "lead":     "KOSTAS_2_KPI_LEAD_TIME",
        "alos":     "ALOS_METRICS_VIEW_2",
        "color":    S2_COLOR,
        "label":    "KOSTAS ACROPOLIS STUDIO 2",
    },
}

# ─────────────────────────────────────────────────────
# SNOWFLAKE  (cached connection + queries)
# ─────────────────────────────────────────────────────
@st.cache_resource(show_spinner=False)
def get_conn():
    return snowflake.connector.connect(
        account   = SNOWFLAKE_ACCOUNT,
        user      = SNOWFLAKE_USER,
        password  = SNOWFLAKE_PASSWORD,
        database  = SNOWFLAKE_DB,
        schema    = SNOWFLAKE_SCHEMA,
        warehouse = SNOWFLAKE_WH,
    )

@st.cache_data(ttl=300, show_spinner=False)
def query(view: str) -> pd.DataFrame:
    df = pd.read_sql(f"SELECT * FROM AIRBNB_DB.RAW.{view}", get_conn())
    df.columns = [c.upper() for c in df.columns]
    return df

def sort_by_month(df: pd.DataFrame, col="MONTH") -> pd.DataFrame:
    if col in df.columns:
        df = df.copy()
        df["_sort"] = pd.Categorical(df[col], categories=MONTH_ORDER, ordered=True)
        df = df.sort_values("_sort").drop(columns="_sort").reset_index(drop=True)
    return df

# ─────────────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────────────
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
    st.markdown("**📊 Data Sources**")
    for v in ["BOOKED_DAYS_VIEW_FINAL", "CANCELATION_RATE",
              "KPI_LEAD_TIME", "ALOS_METRICS_VIEW"]:
        st.markdown(f"<span style='color:#64748b;font-size:.78rem'>✓ {v}</span>", unsafe_allow_html=True)

    st.markdown("---")
    if st.button("🔄 Ανανέωση δεδομένων", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.markdown("---")
    st.caption("Snowflake · AIRBNB_DB.RAW\nRefresh κάθε 5 λεπτά")

# ─────────────────────────────────────────────────────
# LOAD DATA
# ─────────────────────────────────────────────────────
with st.spinner("Σύνδεση στο Snowflake…"):
    try:
        data = {}
        for sname, cfg in STUDIO_VIEWS.items():
            data[sname] = {
                "booked": sort_by_month(query(cfg["booked"])),
                "cancel": query(cfg["cancel"]),
                "lead":   query(cfg["lead"]),
                "alos":   sort_by_month(query(cfg["alos"])),
            }
        # Total combined revenue view
        df_total_revenue = sort_by_month(query("KOSTAS_TOTAL_REVENUE"))
        ok = True
    except Exception as e:
        st.error(f"❌ Σφάλμα σύνδεσης Snowflake: {e}")
        ok = False

if not ok:
    st.stop()

active_studios = []
if show1: active_studios.append("Studio 1")
if show2: active_studios.append("Studio 2")

# ─────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────
def kpi_card(label, value, delta_text="", delta_dir="neu"):
    delta_html = f'<div class="delta {delta_dir}">{delta_text}</div>' if delta_text else ""
    return f"""
    <div class="kpi-card">
      <div class="label">{label}</div>
      <div class="value">{value}</div>
      {delta_html}
    </div>"""

def plot_cfg(fig, h=340):
    fig.update_layout(
        height=h,
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter", size=12),
        margin=dict(l=10, r=10, t=36, b=10),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    fig.update_xaxes(showgrid=False, linecolor="#e8eaf0")
    fig.update_yaxes(gridcolor="#f1f5f9", zeroline=False)
    return fig

def color_map():
    return {"Studio 1": S1_COLOR, "Studio 2": S2_COLOR}

# ─────────────────────────────────────────────────────
# HEADER
# ─────────────────────────────────────────────────────
st.markdown("# 🏛️ Kostas Acropolis Studios")
col_badge, col_sub = st.columns([3, 7])
with col_badge:
    if show1: st.markdown('<span class="badge-s1">● Studio 1</span>', unsafe_allow_html=True)
    if show2: st.markdown('<span class="badge-s2">● Studio 2</span>', unsafe_allow_html=True)
with col_sub:
    st.markdown("<span style='color:#8a94a6;font-size:.85rem'>Athens Hospitality KPI Dashboard · Real-time data from Snowflake</span>", unsafe_allow_html=True)
st.markdown("---")

# ─────────────────────────────────────────────────────
# TOP KPI SUMMARY ROW
# ─────────────────────────────────────────────────────
def aggregate_booked(studios):
    frames = [data[s]["booked"] for s in studios if not data[s]["booked"].empty]
    if not frames: return {}
    df = pd.concat(frames)
    return {
        "gross":  df["PAYMENT_GROSS"].sum() if "PAYMENT_GROSS" in df.columns else 0,
        "net":    df["PAYMENT_NET"].sum()   if "PAYMENT_NET"   in df.columns else 0,
        "occ":    df["OCCUPANCY_RATE"].mean() if "OCCUPANCY_RATE" in df.columns else 0,
        "adr":    df["AVERAGE_DAILY_RATE"].mean() if "AVERAGE_DAILY_RATE" in df.columns else 0,
        "revpar": df["REVENUE_PER_AVAILABLE_ROOM"].mean() if "REVENUE_PER_AVAILABLE_ROOM" in df.columns else 0,
        "nights": df["TOTAL_BOOKED_DAYS"].sum() if "TOTAL_BOOKED_DAYS" in df.columns else 0,
    }

def aggregate_alos(studios):
    frames = [data[s]["alos"] for s in studios if not data[s]["alos"].empty]
    if not frames: return 0
    df = pd.concat(frames)
    return df["ALOS"].mean() if "ALOS" in df.columns else 0

def aggregate_cancel(studios):
    frames = [data[s]["cancel"] for s in studios if not data[s]["cancel"].empty]
    if not frames: return 0
    df = pd.concat(frames)
    return df["CANCELLATION_RATE"].mean() * 100 if "CANCELLATION_RATE" in df.columns else 0

def aggregate_lead(studios):
    frames = [data[s]["lead"] for s in studios if not data[s]["lead"].empty]
    if not frames: return 0
    df = pd.concat(frames)
    return df["GLOBAL_AVG_LEAD_TIME"].iloc[0] if "GLOBAL_AVG_LEAD_TIME" in df.columns and len(df) > 0 else 0

t      = aggregate_booked(active_studios)
alos   = aggregate_alos(active_studios)
cancel = aggregate_cancel(active_studios)
lead   = aggregate_lead(active_studios)

cols = st.columns(8)
cards = [
    ("Gross Revenue",    f"€{t.get('gross',0):,.0f}",   "", "neu"),
    ("Net Revenue",      f"€{t.get('net',0):,.0f}",     "", "neu"),
    ("Booked Nights",    f"{t.get('nights',0):.0f}",    "", "neu"),
    ("Avg Occupancy",    f"{t.get('occ',0):.1f}%",      "", "up" if t.get('occ',0)>=70 else "down"),
    ("ADR",              f"€{t.get('adr',0):.0f}",      "", "neu"),
    ("RevPAR",           f"€{t.get('revpar',0):.0f}",   "", "neu"),
    ("ALOS",             f"{alos:.1f} nights",           "", "neu"),
    ("Cancel Rate",      f"{cancel:.1f}%",               "", "down" if cancel>10 else "up"),
]
for col, (lbl, val, dlt, ddir) in zip(cols, cards):
    col.markdown(kpi_card(lbl, val, dlt, ddir), unsafe_allow_html=True)

st.markdown("---")

# ─────────────────────────────────────────────────────
# TABS
# ─────────────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📈 Revenue & Occupancy",
    "❌ Cancellations",
    "⏳ Lead Time",
    "🛏️ ALOS",
    "📋 Raw Data",
])

# ════════════════════════════════════════════
# TAB 1 — REVENUE & OCCUPANCY
# ════════════════════════════════════════════
with tab1:

    # ── Revenue bar chart ──────────────────
    st.markdown('<div class="sec-title">Monthly Revenue (Gross vs Net)</div>', unsafe_allow_html=True)
    rev_rows = []
    for s in active_studios:
        df = data[s]["booked"]
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
                            shared_yaxes=True,
                            horizontal_spacing=0.08)
        for s in active_studios:
            clr = color_map()[s]
            sub = df_r[df_r["Studio"] == s]
            fig.add_trace(go.Bar(name=f"{s} Gross", x=sub["Month"], y=sub["Gross €"],
                                 marker_color=clr, opacity=0.9), row=1, col=1)
            fig.add_trace(go.Bar(name=f"{s} Net", x=sub["Month"], y=sub["Net €"],
                                 marker_color=clr, opacity=0.5), row=1, col=2)
        plot_cfg(fig, 420)
        # Extra top margin so subplot titles don't overlap the bars
        fig.update_layout(margin=dict(l=10, r=10, t=70, b=10))
        # Push subplot title annotations higher
        for ann in fig.layout.annotations:
            ann.y = 1.06
        st.plotly_chart(fig, use_container_width=True)

    # ── Occupancy + RevPAR ─────────────────
    c1, c2 = st.columns(2)

    with c1:
        st.markdown('<div class="sec-title">Occupancy Rate %</div>', unsafe_allow_html=True)
        occ_rows = []
        for s in active_studios:
            df = data[s]["booked"]
            if "OCCUPANCY_RATE" in df.columns:
                for _, r in df.iterrows():
                    occ_rows.append({"Month": r["MONTH"], "Studio": s, "Occupancy %": r["OCCUPANCY_RATE"]})
        if occ_rows:
            df_o = pd.DataFrame(occ_rows)
            df_o["Month"] = pd.Categorical(df_o["Month"], categories=MONTH_ORDER, ordered=True)
            df_o = df_o.sort_values("Month")
            fig = px.line(df_o, x="Month", y="Occupancy %", color="Studio", markers=True,
                          color_discrete_map=color_map())
            fig.add_hrect(y0=80, y1=100, fillcolor="#22c55e", opacity=0.05, line_width=0)
            fig.add_hline(y=80, line_dash="dot", line_color="#22c55e",
                          annotation_text="Target 80%", annotation_font_color="#22c55e")
            plot_cfg(fig, 320)
            st.plotly_chart(fig, use_container_width=True)

    with c2:
        st.markdown('<div class="sec-title">ADR vs RevPAR</div>', unsafe_allow_html=True)
        pr_rows = []
        for s in active_studios:
            df = data[s]["booked"]
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

    # ── Booked vs Available days waterfall ─
    st.markdown('<div class="sec-title">Booked Days vs Days in Month</div>', unsafe_allow_html=True)
    bd_rows = []
    for s in active_studios:
        df = data[s]["booked"]
        for _, r in df.iterrows():
            bd_rows.append({"Month": r["MONTH"], "Studio": s,
                            "Booked":    r.get("TOTAL_BOOKED_DAYS", 0),
                            "Available": r.get("DAYS", 30)})
    if bd_rows:
        df_bd = pd.DataFrame(bd_rows)
        df_bd["Month"] = pd.Categorical(df_bd["Month"], categories=MONTH_ORDER, ordered=True)
        df_bd = df_bd.sort_values("Month")
        fig = px.bar(df_bd, x="Month", y=["Available","Booked"],
                     barmode="overlay", facet_col="Studio" if len(active_studios) > 1 else None,
                     opacity=0.85,
                     color_discrete_map={"Available": "#e2e8f0", "Booked": S1_COLOR if show1 and not show2 else S2_COLOR if show2 and not show1 else ACCENT},
                     labels={"value": "Days", "variable": "Type"})
        plot_cfg(fig, 300)
        st.plotly_chart(fig, use_container_width=True)

    # ── Total Combined Net Revenue (KOSTAS_TOTAL_REVENUE) ─
    st.markdown('<div class="sec-title">📦 Combined Net Revenue — Both Studios (TRevPAR View)</div>', unsafe_allow_html=True)
    if not df_total_revenue.empty and "TOTAL_NET_REVENUE" in df_total_revenue.columns:
        df_tr = df_total_revenue.copy()
        # Rename MONTH column if needed
        if "MONTH" not in df_tr.columns and "CUSTOM_MONTH_NAME" in df_tr.columns:
            df_tr = df_tr.rename(columns={"CUSTOM_MONTH_NAME": "MONTH"})
        if "MONTH" in df_tr.columns:
            df_tr["Month"] = pd.Categorical(df_tr["MONTH"], categories=MONTH_ORDER, ordered=True)
            df_tr = df_tr.sort_values("Month")

        fig_tr = go.Figure()
        fig_tr.add_trace(go.Bar(
            name="Total Net Revenue",
            x=df_tr["Month"],
            y=df_tr["TOTAL_NET_REVENUE"],
            marker=dict(
                color=df_tr["TOTAL_NET_REVENUE"],
                colorscale=[[0, "#fca5a5"], [0.5, "#f97316"], [1, "#16a34a"]],
                showscale=False,
            ),
            text=df_tr["TOTAL_NET_REVENUE"].map(lambda v: f"€{v:,.0f}"),
            textposition="outside",
        ))
        # Overlay TRevPAR as a line on secondary y-axis
        if "TREVPAR" in df_tr.columns:
            fig_tr.add_trace(go.Scatter(
                name="TRevPAR",
                x=df_tr["Month"],
                y=df_tr["TREVPAR"],
                mode="lines+markers",
                line=dict(color="#6366f1", width=2.5),
                marker=dict(size=7),
                yaxis="y2",
            ))
        fig_tr.update_layout(
            title="Combined Net Revenue & TRevPAR per Month (Studio 1 + Studio 2)",
            yaxis=dict(title="Net Revenue (€)", gridcolor="#f1f5f9"),
            yaxis2=dict(title="TRevPAR (€)", overlaying="y", side="right",
                        showgrid=False, zeroline=False),
        )
        plot_cfg(fig_tr, 380)
        fig_tr.update_layout(margin=dict(l=10, r=60, t=60, b=10))
        st.plotly_chart(fig_tr, use_container_width=True)
    else:
        st.info("ℹ️ Το KOSTAS_TOTAL_REVENUE view δεν επέστρεψε δεδομένα.")

# ════════════════════════════════════════════
# TAB 2 — CANCELLATIONS
# ════════════════════════════════════════════
with tab2:
    st.markdown('<div class="sec-title">Cancellation Rate per Month</div>', unsafe_allow_html=True)

    cr_rows = []
    for s in active_studios:
        df = data[s]["cancel"]
        if "CUSTOM_MONTH_NAME" in df.columns:
            df = df.rename(columns={"CUSTOM_MONTH_NAME": "MONTH"})
        for _, r in df.iterrows():
            cr_rows.append({
                "Month": r["MONTH"],
                "Studio": s,
                "Cancel Rate %": round(r.get("CANCELLATION_RATE", 0) * 100, 2),
                "Canceled":      int(r.get("TOTAL_CANCELED", 0)),
                "Confirmed":     int(r.get("TOTAL_CONFIRMED", 0)),
            })

    if cr_rows:
        df_cr = pd.DataFrame(cr_rows)
        df_cr["Month"] = pd.Categorical(df_cr["Month"], categories=MONTH_ORDER, ordered=True)
        df_cr = df_cr.sort_values("Month")

        # Rate line chart
        fig = px.bar(df_cr, x="Month", y="Cancel Rate %", color="Studio",
                     barmode="group", color_discrete_map=color_map(),
                     text_auto=".1f")
        fig.add_hline(y=10, line_dash="dot", line_color="#ef4444",
                      annotation_text="Warning 10%", annotation_font_color="#ef4444")
        fig.update_traces(textposition="outside")
        plot_cfg(fig, 360)
        st.plotly_chart(fig, use_container_width=True)

        # Confirmed vs Canceled stacked
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

        # KPI summary per studio
        st.markdown('<div class="sec-title">Cancellation Summary</div>', unsafe_allow_html=True)
        summ_cols = st.columns(len(active_studios))
        for i, s in enumerate(active_studios):
            sub = df_cr[df_cr["Studio"] == s]
            total_can  = sub["Canceled"].sum()
            total_conf = sub["Confirmed"].sum()
            avg_rate   = sub["Cancel Rate %"].mean()
            with summ_cols[i]:
                st.markdown(kpi_card(f"{s} · Avg Cancel Rate", f"{avg_rate:.1f}%",
                                     delta_text="⚠ High" if avg_rate > 10 else "✓ OK",
                                     delta_dir="down" if avg_rate > 10 else "up"),
                            unsafe_allow_html=True)
                st.markdown(kpi_card(f"{s} · Total Canceled", str(total_can)), unsafe_allow_html=True)
                st.markdown(kpi_card(f"{s} · Total Confirmed", str(total_conf)), unsafe_allow_html=True)

# ════════════════════════════════════════════
# TAB 3 — LEAD TIME
# ════════════════════════════════════════════
with tab3:
    st.markdown('<div class="sec-title">Booking Lead Time — How Early Guests Book</div>', unsafe_allow_html=True)
    st.caption("Lead Time = days between booking date and check-in date. Longer lead time = more stable pipeline.")

    lt_summary = st.columns(len(active_studios) * 2)
    col_idx = 0
    for s in active_studios:
        df_lt = data[s]["lead"]
        if df_lt.empty: continue
        g_avg = df_lt["GLOBAL_AVG_LEAD_TIME"].iloc[0] if "GLOBAL_AVG_LEAD_TIME" in df_lt.columns else 0
        med   = df_lt["LEAD_TIME_DAYS"].median()       if "LEAD_TIME_DAYS" in df_lt.columns else 0
        lt_summary[col_idx].markdown(kpi_card(f"{s} · Global Avg Lead Time", f"{g_avg:.0f} days"), unsafe_allow_html=True)
        col_idx += 1
        lt_summary[col_idx].markdown(kpi_card(f"{s} · Median Lead Time", f"{med:.0f} days"), unsafe_allow_html=True)
        col_idx += 1

    st.markdown("")

    # Distribution histogram
    hist_rows = []
    for s in active_studios:
        df_lt = data[s]["lead"]
        if "LEAD_TIME_DAYS" in df_lt.columns:
            for v in df_lt["LEAD_TIME_DAYS"].dropna():
                hist_rows.append({"Studio": s, "Lead Time (days)": v})
    if hist_rows:
        fig = px.histogram(pd.DataFrame(hist_rows), x="Lead Time (days)",
                           color="Studio", barmode="overlay", nbins=35, opacity=0.75,
                           color_discrete_map=color_map(),
                           title="Distribution of Lead Times")
        fig.add_vline(x=7,  line_dash="dot", line_color="#f59e0b", annotation_text="7d")
        fig.add_vline(x=30, line_dash="dot", line_color="#3b82f6", annotation_text="30d")
        plot_cfg(fig, 360)
        st.plotly_chart(fig, use_container_width=True)

    # Monthly avg lead time bars
    c1, c2 = st.columns(2)
    chart_cols = [c1, c2]
    for i, s in enumerate(active_studios):
        df_lt = data[s]["lead"]
        if "MONTHLY_AVG_LEAD_TIME" not in df_lt.columns or "CHECK_IN_DATE_CONVERTED" not in df_lt.columns:
            continue
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
                     title=f"{s} · Monthly Avg Lead Time",
                     text_auto=".0f")
        fig.update_traces(textposition="outside")
        plot_cfg(fig, 300)
        chart_cols[i].plotly_chart(fig, use_container_width=True)

    # Scatter: every booking
    sc_rows = []
    for s in active_studios:
        df_lt = data[s]["lead"]
        if "LEAD_TIME_DAYS" in df_lt.columns and "CHECK_IN_DATE_CONVERTED" in df_lt.columns:
            tmp = df_lt[["CHECK_IN_DATE_CONVERTED", "LEAD_TIME_DAYS", "GUEST_NAME"]].copy()
            tmp["Studio"] = s
            sc_rows.append(tmp)
    if sc_rows:
        df_sc = pd.concat(sc_rows)
        df_sc["CHECK_IN_DATE_CONVERTED"] = pd.to_datetime(df_sc["CHECK_IN_DATE_CONVERTED"], errors="coerce")
        df_sc = df_sc.dropna(subset=["CHECK_IN_DATE_CONVERTED", "LEAD_TIME_DAYS"])
        fig = px.scatter(df_sc, x="CHECK_IN_DATE_CONVERTED", y="LEAD_TIME_DAYS",
                         color="Studio", hover_data=["GUEST_NAME"],
                         color_discrete_map=color_map(),
                         title="Lead Time per Booking",
                         labels={"CHECK_IN_DATE_CONVERTED": "Check-in Date",
                                 "LEAD_TIME_DAYS": "Lead Time (days)"})
        fig.update_traces(marker=dict(size=8, opacity=0.75))
        plot_cfg(fig, 380)
        st.plotly_chart(fig, use_container_width=True)

# ════════════════════════════════════════════
# TAB 4 — ALOS
# ════════════════════════════════════════════
with tab4:
    st.markdown('<div class="sec-title">ALOS — Average Length of Stay</div>', unsafe_allow_html=True)
    st.caption("ALOS = Total Nights ÷ Total Bookings. Higher ALOS = fewer turnovers, lower operational costs.")

    # ALOS summary cards
    alos_cols = st.columns(len(active_studios) * 2)
    ai = 0
    for s in active_studios:
        df_a = data[s]["alos"]
        if df_a.empty: continue
        avg_alos   = df_a["ALOS"].mean() if "ALOS" in df_a.columns else 0
        total_bk   = df_a["TOTAL_BOOKINGS"].sum() if "TOTAL_BOOKINGS" in df_a.columns else 0
        total_nts  = df_a["TOTAL_NIGHTS_CAPPED"].sum() if "TOTAL_NIGHTS_CAPPED" in df_a.columns else 0
        alos_cols[ai].markdown(kpi_card(f"{s} · Avg ALOS", f"{avg_alos:.1f} nights",
                                        "Above target" if avg_alos >= 3 else "Below 3 nights",
                                        "up" if avg_alos >= 3 else "down"),
                               unsafe_allow_html=True)
        ai += 1
        alos_cols[ai].markdown(kpi_card(f"{s} · Total Bookings", str(int(total_bk))), unsafe_allow_html=True)
        ai += 1

    st.markdown("")

    # ALOS trend line
    alos_rows = []
    for s in active_studios:
        df_a = data[s]["alos"]
        if "ALOS" in df_a.columns:
            for _, r in df_a.iterrows():
                alos_rows.append({"Month": r["MONTH"], "Studio": s,
                                  "ALOS": r["ALOS"], "Bookings": r.get("TOTAL_BOOKINGS", 0),
                                  "Nights": r.get("TOTAL_NIGHTS_CAPPED", 0)})
    if alos_rows:
        df_al = pd.DataFrame(alos_rows)
        df_al["Month"] = pd.Categorical(df_al["Month"], categories=MONTH_ORDER, ordered=True)
        df_al = df_al.sort_values("Month")

        c1, c2 = st.columns(2)
        with c1:
            st.markdown('<div class="sec-title">ALOS Trend</div>', unsafe_allow_html=True)
            fig = px.line(df_al, x="Month", y="ALOS", color="Studio", markers=True,
                          color_discrete_map=color_map())
            fig.add_hline(y=3, line_dash="dot", line_color="#f59e0b",
                          annotation_text="Min target: 3 nights")
            plot_cfg(fig, 320)
            st.plotly_chart(fig, use_container_width=True)

        with c2:
            st.markdown('<div class="sec-title">Total Bookings per Month</div>', unsafe_allow_html=True)
            fig = px.bar(df_al, x="Month", y="Bookings", color="Studio",
                         barmode="group", color_discrete_map=color_map(),
                         text_auto=True)
            fig.update_traces(textposition="outside")
            plot_cfg(fig, 320)
            st.plotly_chart(fig, use_container_width=True)

        # Total booked nights stacked
        st.markdown('<div class="sec-title">Total Nights Capped per Month</div>', unsafe_allow_html=True)
        fig = px.area(df_al, x="Month", y="Nights", color="Studio",
                      color_discrete_map=color_map())
        plot_cfg(fig, 300)
        st.plotly_chart(fig, use_container_width=True)

# ════════════════════════════════════════════
# TAB 5 — RAW DATA
# ════════════════════════════════════════════
with tab5:
    st.markdown('<div class="sec-title">Raw View Data</div>', unsafe_allow_html=True)

    for s in active_studios:
        st.markdown(f"#### {s}")
        t1, t2, t3, t4 = st.tabs(["Revenue / Occupancy", "Cancellations", "Lead Time", "ALOS"])
        with t1:
            st.dataframe(data[s]["booked"], use_container_width=True, hide_index=True)
        with t2:
            st.dataframe(data[s]["cancel"], use_container_width=True, hide_index=True)
        with t3:
            lt_display_cols = [c for c in ["GUEST_NAME","CHECK_IN_DATE_CONVERTED","BOOKING_DATE",
                                            "LEAD_TIME_DAYS","GLOBAL_AVG_LEAD_TIME","MONTHLY_AVG_LEAD_TIME"]
                               if c in data[s]["lead"].columns]
            st.dataframe(data[s]["lead"][lt_display_cols], use_container_width=True, hide_index=True)
        with t4:
            st.dataframe(data[s]["alos"], use_container_width=True, hide_index=True)

    # ── Export ──
    st.markdown("---")
    st.markdown('<div class="sec-title">Export</div>', unsafe_allow_html=True)
    export_cols = st.columns(4)

    for i, s in enumerate(active_studios):
        csv = data[s]["booked"].to_csv(index=False).encode("utf-8")
        export_cols[i * 2].download_button(
            f"⬇️ {s} Revenue CSV", csv, f"{s.replace(' ','_')}_revenue.csv", "text/csv"
        )
        csv2 = data[s]["alos"].to_csv(index=False).encode("utf-8")
        export_cols[i * 2 + 1].download_button(
            f"⬇️ {s} ALOS CSV", csv2, f"{s.replace(' ','_')}_alos.csv", "text/csv"
        )

# ─────────────────────────────────────────────────────
# FOOTER
# ─────────────────────────────────────────────────────
st.markdown("---")
st.markdown(
    "<div style='text-align:center;color:#8a94a6;font-size:.78rem'>"
    "🏛️ Kostas Acropolis Studios · KPI Dashboard · "
    "Data: Snowflake AIRBNB_DB.RAW · Built with Streamlit & Plotly"
    "</div>",
    unsafe_allow_html=True,
)
