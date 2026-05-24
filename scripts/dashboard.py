"""
dashboard.py
------------
Optional real-time Streamlit dashboard for the Heart Beat Monitoring System.

Requirements:
    pip install streamlit psycopg2-binary plotly pandas

Run:
    streamlit run scripts/dashboard.py
"""

import os
import time
import psycopg2
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Heart Beat Monitor",
    page_icon="❤️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Database connection
# ---------------------------------------------------------------------------
DB_CONFIG = {
    "host":     os.getenv("PG_HOST",     "localhost"),
    "port":     int(os.getenv("PG_PORT", "5432")),
    "dbname":   os.getenv("PG_DB",       "heartbeat_db"),
    "user":     os.getenv("PG_USER",     "heartbeat_user"),
    "password": os.getenv("PG_PASSWORD", "heartbeat_pass"),
}


@st.cache_resource
def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def query_df(sql: str, params=None) -> pd.DataFrame:
    conn = get_connection()
    return pd.read_sql_query(sql, conn, params=params)


# ---------------------------------------------------------------------------
# Sidebar controls
# ---------------------------------------------------------------------------
st.sidebar.title("⚙️ Controls")
refresh_interval = st.sidebar.slider("Auto-refresh (seconds)", 2, 30, 5)
lookback_minutes = st.sidebar.slider("Lookback window (minutes)", 5, 120, 30)
selected_customer = st.sidebar.selectbox(
    "Focus on customer",
    ["All"] + ["C001", "C002", "C003", "C004", "C005"],
)

# ---------------------------------------------------------------------------
# Title
# ---------------------------------------------------------------------------
st.title("❤️ Real-Time Customer Heart Beat Monitor")
st.caption(f"Auto-refreshing every {refresh_interval}s · last {lookback_minutes} min of data")

# ---------------------------------------------------------------------------
# KPI cards – top row
# ---------------------------------------------------------------------------
kpi_sql = f"""
    SELECT
        COUNT(DISTINCT customer_id)                       AS active_customers,
        COUNT(*)                                          AS total_readings,
        ROUND(AVG(heart_rate)::NUMERIC, 1)                AS avg_bpm,
        COUNT(*) FILTER (WHERE status = 'WARNING')        AS warnings,
        COUNT(*) FILTER (WHERE status = 'CRITICAL')       AS criticals
    FROM heart_rate_readings
    WHERE reading_timestamp >= NOW() - INTERVAL '{lookback_minutes} minutes'
"""
kpi = query_df(kpi_sql).iloc[0]

col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Active Customers", int(kpi["active_customers"]))
col2.metric("Total Readings",   int(kpi["total_readings"]))
col3.metric("Avg BPM",          kpi["avg_bpm"])
col4.metric("⚠️ Warnings",       int(kpi["warnings"]))
col5.metric("🚨 Criticals",      int(kpi["criticals"]))

st.divider()

# ---------------------------------------------------------------------------
# Time-series chart
# ---------------------------------------------------------------------------
ts_where = f"WHERE reading_timestamp >= NOW() - INTERVAL '{lookback_minutes} minutes'"
if selected_customer != "All":
    ts_where += f" AND customer_id = '{selected_customer}'"

ts_sql = f"""
    SELECT customer_id, customer_name, reading_timestamp, heart_rate, status
    FROM   heart_rate_readings
    {ts_where}
    ORDER  BY reading_timestamp
"""
ts_df = query_df(ts_sql)

if not ts_df.empty:
    fig = px.line(
        ts_df,
        x="reading_timestamp",
        y="heart_rate",
        color="customer_name",
        title=f"Heart Rate – last {lookback_minutes} min",
        labels={"reading_timestamp": "Time", "heart_rate": "BPM"},
        template="plotly_dark",
    )
    # Horizontal reference bands
    fig.add_hrect(y0=0,   y1=40,  fillcolor="red",    opacity=0.1, line_width=0)
    fig.add_hrect(y0=40,  y1=50,  fillcolor="orange",  opacity=0.1, line_width=0)
    fig.add_hrect(y0=130, y1=150, fillcolor="orange",  opacity=0.1, line_width=0)
    fig.add_hrect(y0=150, y1=220, fillcolor="red",    opacity=0.1, line_width=0)
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No data yet – make sure the producer and consumer are running.")

# ---------------------------------------------------------------------------
# Per-customer latest status cards
# ---------------------------------------------------------------------------
st.subheader("Customer Status")
latest_sql = """
    SELECT DISTINCT ON (customer_id)
        customer_id, customer_name, heart_rate, status, reading_timestamp
    FROM heart_rate_readings
    ORDER BY customer_id, reading_timestamp DESC
"""
latest_df = query_df(latest_sql)

status_color = {"NORMAL": "🟢", "WARNING": "🟡", "CRITICAL": "🔴"}
cols = st.columns(len(latest_df) if not latest_df.empty else 1)
for i, (_, row) in enumerate(latest_df.iterrows()):
    icon = status_color.get(row["status"], "⚪")
    cols[i].metric(
        label=f"{icon} {row['customer_name']}",
        value=f"{row['heart_rate']} BPM",
        delta=row["status"],
    )

st.divider()

# ---------------------------------------------------------------------------
# Recent anomalies table
# ---------------------------------------------------------------------------
st.subheader("🚨 Recent Anomalies")
anom_sql = f"""
    SELECT customer_id, customer_name, reading_timestamp, heart_rate, status
    FROM   heart_rate_readings
    WHERE  status <> 'NORMAL'
      AND  reading_timestamp >= NOW() - INTERVAL '{lookback_minutes} minutes'
    ORDER  BY reading_timestamp DESC
    LIMIT  50
"""
anom_df = query_df(anom_sql)
if not anom_df.empty:
    st.dataframe(anom_df, use_container_width=True, hide_index=True)
else:
    st.success("No anomalies in the selected window. All customers look healthy! 🎉")

# ---------------------------------------------------------------------------
# Status distribution pie chart
# ---------------------------------------------------------------------------
dist_sql = f"""
    SELECT status, COUNT(*) AS cnt
    FROM   heart_rate_readings
    WHERE  reading_timestamp >= NOW() - INTERVAL '{lookback_minutes} minutes'
    GROUP  BY status
"""
dist_df = query_df(dist_sql)
if not dist_df.empty:
    pie = px.pie(
        dist_df, names="status", values="cnt",
        title="Reading Status Distribution",
        color="status",
        color_discrete_map={"NORMAL": "#22c55e", "WARNING": "#f59e0b", "CRITICAL": "#ef4444"},
        template="plotly_dark",
    )
    left, right = st.columns(2)
    left.plotly_chart(pie, use_container_width=True)

    # Hourly average BPM bar chart
    avg_sql = f"""
        SELECT customer_name,
               ROUND(AVG(heart_rate)::NUMERIC, 1) AS avg_bpm
        FROM   heart_rate_readings
        WHERE  reading_timestamp >= NOW() - INTERVAL '{lookback_minutes} minutes'
        GROUP  BY customer_name
        ORDER  BY avg_bpm DESC
    """
    avg_df = query_df(avg_sql)
    bar = px.bar(
        avg_df, x="customer_name", y="avg_bpm",
        title=f"Average BPM per Customer (last {lookback_minutes} min)",
        labels={"customer_name": "Customer", "avg_bpm": "Avg BPM"},
        color="avg_bpm",
        color_continuous_scale="RdYlGn_r",
        template="plotly_dark",
    )
    right.plotly_chart(bar, use_container_width=True)

# ---------------------------------------------------------------------------
# Auto-refresh
# ---------------------------------------------------------------------------
time.sleep(refresh_interval)
st.rerun()
