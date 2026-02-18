"""
Streamlit Dashboard for Heartbeat Monitoring System

Provides a real-time visualization dashboard for monitoring customer
heart rates, anomalies, and pipeline health metrics.

Run with: streamlit run src/dashboard/app.py
"""

import time
from datetime import datetime, timedelta

import pandas as pd
import streamlit as st

from src.config import config
from src.database.db_handler import DatabaseHandler


# ─── Page Configuration ─────────────────────────────────────────────────────
st.set_page_config(
    page_title="Heartbeat Monitor",
    page_icon="💓",
    layout="wide",
    initial_sidebar_state="expanded",
)


@st.cache_resource
def get_db_handler() -> DatabaseHandler:
    """Create a cached database handler."""
    handler = DatabaseHandler()
    handler.connect()
    return handler


def render_header():
    """Render the dashboard header."""
    st.title("💓 Real-Time Customer Heartbeat Monitor")
    st.markdown(
        "Monitoring heart rate data from the streaming pipeline. "
        f"Data refreshes every **{config.dashboard.refresh_interval}** seconds."
    )
    st.divider()


def render_kpi_cards(db: DatabaseHandler):
    """Render top-level KPI metric cards."""
    col1, col2, col3, col4 = st.columns(4)

    try:
        total_readings = db.get_total_readings_count()
        latest = db.get_latest_readings(limit=100)
        anomalies = db.get_anomalies(limit=100)
        summaries = db.get_customer_summary()

        with col1:
            st.metric(
                label="Total Readings",
                value=f"{total_readings:,}",
            )
        with col2:
            avg_hr = 0
            if latest:
                avg_hr = round(sum(r["heart_rate"] for r in latest) / len(latest), 1)
            st.metric(
                label="Avg Heart Rate (recent)",
                value=f"{avg_hr} bpm",
            )
        with col3:
            st.metric(
                label="Recent Anomalies",
                value=len(anomalies),
            )
        with col4:
            active_customers = len(summaries) if summaries else 0
            st.metric(
                label="Active Customers",
                value=active_customers,
            )
    except Exception as e:
        st.error(f"Error loading KPIs: {e}")


def render_latest_readings(db: DatabaseHandler):
    """Render a table of latest heart rate readings."""
    st.subheader("📊 Latest Heart Rate Readings")

    try:
        readings = db.get_latest_readings(limit=50)
        if readings:
            df = pd.DataFrame(readings)
            df["timestamp"] = pd.to_datetime(df["timestamp"])

            # Color-code anomalies
            def highlight_anomaly(row):
                if row.get("is_anomaly"):
                    return ["background-color: #ffcccc"] * len(row)
                return [""] * len(row)

            styled_df = df.style.apply(highlight_anomaly, axis=1)
            st.dataframe(styled_df, use_container_width=True, height=400)
        else:
            st.info("No readings available yet. Start the pipeline to see data.")
    except Exception as e:
        st.error(f"Error loading readings: {e}")


def render_customer_chart(db: DatabaseHandler):
    """Render heart rate time-series chart per customer."""
    st.subheader("📈 Heart Rate Over Time")

    try:
        summaries = db.get_customer_summary()
        if not summaries:
            st.info("No customer data available.")
            return

        customer_ids = [s["customer_id"] for s in summaries]
        selected_customer = st.selectbox(
            "Select Customer",
            customer_ids,
            index=0,
        )

        readings = db.get_customer_readings(selected_customer, limit=200)
        if readings:
            df = pd.DataFrame(readings)
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            df = df.sort_values("timestamp")

            st.line_chart(
                df.set_index("timestamp")["heart_rate"],
                use_container_width=True,
            )

            # Show summary stats
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Min HR", f"{df['heart_rate'].min()} bpm")
            with col2:
                st.metric("Max HR", f"{df['heart_rate'].max()} bpm")
            with col3:
                st.metric("Avg HR", f"{df['heart_rate'].mean():.1f} bpm")
        else:
            st.info(f"No readings for {selected_customer}")
    except Exception as e:
        st.error(f"Error loading chart: {e}")


def render_anomaly_log(db: DatabaseHandler):
    """Render the anomaly event log."""
    st.subheader("🚨 Anomaly Log")

    try:
        anomalies = db.get_anomalies(limit=30)
        if anomalies:
            df = pd.DataFrame(anomalies)
            df["timestamp"] = pd.to_datetime(df["timestamp"])

            # Color HIGH vs LOW anomalies
            for _, row in df.iterrows():
                if row["anomaly_type"] == "HIGH":
                    st.error(
                        f"⬆️ **HIGH** | {row['customer_name']} ({row['customer_id']}) | "
                        f"HR: {row['heart_rate']} bpm | {row['timestamp']}"
                    )
                else:
                    st.warning(
                        f"⬇️ **LOW** | {row['customer_name']} ({row['customer_id']}) | "
                        f"HR: {row['heart_rate']} bpm | {row['timestamp']}"
                    )
        else:
            st.success("No anomalies detected recently. All systems normal.")
    except Exception as e:
        st.error(f"Error loading anomalies: {e}")


def render_customer_summary(db: DatabaseHandler):
    """Render customer summary statistics."""
    st.subheader("👥 Customer Summary")

    try:
        summaries = db.get_customer_summary()
        if summaries:
            df = pd.DataFrame(summaries)
            st.dataframe(df, use_container_width=True)
        else:
            st.info("No customer data available.")
    except Exception as e:
        st.error(f"Error loading summary: {e}")


def render_sidebar():
    """Render the sidebar with settings and info."""
    with st.sidebar:
        st.header("⚙️ Settings")
        refresh = st.slider(
            "Refresh Interval (sec)",
            min_value=1,
            max_value=30,
            value=config.dashboard.refresh_interval,
        )

        st.divider()
        st.header("ℹ️ Pipeline Info")
        st.markdown(f"""
        - **Kafka Topic:** `{config.kafka.topic}`
        - **Database:** `{config.postgres.database}`
        - **Anomaly Low:** `< {config.anomaly.low_threshold} bpm`
        - **Anomaly High:** `> {config.anomaly.high_threshold} bpm`
        """)

        st.divider()
        st.markdown(
            "Built with ❤️ by Emmanuel Kabu\n\n"
            "Real-Time Customer Heartbeat Monitoring System"
        )

        return refresh


def main():
    """Main dashboard entry point."""
    refresh_interval = render_sidebar()
    render_header()

    db = get_db_handler()

    # KPI Cards
    render_kpi_cards(db)

    # Two-column layout
    col_left, col_right = st.columns([2, 1])

    with col_left:
        render_customer_chart(db)
        render_latest_readings(db)

    with col_right:
        render_anomaly_log(db)

    # Full-width customer summary
    render_customer_summary(db)

    # Auto-refresh
    time.sleep(refresh_interval)
    st.rerun()


if __name__ == "__main__":
    main()
