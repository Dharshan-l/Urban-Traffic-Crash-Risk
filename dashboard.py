import streamlit as st
import pandas as pd

# ---------------------------------------
# PAGE CONFIG
# ---------------------------------------
st.set_page_config(
    page_title="🚦 Traffic Crash Dashboard",
    layout="wide",
    page_icon="🚦"
)

# ---------------------------------------
# CUSTOM CSS (UI IMPROVEMENT)
# ---------------------------------------
st.markdown("""
    <style>
        body {
            background-color: #0e1117;
        }
        .main {
            background-color: #0e1117;
            color: white;
        }
        .stMetric {
            background-color: #1f2937;
            padding: 15px;
            border-radius: 10px;
        }
    </style>
""", unsafe_allow_html=True)

# ---------------------------------------
# TITLE
# ---------------------------------------
st.title("🚦 Urban Traffic Crash Dashboard")
st.markdown("### 📊 Real-time Crash Data Insights")

# ---------------------------------------
# LOAD DATA
# ---------------------------------------
@st.cache_data
def load_data():
    crash_by_area = pd.read_parquet("data_lake/gold/crash_by_area")
    monthly_trend = pd.read_parquet("data_lake/gold/monthly_trend")
    high_risk = pd.read_parquet("data_lake/gold/high_risk")
    return crash_by_area, monthly_trend, high_risk

crash_by_area, monthly_trend, high_risk = load_data()

# ---------------------------------------
# KPI SECTION
# ---------------------------------------
col1, col2, col3 = st.columns(3)

col1.metric("🚗 Total Crashes", len(crash_by_area))
col2.metric("📍 Unique Streets", crash_by_area["street_name"].nunique())
col3.metric("⚠️ High Risk Roads", len(high_risk))

st.divider()

# ---------------------------------------
# FILTER SECTION
# ---------------------------------------
st.sidebar.header("🔍 Filters")

selected_year = st.sidebar.selectbox(
    "Select Year",
    sorted(monthly_trend["year"].unique())
)

filtered_data = monthly_trend[monthly_trend["year"] == selected_year]

# ---------------------------------------
# CHARTS
# ---------------------------------------
col1, col2 = st.columns(2)

# BAR CHART
with col1:
    st.subheader("📊 Crash by Street")
    st.bar_chart(crash_by_area.set_index("street_name"))

# LINE CHART
with col2:
    st.subheader("📈 Monthly Trend")
    filtered_data["year_month"] = filtered_data["year"].astype(str) + "-" + filtered_data["month"].astype(str)
    st.line_chart(filtered_data.set_index("year_month")["monthly_crashes"])

st.divider()

# ---------------------------------------
# TABLE SECTION
# ---------------------------------------
st.subheader("⚠️ Top High Risk Roads")

st.dataframe(high_risk, use_container_width=True)

# ---------------------------------------
# FOOTER
