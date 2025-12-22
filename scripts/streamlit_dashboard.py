"""
Streamlit Real-Time Dashboard for Airline Delays
Displays real-time data from Kafka and historical data from ClickHouse
"""

import os
import json
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import clickhouse_connect
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv
import time

# Load environment variables
load_dotenv()

# Configuration
st.set_page_config(
    page_title="Airline Delay Dashboard",
    page_icon="✈️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Environment variables
KAFKA_HOST = os.getenv('KAFKA_HOST', 'localhost')
KAFKA_PORT = os.getenv('KAFKA_PORT', '9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'airline-delays')

CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST', 'localhost')
CLICKHOUSE_PORT = int(os.getenv('CLICKHOUSE_HTTP_PORT', '8123'))
CLICKHOUSE_DATABASE = os.getenv('CLICKHOUSE_DATABASE', 'airline_data')


@st.cache_resource
def init_clickhouse():
    """Initialize ClickHouse connection"""
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        database=CLICKHOUSE_DATABASE
    )


def get_overall_stats(client):
    """Get overall statistics"""
    query = """
    SELECT
        sum(arr_flights) as total_flights,
        sum(arr_del15) as total_delayed,
        sum(arr_cancelled) as total_cancelled,
        sum(arr_diverted) as total_diverted,
        round(sum(arr_del15) * 100.0 / sum(arr_flights), 2) as delay_percentage
    FROM flights
    """
    result = client.query(query)
    return dict(zip(result.column_names, result.result_rows[0]))


def get_top_carriers(client, limit=10):
    """Get top carriers by delay percentage"""
    query = f"""
    SELECT
        carrier_name,
        sum(arr_flights) as flights,
        sum(arr_del15) as delayed,
        round(sum(arr_del15) * 100.0 / sum(arr_flights), 2) as delay_pct
    FROM flights
    GROUP BY carrier_name
    HAVING flights > 1000
    ORDER BY delay_pct DESC
    LIMIT {limit}
    """
    result = client.query(query)
    return pd.DataFrame(result.result_rows, columns=result.column_names)


def get_top_airports(client, limit=10):
    """Get top airports by delay percentage"""
    query = f"""
    SELECT
        airport_name,
        sum(arr_flights) as flights,
        sum(arr_del15) as delayed,
        round(sum(arr_del15) * 100.0 / sum(arr_flights), 2) as delay_pct
    FROM flights
    GROUP BY airport_name
    HAVING flights > 1000
    ORDER BY delay_pct DESC
    LIMIT {limit}
    """
    result = client.query(query)
    return pd.DataFrame(result.result_rows, columns=result.column_names)


def get_delay_causes(client):
    """Get breakdown of delay causes"""
    query = """
    SELECT
        sum(carrier_delay) as carrier_delay,
        sum(weather_delay) as weather_delay,
        sum(nas_delay) as nas_delay,
        sum(security_delay) as security_delay,
        sum(late_aircraft_delay) as late_aircraft_delay
    FROM flights
    """
    result = client.query(query)
    data = dict(zip(result.column_names, result.result_rows[0]))
    
    # Convert to DataFrame for plotting
    df = pd.DataFrame({
        'Cause': ['Carrier', 'Weather', 'NAS', 'Security', 'Late Aircraft'],
        'Minutes': [
            data['carrier_delay'],
            data['weather_delay'],
            data['nas_delay'],
            data['security_delay'],
            data['late_aircraft_delay']
        ]
    })
    return df


def get_monthly_trend(client):
    """Get monthly delay trends"""
    query = """
    SELECT
        year,
        month,
        sum(arr_flights) as flights,
        sum(arr_del15) as delayed,
        round(sum(arr_del15) * 100.0 / sum(arr_flights), 2) as delay_pct
    FROM flights
    GROUP BY year, month
    ORDER BY year, month
    """
    result = client.query(query)
    df = pd.DataFrame(result.result_rows, columns=result.column_names)
    df['date'] = pd.to_datetime(df[['year', 'month']].assign(day=1))
    return df


def main():
    # Title
    st.title("✈️ Real-Time Airline Delay Dashboard")
    st.markdown("---")
    
    # Initialize ClickHouse
    try:
        client = init_clickhouse()
    except Exception as e:
        st.error(f"Failed to connect to ClickHouse: {e}")
        return
    
    # Sidebar
    st.sidebar.header("Dashboard Controls")
    refresh_interval = st.sidebar.slider("Refresh Interval (seconds)", 5, 60, 10)
    auto_refresh = st.sidebar.checkbox("Auto Refresh", value=True)
    
    if st.sidebar.button("Refresh Now") or auto_refresh:
        st.rerun()
    
    # Overall Statistics
    st.header("📊 Overall Statistics")
    stats = get_overall_stats(client)
    
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric("Total Flights", f"{stats['total_flights']:,}")
    with col2:
        st.metric("Delayed Flights", f"{stats['total_delayed']:,}")
    with col3:
        st.metric("Delay Rate", f"{stats['delay_percentage']}%")
    with col4:
        st.metric("Cancelled", f"{stats['total_cancelled']:,}")
    with col5:
        st.metric("Diverted", f"{stats['total_diverted']:,}")
    
    st.markdown("---")
    
    # Two columns for charts
    col1, col2 = st.columns(2)
    
    # Top Carriers
    with col1:
        st.subheader("🏢 Top Carriers by Delay Rate")
        carriers_df = get_top_carriers(client)
        fig = px.bar(
            carriers_df,
            x='delay_pct',
            y='carrier_name',
            orientation='h',
            title='Carriers with Highest Delay Percentage',
            labels={'delay_pct': 'Delay %', 'carrier_name': 'Carrier'},
            color='delay_pct',
            color_continuous_scale='Reds'
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    
    # Top Airports
    with col2:
        st.subheader("🛫 Top Airports by Delay Rate")
        airports_df = get_top_airports(client)
        fig = px.bar(
            airports_df,
            x='delay_pct',
            y='airport_name',
            orientation='h',
            title='Airports with Highest Delay Percentage',
            labels={'delay_pct': 'Delay %', 'airport_name': 'Airport'},
            color='delay_pct',
            color_continuous_scale='Oranges'
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    
    # Delay Causes
    st.subheader("🔍 Delay Causes Breakdown")
    causes_df = get_delay_causes(client)
    
    col1, col2 = st.columns([2, 1])
    with col1:
        fig = px.pie(
            causes_df,
            values='Minutes',
            names='Cause',
            title='Distribution of Delay Minutes by Cause',
            color_discrete_sequence=px.colors.sequential.RdBu
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.dataframe(
            causes_df.style.format({'Minutes': '{:,.0f}'}),
            use_container_width=True,
            height=300
        )
    
    # Monthly Trend
    st.subheader("📈 Monthly Delay Trends")
    trend_df = get_monthly_trend(client)
    
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=trend_df['date'],
        y=trend_df['delay_pct'],
        mode='lines+markers',
        name='Delay %',
        line=dict(color='red', width=2),
        marker=dict(size=6)
    ))
    fig.update_layout(
        title='Monthly Delay Percentage Over Time',
        xaxis_title='Date',
        yaxis_title='Delay Percentage (%)',
        hovermode='x unified',
        height=400
    )
    st.plotly_chart(fig, use_container_width=True)
    
    # Footer
    st.markdown("---")
    st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Auto-refresh
    if auto_refresh:
        time.sleep(refresh_interval)
        st.rerun()


if __name__ == "__main__":
    main()
