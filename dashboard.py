import duckdb
import pandas as pd
import streamlit as st
import plotly.express as px
import time
from streamlit_autorefresh import st_autorefresh

DB_PATH = "bus_positions.duckdb"
refresh_ms = 30000

st.set_page_config(
    page_title="WMATA Live Bus Positions",
    layout="wide"
)

count = st_autorefresh(interval=refresh_ms, limit=None, key="bus_refresh")

try:
    con = duckdb.connect("bus_positions.duckdb", read_only=True)
    df = con.execute("""
        SELECT VehicleID, Lat, Lon, DateTime, TripHeadsign
        FROM bus_positions
        WHERE Lat IS NOT NULL AND Lon IS NOT NULL
        """).fetchdf()
    
except Exception as e:
    st.error(f"Failed to read from DuckDB: {e}")
    df = pd.DataFrame

if df.empty:
    st.warning("No bus data available yet. Waiting for consumer")
else: 
    fig = px.scatter_map(
        df,
        lat="Lat",
        lon="Lon",
        hover_name="TripHeadsign",
        hover_data=["VehicleID", "DateTime"],
        height=600,
        zoom=12
    )
    fig.update_layout(
        mapbox_style="open-street-map",
        title="Live Bus Locations",
    )
    st.plotly_chart(fig, width='stretch')

st.write("Last updated:", pd.Timestamp.utcnow())