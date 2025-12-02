import duckdb
import pandas as pd
import streamlit as st
import plotly.express as px

DB_PATH = "bus_positions.duckdb" #set path to duckdb database 

st.set_page_config(
    page_title="WMATA Bus Historical Performance",
    layout="wide"
)

st.title(f"WMATA Bus Performance")

try: #connect to duckdb and read bus positions that are not null
    con = duckdb.connect(DB_PATH, read_only=True)
    df = con.execute(f"""
        SELECT *
        FROM bus_positions
        WHERE Lat IS NOT NULL AND Lon IS NOT NULL
    """).fetchdf()
except Exception as e: #handle errors and missing data
    st.error(f"Failed to read from DuckDB: {e}")
    df = pd.DataFrame()

if df.empty: #error handling for empty database 
    st.warning("No bus data available in the selected time window.")
    st.stop()

fig = px.scatter_mapbox( #create map using dataframe with color by deviation
    df,
    lat="Lat",
    lon="Lon",
    color="Deviation",
    color_continuous_scale="Jet",
    zoom=12,
    mapbox_style="open-street-map",
    hover_data=["RouteID", "VehicleID", "Deviation", "DateTime"]
)

fig.update_traces(marker=dict(size=12, opacity=0.4)) #have data points translucent 

fig.update_coloraxes( #add axis and set max and min 
    colorbar_title="Deviation (min)",
    cmin=-10,  
    cmax=10
)

fig.update_layout(
    title=f"Bus On-Time Performance",
    mapbox=dict(center={"lat": 38.89511, "lon": -77.03637}) #center in middle of DC
)

st.plotly_chart(fig, config={"fillContainer": True})

#show dataframe 
st.subheader("Bus Data") 
st.dataframe(df[["RouteID", "Lat", "Lon", "Deviation", "DateTime"]])

st.write(f"Last updated: {pd.Timestamp.utcnow()}")
