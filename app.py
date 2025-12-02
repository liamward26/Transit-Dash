import duckdb
import pandas as pd
import streamlit as st
import plotly.graph_objects as go
import requests
import os 
from streamlit_autorefresh import st_autorefresh

DB_PATH = "bus_positions.duckdb"
ROUTE_DETAILS_URL = "https://api.wmata.com/Bus.svc/json/jRouteDetails"
API_KEY = os.getenv('WMATA_key')

refresh_ms = 30000 #referesh page every 30 seconds 

st.set_page_config(
    page_title="WMATA Live Bus Positions",
    layout="wide"
)

#use streamlit autorefresh package to continuously update page 
count = st_autorefresh(interval=refresh_ms, limit=None, key="bus_refresh")

try:
    con = duckdb.connect(DB_PATH, read_only=True) #connect to duckdb with read only 
    #Get most recent position for for each unique Vehicle ID
    df = con.execute("""
        SELECT *
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY VehicleID ORDER BY DateTime DESC) as rn
            FROM bus_positions
            WHERE Lat IS NOT NULL AND Lon IS NOT NULL
        )
        WHERE rn = 1
    """).fetchdf()
except Exception as e:
    st.error(f"Failed to read from DuckDB: {e}")
    df = pd.DataFrame()

def get_route_details(route_id):
    #use Routes api to get information on all routes to map
    try:
        resp = requests.get(
            ROUTE_DETAILS_URL,
            params={"RouteID": route_id},
            headers={"api_key": API_KEY},
            timeout=10
        )
        resp.raise_for_status()
        return resp.json() #return response as json
    except Exception as e:
        st.warning(f"Could not fetch route details: {e}")
        return None

if df.empty: #if no data in dataframe yet 
    st.warning("No bus data available yet.")
else:
    #Give number of buses currently active
    total_unique_buses = df["VehicleID"].nunique()
    st.text(f"Total Active Buses: {total_unique_buses}")

    #Create dropdown to select bus route to see
    route_options = sorted(df["RouteID"].dropna().unique())
    selected_route = st.selectbox("Select a Bus Route", options=route_options)

    #Get route details from api and get name 
    route_details = get_route_details(selected_route)
    route_name = route_details["Name"] if route_details and "Name" in route_details else selected_route
    route_desc = route_details["Description"] if route_details and "Description" in route_details else ""

    st.markdown(f"### {route_name}") #display route name 
    if route_desc:
        st.markdown(f"*{route_desc}*")

    #filter only selected route in dataframe
    route_df = df[df["RouteID"] == selected_route]

    #show how many buses currently active on selected route 
    st.markdown(f"#### Buses on Route {selected_route} ({len(route_df)} active)")

    #show summary statistics for deviation for selected route 
    if not route_df.empty:
        avg_dev = route_df["Deviation"].mean()
        late = (route_df["Deviation"] > 2).sum()
        early = (route_df["Deviation"] < -2).sum()
        on_time = ((route_df["Deviation"] >= -2) & (route_df["Deviation"] <= 2)).sum()
        st.markdown(
            f"**Deviation Summary:**  \n"
            f"- Average deviation: `{avg_dev:.2f}` min  \n"
            f"- Late (>2 min): `{late}`  \n"
            f"- Early (<-2 min): `{early}`  \n"
            f"- On time (±2 min): `{on_time}`"
        )

    #create figure to map
    fig = go.Figure()

    #plot route shape for selected route 
    if route_details:
        for direction in ["Direction0", "Direction1"]: #get outbound and inbound
            if direction in route_details and "Shape" in route_details[direction]:
                shape = route_details[direction]["Shape"]
                if shape:
                    lats = [pt["Lat"] for pt in shape]
                    lons = [pt["Lon"] for pt in shape]
                    fig.add_trace(go.Scattermapbox( #plot shape of route
                        lat=lats,
                        lon=lons,
                        mode="lines",
                        line=dict(width=4, color="blue"),
                        name=f"Route {selected_route} {direction[-1]}",
                        showlegend=False,
                        hoverinfo="skip" #no info for routes
                    ))

    if not route_df.empty:
        fig.add_trace(go.Scattermapbox( #create black outlines for bus markers
            lat=route_df["Lat"].tolist(),
            lon=route_df["Lon"].tolist(),
            mode="markers",
            marker=dict(
                size=22,
                color="black",
                opacity=0.6,
                symbol="circle"
            ),
            hoverinfo="skip",
            showlegend=False #no info for outlines
        ))

        #plot all the buses as circles 
        fig.add_trace(go.Scattermapbox(
            lat=route_df["Lat"].tolist(),
            lon=route_df["Lon"].tolist(),
            mode="markers",
            marker=dict(
                size=16,
                color=route_df["Deviation"].tolist(), #color by deviation 
                colorscale="RdYlGn_r",
                cmin=-10,
                cmax=10,
                colorbar=dict(title="Deviation"),
                opacity=1.0,
                symbol="circle"
            ),
            text=[ #show route name, direction, and deviation 
                f"Headsign: {h}<br>Direction: {d}<br>Deviation: {dev} min" 
                for h, d, dev in zip(route_df["TripHeadsign"], route_df["DirectionText"], route_df["Deviation"])
            ],
            hoverinfo="text",
            name="Buses",
            showlegend=False
        ))
        center_lat = route_df["Lat"].mean() #center map for display
        center_lon = route_df["Lon"].mean()

    elif route_details and "Direction0" in route_details and "Shape" in route_details["Direction0"]:
        #center in route center if no buses 
        shape = route_details["Direction0"]["Shape"]
        center_lat = sum(pt["Lat"] for pt in shape) / len(shape)
        center_lon = sum(pt["Lon"] for pt in shape) / len(shape)
    else:
        center_lat, center_lon = 38.89511, -77.03637 #center in DC if no data

    fig.update_layout( #modify style of map
        mapbox_style="open-street-map",
        mapbox_zoom=12,
        mapbox_center={"lat": center_lat, "lon": center_lon},
        height=600,
        margin=dict(l=0, r=0, t=40, b=0),
        title=f"Live Bus Locations for Route {selected_route}",
        showlegend=True
    )
    st.plotly_chart(fig, use_container_width=True)

    #show data table at the bottom for selected route 
    st.dataframe(route_df[["VehicleID", "TripHeadsign", "DateTime", "Deviation", "TripStartTime", "TripEndTime"]])

st.write("Last updated:", pd.Timestamp.utcnow()) #information on last refresh