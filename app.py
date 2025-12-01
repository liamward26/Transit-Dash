import duckdb
import pandas as pd
import streamlit as st
import plotly.graph_objects as go
import requests
from streamlit_autorefresh import st_autorefresh

DB_PATH = "bus_positions.duckdb"
WMATA_API_KEY = ""  # <-- Replace with your WMATA API key
WMATA_ROUTE_DETAILS_URL = "https://api.wmata.com/Bus.svc/json/jRouteDetails"
refresh_ms = 30000

st.set_page_config(
    page_title="WMATA Live Bus Positions",
    layout="wide"
)

count = st_autorefresh(interval=refresh_ms, limit=None, key="bus_refresh")

# Fetch latest data
try:
    con = duckdb.connect(DB_PATH, read_only=True)
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
    """Fetch route shape and info from WMATA API."""
    try:
        resp = requests.get(
            WMATA_ROUTE_DETAILS_URL,
            params={"RouteID": route_id},
            headers={"api_key": WMATA_API_KEY},
            timeout=10
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        st.warning(f"Could not fetch route details: {e}")
        return None

if df.empty:
    st.warning("No bus data available yet. Waiting for consumer")
else:
    # Route selection
    route_options = sorted(df["RouteID"].dropna().unique())
    selected_route = st.selectbox("Select a Bus Route", options=route_options)

    # Fetch route details from WMATA API
    route_details = get_route_details(selected_route)
    route_name = route_details["Name"] if route_details and "Name" in route_details else selected_route
    route_desc = route_details["Description"] if route_details and "Description" in route_details else ""

    st.markdown(f"### {route_name}")
    if route_desc:
        st.markdown(f"*{route_desc}*")

    # Filter for selected route
    route_df = df[df["RouteID"] == selected_route]

    st.markdown(f"#### Buses on Route {selected_route} ({len(route_df)} active)")

    # Deviation summary
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

    # Prepare map
    fig = go.Figure()

    # Add route shape (both directions if available)
    if route_details:
        for direction in ["Direction0", "Direction1"]:
            if direction in route_details and "Shape" in route_details[direction]:
                shape = route_details[direction]["Shape"]
                if shape:
                    lats = [pt["Lat"] for pt in shape]
                    lons = [pt["Lon"] for pt in shape]
                    fig.add_trace(go.Scattermapbox(
                        lat=lats,
                        lon=lons,
                        mode="lines",
                        line=dict(width=4, color="blue"),
                        name=f"Route {selected_route} {direction[-1]}",
                        showlegend=False,      # Hide legend for route lines
                        hoverinfo="skip"       # Disable hover for route lines
                    ))

    if not route_df.empty:
        # Plot black outlines for all buses
        fig.add_trace(go.Scattermapbox(
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
            showlegend=False  # Ensure outline does not appear in legend
        ))

        # Plot all buses as circles colored by deviation, with DirectionText in hover
        fig.add_trace(go.Scattermapbox(
            lat=route_df["Lat"].tolist(),
            lon=route_df["Lon"].tolist(),
            mode="markers",
            marker=dict(
                size=16,
                color=route_df["Deviation"].tolist(),
                colorscale="RdYlGn_r",
                cmin=-10,
                cmax=10,
                colorbar=dict(title="Deviation"),
                opacity=1.0,
                symbol="circle"
            ),
            text=[
                f"Headsign: {h}<br>Direction: {d}<br>Deviation: {dev} min" 
                for h, d, dev in zip(route_df["TripHeadsign"], route_df["DirectionText"], route_df["Deviation"])
            ],
            hoverinfo="text",
            name="Buses",
            showlegend=False
        ))
        center_lat = route_df["Lat"].mean()
        center_lon = route_df["Lon"].mean()

    elif route_details and "Direction0" in route_details and "Shape" in route_details["Direction0"]:
        # Fallback: center on route shape
        shape = route_details["Direction0"]["Shape"]
        center_lat = sum(pt["Lat"] for pt in shape) / len(shape)
        center_lon = sum(pt["Lon"] for pt in shape) / len(shape)
    else:
        center_lat, center_lon = 38.89511, -77.03637  # DC center

    fig.update_layout(
        mapbox_style="open-street-map",
        mapbox_zoom=12,
        mapbox_center={"lat": center_lat, "lon": center_lon},
        height=600,
        margin=dict(l=0, r=0, t=40, b=0),
        title=f"Live Bus Locations for Route {selected_route}",
        showlegend=True
    )
    st.plotly_chart(fig, use_container_width=True)

    st.dataframe(route_df[["VehicleID", "TripHeadsign", "DateTime", "Deviation", "TripStartTime", "TripEndTime"]])

st.write("Last updated:", pd.Timestamp.utcnow())