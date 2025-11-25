import duckdb
import pandas as pd
import plotly.express as px

con = duckdb.connect("bus_data.duckdb")

df = con.execute("""
    SELECT trip_id, stop_id, latitude, longitude, timestamp
    FROM bus_updates
    WHERE latitude IS NOT NULL AND longitude IS NOT NULL
""").fetchdf()

fig = px.scatter_map(
    df,
    lat="latitude",
    lon="longitude",
    hover_name="trip_id",
    hover_data=["stop_id", "timestamp"],
    height=600,
    zoom=12
)

fig.update_layout(title="Bus Stops and Trip Updates")

fig.show()
fig.write_html("bus_map.html")
print("Map saved to bus_map.html")