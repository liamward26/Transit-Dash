import json
import logging
from quixstreams import Application
import pandas as pd
import duckdb

TOPIC = "bus_status"   
con = duckdb.connect("bus_data.duckdb")

con.execute("""
CREATE TABLE IF NOT EXISTS bus_updates (
    entity_id TEXT,
    entity_type TEXT,
    trip_id TEXT,
    route_id TEXT,
    stop_id TEXT,
    stop_sequence INTEGER,
    arrival_delay INTEGER,
    departure_delay INTEGER,
    latitude DOUBLE,
    longitude DOUBLE,
    timestamp BIGINT
);
""")

#Create lookup table of stops
con.execute("""
    CREATE OR REPLACE TABLE stops AS
    SELECT *
    FROM read_csv_auto('stops.txt')
""")

con.execute("""
    UPDATE bus_updates AS g
    SET latitude = s.stop_lat,
        longitude = s.stop_lon
    FROM stops AS s
    WHERE g.stop_id = s.stop_id
    AND g.entity_type = 'trip_update';
""")

def process_message(message):
    value = message.value().decode() if message.value() else None

    if not value:
        logging.warning("Received empty message, skipping")
        return

    try:
        data = json.loads(value)  
    except Exception as e:
        logging.error(f"Could not parse message JSON: {e}")
        return
    
    if "entity" not in data:
        logging.warning("No entities in message")
        return

    rows = []

    for ent in data["entity"]:
        entity_id = ent.get("id")

        if "tripUpdate" in ent:
            tu = ent["tripUpdate"]
            trip = tu.get("trip", {})

            for stu in tu.get("stopTimeUpdate", []):
                rows.append({
                    "entity_id": entity_id,
                    "entity_type": "trip_update",
                    "trip_id": trip.get("tripId"),
                    "route_id": trip.get("routeId"),
                    "stop_id": stu.get("stopId"),
                    "stop_sequence": stu.get("stopSequence"),
                    "arrival_delay": stu.get("arrival", {}).get("delay"),
                    "departure_delay": stu.get("departure", {}).get("delay"),
                    "latitude": None,
                    "longitude": None,
                    "timestamp": tu.get("timestamp"),
                })

    if not rows:
        return
    
    df = pd.DataFrame(rows)
    con.register("tmp", df)
    
    con.execute("""
        INSERT INTO bus_updates
        SELECT tmp.entity_id,
               tmp.entity_type,
               tmp.trip_id,
               tmp.route_id,
               tmp.stop_id,
               tmp.stop_sequence,
               tmp.arrival_delay,
               tmp.departure_delay,
               s.stop_lat AS latitude,
               s.stop_lon AS longitude,
               tmp.timestamp
        FROM tmp
        LEFT JOIN stops AS s
        ON tmp.stop_id = s.stop_id
    """)
    
    logging.info(f"Inserted {len(rows)} rows")


def main():

    app = Application(
        broker_address="localhost:19092", 
        consumer_group="gtfs-consumer",
        auto_offset_reset="earliest",
        loglevel="DEBUG"
    )

    consumer = app.get_consumer()
    consumer.subscribe([TOPIC])

    logging.info(f"Consuming from topic: {TOPIC}")

    while True:
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            continue  

        if msg.error():
            logging.error(f"Kafka error: {msg.error()}")
            continue

        process_message(msg)
        consumer.commit(msg)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    try:
        main()
    except KeyboardInterrupt:
        print("\nShutting down...")
