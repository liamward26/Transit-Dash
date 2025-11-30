import json
import logging
from quixstreams import Application
import pandas as pd
import duckdb

TOPIC = "bus_positions"   

def process_message(message):
    value = message.value()

    if not value:
        logging.warning("Received empty message, skipping")
        return
    try:
        data = json.loads(value.decode())
    except Exception as e:
        logging.error(f"Could not parse message JSON: {e}")
        return

    df = pd.DataFrame(data["BusPositions"])
    
    try:
        with duckdb.connect("bus_positions.duckdb") as con:
            con.execute("""
                CREATE TABLE IF NOT EXISTS bus_positions (
                    VehicleID INT,
                    Lat DOUBLE,
                    Lon DOUBLE,
                    Deviation INT,
                    DateTime TEXT,
                    TripID TEXT,
                    RouteID TEXT,
                    DirectionNum INT,
                    DirectionText TEXT,
                    TripHeadsign TEXT,
                    TripStartTime TEXT,
                    TripEndTime TEXT,
                    BlockNumber TEXT)
            """)
            con.register("df_view", df)
            con.execute("INSERT INTO bus_positions BY NAME SELECT * FROM df_view")
            logging.info(f"Inserted {len(df)} rows")
    except Exception as e:
        logging.error(f"DuckDB insert error: {e}")


def main():
    app = Application(
        broker_address="localhost:19092", 
        consumer_group="wmata-consumer",
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
