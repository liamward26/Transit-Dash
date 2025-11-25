import requests
import json
import logging
import time
import os 
from quixstreams import Application
from google.protobuf import json_format
from google.transit import gtfs_realtime_pb2

API_URL = "https://api.nationaltransport.ie/gtfsr/v2/gtfsr"
API_KEY = os.getenv("GTFS_KEY")

def fetch_data():
    response = requests.get(
        API_URL, headers={"x-api-key": API_KEY})
    
    response.raise_for_status()

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)

    return json.loads(json_format.MessageToJson(feed))

def main():
    app = Application(
        broker_address="localhost:19092",
        loglevel="DEBUG",
    )

    with app.get_producer() as producer:
        while True:
            info = fetch_data()
            logging.debug("Got info: %s", info)
            producer.produce(
                topic="bus_status",
                key="BusStatus".encode("utf-8"),
                value=json.dumps(info).encode("utf-8"),
            )
            logging.info("Produced. Sleeping...")
            time.sleep(60)


if __name__ == "__main__":
  try:
    logging.basicConfig(level="DEBUG")
    main()
  except KeyboardInterrupt:
    pass
