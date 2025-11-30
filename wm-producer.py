import requests
import json
import logging
import time
from quixstreams import Application

API_URL = "https://api.wmata.com/Bus.svc/json/jBusPositions"
API_KEY = ""

def fetch_data():
    response = requests.get(
        API_URL, headers={"api_key": API_KEY})
    response.raise_for_status()
    return response.json()

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
                topic="bus_positions",
                key="BusPositions".encode("utf-8"),
                value=json.dumps(info).encode("utf-8"),
            )
            logging.info("Produced. Sleeping...")
            time.sleep(30)


if __name__ == "__main__":
  try:
    logging.basicConfig(level="DEBUG")
    main()
  except KeyboardInterrupt:
    pass
