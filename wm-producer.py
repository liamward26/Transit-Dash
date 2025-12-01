import requests
import json
import logging
import time
import os 
from quixstreams import Application

#WMATA bus positions API
API_URL = "https://api.wmata.com/Bus.svc/json/jBusPositions"
API_KEY = os.getenv('WMATA_key') #get API key from env variable

def fetch_data(): #fetch curernt position data from api
    response = requests.get(
        API_URL, headers={"api_key": API_KEY})
    response.raise_for_status()
    return response.json() #return json data

def main():
    app = Application( #create quixstreams application
        broker_address="localhost:19092",
        loglevel="DEBUG",
    )
    #Create producer to poll API 
    with app.get_producer() as producer:
        while True:
            info = fetch_data()
            logging.debug("Got info: %s", info)
            producer.produce( #produce messages to topic
                topic="bus_positions",
                key="BusPositions".encode("utf-8"),
                value=json.dumps(info).encode("utf-8"),
            )
            logging.info("Produced. Sleeping...")
            time.sleep(30) #polls every 30 seconds


if __name__ == "__main__":
  try:
    logging.basicConfig(level="DEBUG")
    main()
  except KeyboardInterrupt:
    pass
