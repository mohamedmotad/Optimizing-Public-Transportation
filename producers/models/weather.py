"""Methods pertaining to weather data"""
import json
import random
import logging
import requests
import urllib.parse
from enum import IntEnum
from pathlib import Path
from models.producer import Producer


# Global Variables:
logger = logging.getLogger(__name__)
weather_topic = "org.chicago.cta.weather"


class Weather(Producer):
    """Defines a simulated weather model"""
    
    proxy_url = "http://localhost:8082"
    status = IntEnum(
        "status", "sunny partly_cloudy cloudy windy precipitation", start=0
    )

    rest_proxy_url = proxy_url

    key_schema = None
    value_schema = None

    winter_months = set((0, 1, 2, 3, 10, 11))
    summer_months = set((6, 7, 8))

    def __init__(self, month):
        # TODO: Complete the below by deciding on a topic name, number of partitions, and number of
        # replicas
        super().__init__(
            weather_topic,
            key_schema=Weather.key_schema,
            value_schema=Weather.value_schema,
        )
        # Done
        self.status = Weather.status.sunny
        self.temp = 70.0
        if month in Weather.winter_months:
            self.temp = 40.0
        elif month in Weather.summer_months:
            self.temp = 85.0

        if Weather.key_schema is None:
            with open(f"{Path(__file__).parents[0]}/schemas/weather_key.json") as f:
                Weather.key_schema = json.load(f)

        # TODO: Define this value schema in `schemas/weather_value.json
        if Weather.value_schema is None:
            with open(f"{Path(__file__).parents[0]}/schemas/weather_value.json") as f:
                Weather.value_schema = json.load(f)
        # Done
        
    def _set_weather(self, month):
        """Returns the current weather"""
        mode = 0.0
        if month in Weather.winter_months:
            mode = -1.0
        elif month in Weather.summer_months:
            mode = 1.0
        self.temp += min(max(-20.0, random.triangular(-10.0, 10.0, mode)), 100.0)
        self.status = random.choice(list(Weather.status))

    def run(self, month):
        self._set_weather(month)
        
        # TODO: Complete the function by posting a weather event to REST Proxy. Make sure to
        # specify the Avro schemas and verify that you are using the correct Content-Type header.
        # Done
        weather_data = {
            "key_schema": json.dumps(Weather.key_schema),
            "value_schema": json.dumps(Weather.value_schema),
                    'records': [{
                        'key': {'timestamp': self.time_millis()},
                        'value': {'temperature': self.temp, 'status': self.status.name}
                    }]
        }
        resp = requests.post(
            f"{Weather.rest_proxy_url}/topics/{self.topic_name}",
            headers={"Content-Type": "application/vnd.kafka.avro.v2+json"},
            data=json.dumps(weather_data),
        )
        #logger.info(f"{Weather.rest_proxy_url}/topics/{self.topic_name}")
        #logger.info(proxy_data)
        logger.info(f"[Producer] {resp}")
        resp.raise_for_status()
        #logger.info("[Producer] weather runing...")

        logger.debug(
            "sent weather data to kafka, temp: %s, status: %s",
            self.temp,
            self.status.name,
        )
