"""Configures a Kafka Connector for Postgres Station data"""
import json
import logging
import requests

# Global Variables:
topic_prefix = "org.chicago.cta."
connetcor_name = "station"
logger = logging.getLogger(__name__)
kafka_connect_url = "http://localhost:8083/connectors"


def configure_connector():
    """Starts and configures the Kafka Connect connector"""
    logging.debug("creating or updating kafka connect connector...")

    resp = requests.get(f"{kafka_connect_url}/{connetcor_name}")
    if resp.status_code == 200:
        logging.info("connector already created skipping recreation")
        return

    # TODO: Complete the Kafka Connect Config below.
    # Directions: Use the JDBC Source Connector to connect to Postgres. Load the `stations` table
    # using incrementing mode, with `stop_id` as the incrementing column name.
    # Make sure to think about what an appropriate topic prefix would be, and how frequently Kafka
    # Connect should run this connector (hint: not very often!)
    resp = requests.post(
       kafka_connect_url,
       headers={"Content-Type": "application/json"},
       data=json.dumps({
           "name": connetcor_name,
           "config": {
               "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
               "key.converter": "org.apache.kafka.connect.json.JsonConverter",
               "key.converter.schemas.enable": "false",
               "value.converter": "org.apache.kafka.connect.json.JsonConverter",
               "value.converter.schemas.enable": "false",
               "batch.max.rows": "500",
               "connection.url": "jdbc:postgresql://localhost:5432/cta",
               "connection.user": "cta_admin",
               "connection.password": "chicago",
               "table.whitelist": "stations",
               "mode": "incrementing",
               "incrementing.column.name": "stop_id",
               "topic.prefix": topic_prefix
               #"poll.interval.ms": 1000 * 60 * 5,
           }
       }),
    )

    # Ensure a healthy response was given
    resp.raise_for_status()
    logging.debug("connector created successfully")


if __name__ == "__main__":
    configure_connector()