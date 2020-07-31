"""Defines trends calculations for stations"""
import faust
import logging


# Global Variables:
logger = logging.getLogger(__name__)
broker_url = "kafka://localhost:9092"
input_topic = "org.chicago.cta.stations"
output_topic = "org.chicago.cta.stations.table"
faust_table_topic = "org.chicago.cta.stations.table"


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


# TODO: Define a Faust Stream that ingests data from the Kafka Connect stations topic and
#   places it into a new topic with only the necessary information.
app = faust.App('faust-station', broker=broker_url, store="memory://")
# TODO: Define the input Kafka Topic. Hint: What topic did Kafka Connect output to?
# topic = app.topic("TODO", value_type=Station)
topic = app.topic(input_topic, value_type=Station)

# TODO: Define the output Kafka Topic
# out_topic = app.topic("TODO", partitions=1)
out_topic = app.topic(output_topic, partitions=1)

# TODO: Define a Faust Table
table = app.Table(
    name=faust_table_topic,
    default=TransformedStation,
    partitions=1,
    changelog_topic=out_topic,
)


#
#
# TODO: Using Faust, transform input `Station` records into `TransformedStation` records. Note that
# "line" is the color of the station. So if the `Station` record has the field `red` set to true,
# then you would set the `line` of the `TransformedStation` record to the string `"red"`
# Done
@app.agent(topic)
async def station_event(events):
    async for e in events:
        if e.red:
            line = 'red'
        elif e.blue:
            line = 'blue'
        else:
            line = 'green'

        transformed_station = TransformedStation(e.station_id, e.stop_name, e.order, line)
        table[transformed_station.station_id] = transformed_station


if __name__ == "__main__":
    app.main()
