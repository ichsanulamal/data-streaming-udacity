"""Defines trends calculations for stations"""

import logging

import faust

logger = logging.getLogger(__name__)


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


app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")

topic = app.topic("org.chicago.cta.stations", value_type=Station)

out_topic = app.topic("org.chicago.cta.stations.table.v1", partitions=1)

table = app.Table(
    name="org.chicago.cta.stations.table.v1",
    default=TransformedStation,
    partitions=1,
    changelog_topic=out_topic,
)


@app.agent(topic)
async def station_event(events):
    async for e in events:
        if e.red:
            line = "red"
        elif e.blue:
            line = "blue"
        else:
            line = "green"

        transformed_station = TransformedStation(
            e.station_id, e.stop_name, e.order, line
        )
        table[transformed_station.station_id] = transformed_station


if __name__ == "__main__":
    app.main()
