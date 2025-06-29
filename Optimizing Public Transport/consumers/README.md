# CTA Public Transportation Optimizer - Consumers

This project ingests, processes, and displays real-time CTA (Chicago Transit Authority) data using Kafka, Faust, Tornado, and KSQL. It is designed to demonstrate streaming data pipelines, event-driven architectures, and real-time dashboards.

---

## Directory Structure

```
consumers/
│
├── __init__.py
├── consumer.py
├── faust_stream.py
├── ksql.py
├── logging.ini
├── requirements.txt
├── server.py
├── topic_check.py
│
├── models/
│   ├── __init__.py
│   ├── line.py
│   ├── lines.py
│   ├── station.py
│   └── weather.py
│
├── stations-stream-data/
│   └── v1/
│       └── tables/
│
└── templates/
    └── status.html
```

---

## Main Components

### 1. **consumer.py**
Defines the `KafkaConsumer` class, which wraps a Kafka (or Avro) consumer for asynchronous message consumption. It handles subscribing to topics, polling for messages, and invoking message handlers.

- **Key Classes/Functions:**
  - `KafkaConsumer`: Consumes messages from Kafka topics and passes them to handler functions.
  - Handles Avro and non-Avro topics.
  - Manages partition assignment and offset configuration.

---

### 2. **faust_stream.py**
Defines a Faust stream processing application that transforms raw station data into a simplified format and writes it to a new Kafka topic.

- **Key Classes:**
  - `Station`: Faust record for input station data.
  - `TransformedStation`: Faust record for output data (station_id, station_name, order, line).
- **Key Functions:**
  - `station_event`: Faust agent that processes incoming station events and writes transformed records to a table and topic.

---

### 3. **ksql.py**
Configures and executes KSQL statements to create tables for turnstile data and turnstile summaries.

- **Key Functions:**
  - `execute_statement`: Posts KSQL statements to the KSQL REST API to create tables and summaries if they do not already exist.

---

### 4. **server.py**
Defines and runs a Tornado web server that displays real-time CTA status using data consumed from Kafka topics.

- **Key Classes:**
  - `MainHandler`: Tornado request handler that renders the status page.
- **Key Functions:**
  - `run_server`: Starts the Tornado server, initializes models, and spawns Kafka consumers.

---

### 5. **topic_check.py**
Utility for checking if a Kafka topic exists using the Kafka AdminClient.

- **Key Functions:**
  - `topic_exists(topic)`: Returns `True` if the topic exists, `False` otherwise.

---

### 6. **models/**
Contains data models for stations, lines, and weather.

- **station.py**: Defines the `Station` class, representing a CTA station and its state.
- **line.py**: Defines the `Line` class, representing a train line and its stations.
- **lines.py**: Defines the `Lines` class, aggregating all train lines.
- **weather.py**: Defines the `Weather` class, representing current weather conditions.

---

### 7. **status.html**
Jinja2/ Tornado template for rendering the CTA status dashboard. Displays weather, train lines, stations, train arrivals, and turnstile entries.

---

### 8. **requirements.txt**
Lists Python dependencies:
- `confluent-kafka[avro]`
- `faust`
- `tornado`

---

### 9. **logging.ini**
Configures logging for the application.

---

## Data Flow Overview

1. **Kafka Connect** ingests raw CTA data into Kafka topics.
2. **Faust Stream (faust_stream.py)** transforms station data and writes to a new topic.
3. **KSQL (ksql.py)** creates tables and summaries for turnstile data.
4. **Kafka Consumers (consumer.py)** consume weather, station, arrival, and turnstile summary topics.
5. **Models (`models/`)** update in-memory representations of lines, stations, and weather.
6. **Tornado Server (server.py)** serves a real-time dashboard using the latest data.

---

## Running the Application

1. **Install dependencies:**
   ```sh
   pip install -r requirements.txt
   ```

2. **Start Kafka, Schema Registry, and KSQL Server.**

3. **Run Faust Stream:**
   ```sh
   faust -A faust_stream worker -l debug
   ```

4. **Run KSQL setup:**
   ```sh
   python ksql.py
   ```

5. **Start the Tornado server:**
   ```sh
   python server.py
   ```

6. **Open your browser to** [http://localhost:8888](http://localhost:8888) **to view the dashboard.**

---

## Notes

- **Kafka Topics Used:**
  - `org.chicago.cta.weather.v1`
  - `org.chicago.cta.stations.table.v1`
  - `org.chicago.cta.station.{station_name}.arrivals.v1`
  - `TURNSTILE_SUMMARY`
- **Environment:** Requires running Kafka, Schema Registry, and KSQL locally.
- **Extensibility:** Models and consumers can be extended for additional data sources or analytics.

---

## Troubleshooting

- Ensure all Kafka services are running.
- Check topic existence with topic_check.py.
- Review logs for errors (configured via logging.ini).

