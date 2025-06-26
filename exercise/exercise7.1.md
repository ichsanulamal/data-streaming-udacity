## Creating a Stream in KSQL

In this demonstration, you'll learn how to create KSQL streams using various methods, and how to delete them afterward.

We'll work with two Kafka topics throughout this lesson:

* `com.udacity.streams.clickevents`
* `com.udacity.streams.pages`

### Topic Data Shapes

#### `com.udacity.streams.pages`

* **Key**: `<uri: string>`
* **Value**:

  ```json
  {
    "uri": "<string>",
    "description": "<string>",
    "created": "<string>"
  }
  ```

#### `com.udacity.streams.clickevents`

* **Key**: `<uri: string>`
* **Value**:

  ```json
  {
    "email": "<string>",
    "timestamp": "<string>",
    "uri": "<string>",
    "number": <int>
  }
  ```

---

## 1. Launching the KSQL CLI

First, start the KSQL CLI:

```bash
root@c9827c86286f:/home/workspace# ksql
```

Sample output:

```
                  ===========================================
                  =  Streaming SQL Engine for Apache Kafka® =
                  ===========================================

CLI v5.1.3, Server v5.1.3 located at http://localhost:8088

ksql>
```

---

## 2. Viewing Available Kafka Topics

To see all topics:

```sql
SHOW TOPICS;
```

Sample output:

```
 Kafka Topic                     | Registered | Partitions | Partition Replicas | Consumers | ConsumerGroups
-------------------------------------------------------------------------------------------------------------
 _confluent-metrics              | false      | 12         | 1                  | 0         | 0
 _schemas                        | false      | 1          | 1                  | 0         | 0
 com.udacity.streams.clickevents | false      | 1          | 1                  | 0         | 0
 com.udacity.streams.pages       | false      | 1          | 1                  | 0         | 0
...
```

✅ You should see both `com.udacity.streams.clickevents` and `com.udacity.streams.pages`.

---

## 3. Creating a Stream

Let's create a stream based on `com.udacity.streams.clickevents`:

```sql
CREATE STREAM clickevents (
  email VARCHAR,
  timestamp VARCHAR,
  uri VARCHAR,
  number INTEGER
) WITH (
  KAFKA_TOPIC='com.udacity.streams.clickevents',
  VALUE_FORMAT='JSON'
);
```

---

## 4. Viewing Available Streams

To list all existing streams:

```sql
SHOW STREAMS;
```

Sample output:

```
 Stream Name | Kafka Topic                     | Format
-------------------------------------------------------
 CLICKEVENTS | com.udacity.streams.CLICKEVENTS | JSON
-------------------------------------------------------
```

---

## 5. Creating a Stream with a Query

KSQL allows you to create streams from queries. For example, to create a stream of popular URIs:

```sql
CREATE STREAM popular_uris AS
  SELECT * FROM clickevents
  WHERE number >= 100;
```

This stream filters `clickevents` where the `number` field is 100 or greater.

---

## 6. Deleting a Stream

To delete a stream:

```sql
DROP STREAM popular_uris;
```

If you see an error like:

```
Cannot drop POPULAR_URIS.
The following queries write into this source: [CSAS_POPULAR_URIS_0].
You need to terminate them before dropping POPULAR_URIS.
```

Terminate the query first:

```sql
TERMINATE CSAS_POPULAR_URIS_0;
DROP STREAM popular_uris;
```

✅ The stream and its associated query have now been deleted.

---

## 7. Topic Management

Let’s inspect the topics again:

```sql
SHOW TOPICS;
```

You’ll notice something interesting:

```
 Kafka Topic     | ... | Registered
------------------------------------
 POPULAR_URIS    | ... | false
...
```

* ✅ `POPULAR_URIS` was created as a new Kafka topic, because the stream transformed the data.
* ❌ `CLICKEVENTS` didn’t result in a new topic — it simply wraps the existing one.

To remove `POPULAR_URIS` completely, you must delete the topic manually if needed.

---
