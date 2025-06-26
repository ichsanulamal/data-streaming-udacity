# 🛠️ Kafka CLI Tools

In this exercise, you’ll learn how to use some of the most common Kafka CLI tools.

---

## 📜 Listing Topics

To list existing topics in Kafka:

```bash
kafka-topics --list --zookeeper localhost:2181
```

You should see output similar to:

```
__confluent.support.metrics
__consumer_offsets
_confluent-ksql-ksql_service_docker_command_topic
_schemas
connect-config
connect-offset
connect-status
```

* `--list`: Lists all topics.
* `--zookeeper localhost:2181`: Specifies the Zookeeper address Kafka is using.

> ⚠️ **Note:** In newer Kafka versions, `--zookeeper` is deprecated. Use `--bootstrap-server` instead.

These are system topics automatically created by Kafka and related tools.

---

## 🧱 Creating a Topic

Create a new topic:

```bash
kafka-topics --create --topic "my-first-topic" --partitions 1 --replication-factor 1 --zookeeper localhost:2181
```

* `--topic "my-first-topic"`: Name of your topic.
* `--partitions 1`: Number of partitions.
* `--replication-factor 1`: Number of replicas (for fault tolerance).

> ✅ Kafka should return nothing if the topic was created successfully.

Verify topic creation:

```bash
kafka-topics --list --zookeeper localhost:2181 --topic "my-first-topic"
```

Expected output:

```
my-first-topic
```

---

## ✉️ Producing Data

Add data to your topic:

```bash
kafka-console-producer --topic "my-first-topic" --broker-list PLAINTEXT://localhost:9092
```

* `--broker-list`: Specifies where Kafka is running.

You’ll be dropped into an interactive terminal. Try typing messages:

```
>hello
>world!
>my
>first
>kafka
>events!
```

---

## 📥 Consuming Data

Open a new terminal and run:

```bash
kafka-console-consumer --topic "my-first-topic" --bootstrap-server PLAINTEXT://localhost:9092
```

> You won't see past messages—Kafka only sends new messages by default.

Go back to the producer and add a few more messages. You’ll see them appear here in real time.

---

### 🔁 Consume from Beginning

To read **all** messages from the topic:

```bash
kafka-console-consumer --topic "my-first-topic" --bootstrap-server PLAINTEXT://localhost:9092 --from-beginning
```

Example output:

```
hello
world!
my
first
kafka
events!
hello again!
```

---

## 🧹 Deleting a Topic

Delete the topic:

```bash
kafka-topics --delete --topic "my-first-topic" --bootstrap-server PLAINTEXT://localhost:9092
```

> Kafka will not print any confirmation if the delete is successful.

Verify deletion:

```bash
kafka-topics --list --zookeeper localhost:2181
```

`my-first-topic` should no longer be listed.

---
