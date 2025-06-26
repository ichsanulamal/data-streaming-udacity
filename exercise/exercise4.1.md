## Kafka Connect API Guide

In this exercise, we'll explore how to interact with the Kafka Connect API using `curl` commands.

📚 [Official Kafka Connect REST API Documentation](https://docs.confluent.io/current/connect/references/restapi.html)

---

### 🔍 View Available Connector Plugins

```bash
curl http://localhost:8083/connector-plugins | python -m json.tool
```

> 💡 Tip: The `| python -m json.tool` part is used to pretty-print the JSON output. You can omit it if you prefer raw JSON.

---

### ➕ Create a Connector

Let’s create a connector. (We'll explore the details later.)

```bash
curl -X POST -H 'Content-Type: application/json' -d '{
  "name": "first-connector",
  "config": {
    "connector.class": "FileStreamSource",
    "tasks.max": 1,
    "file": "/var/log/journal/confluent-kafka-connect.service.log",
    "topic": "kafka-connect-logs"
  }
}' http://localhost:8083/connectors
```

---

### 📋 List All Connectors

```bash
curl http://localhost:8083/connectors | python -m json.tool
```

> You should see your `first-connector` listed here.

---

### 📄 Get Connector Details

```bash
curl http://localhost:8083/connectors/first-connector | python -m json.tool
```

---

### ⏸ Pause and 🔁 Restart Connectors

**Pause a connector:**

```bash
curl -X PUT http://localhost:8083/connectors/first-connector/pause
```

**Restart a connector:**

```bash
curl -X POST http://localhost:8083/connectors/first-connector/restart
```

---

### ❌ Delete a Connector

```bash
curl -X DELETE http://localhost:8083/connectors/first-connector
```

---