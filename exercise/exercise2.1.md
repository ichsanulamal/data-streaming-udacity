## 🔹 **Topic Storage and Directory Structure**

### **Create Topic**

```bash
kafka-topics --create --topic kafka-arch --partitions 1 --replication-factor 1 --zookeeper localhost:2181
```

This command creates a topic named `kafka-arch` with:

* **1 partition**
* **1 replica**
* **Zookeeper** managing metadata

---

### **Inspect the Directory**

```bash
ls -alh /var/lib/kafka/data | grep kafka-arch
```

#### 🔍 Expected Output (Before Producing Data):

You should see a directory like:

```
drwxr-xr-x 2 kafka kafka 4.0K Jun 16 10:00 kafka-arch-0
```

* `kafka-arch-0` means it's the 0th partition of the topic `kafka-arch`.

### **Inside the Partition Directory**

```bash
ls -alh /var/lib/kafka/data/kafka-arch-0
```

#### 🔍 Output:

```
-rw-r--r-- 1 kafka kafka  0 Jun 16 10:00 00000000000000000000.log
```

#### 🧠 What's inside?

* `.log` files: Kafka's segment files containing message data.
* `.index` files: Store mapping of offsets to physical positions in the log.
* `.timeindex`: Stores timestamps for efficient time-based lookups.

Since we haven't produced any data yet, the log file size is 0 bytes.

---

## 🔹 **Produce Data**

```bash
kafka-console-producer --topic "kafka-arch" --broker-list localhost:9092
```

Type in 5–10 messages like:

```
Message 1
Message 2
...
Message 10
```

Then hit `Ctrl+C`.

---

## 🔹 **Inspect Directory After Producing Data**

```bash
ls -alh /var/lib/kafka/data/kafka-arch-0
```

#### 🔍 New Output:

```
-rw-r--r-- 1 kafka kafka  3.5K Jun 16 10:05 00000000000000000000.log
-rw-r--r-- 1 kafka kafka  1.0K Jun 16 10:05 00000000000000000000.index
-rw-r--r-- 1 kafka kafka  512B Jun 16 10:05 00000000000000000000.timeindex
```

* Now the `.log` file has data.
* Try opening it with:

  ```bash
  cat /var/lib/kafka/data/kafka-arch-0/00000000000000000000.log
  ```

  You might see gibberish — it's in a binary format. Use Kafka tools to read it properly.

---

## 🔹 **Add More Partitions**

```bash
kafka-topics --alter --topic kafka-arch --partitions 3 --zookeeper localhost:2181
```

### **Check Directories Again**

```bash
ls -alh /var/lib/kafka/data | grep kafka-arch
```

#### 🔍 Expected Output:

```
drwxr-xr-x 2 kafka kafka 4.0K kafka-arch-0
drwxr-xr-x 2 kafka kafka 4.0K kafka-arch-1
drwxr-xr-x 2 kafka kafka 4.0K kafka-arch-2
```

You now have **three partition directories** — each one will eventually store its own log files.

---

## 🔁 **Try Modifying Partitions Further**

Kafka allows increasing the number of partitions (not decreasing).

```bash
kafka-topics --alter --topic kafka-arch --partitions 5 --zookeeper localhost:2181
```

Re-run the directory check and observe the newly created folders (`kafka-arch-3`, `kafka-arch-4`, etc.).

---

## ✅ Summary: What You’ve Learned

* **Kafka stores each partition as a folder** with segment files (.log, .index, .timeindex).
* **No data? → 0-byte log files**.
* **After producing data? → log files grow**.
* **Partition count = number of folders per topic**.
* **Kafka does not remove partitions**, only adds more.

Let me know if you'd like to explore the contents using Kafka tools or scripts to consume/inspect data!
