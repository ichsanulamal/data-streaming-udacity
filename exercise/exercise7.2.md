## Creating a Table in KSQL

Creating a table in KSQL is very similar to creating a stream. In this guide, you’ll learn the syntax and the key differences between tables and streams in KSQL.

---

### Managing Offsets

By default, KSQL consumes data from the **latest offset**, like all Kafka consumers. This behavior might not be ideal in some cases.

For example, if we want our table to include **all existing data**, we need KSQL to start reading from the **earliest offset**. To achieve this, use the `SET` command **before running any other commands**:

```sql
SET 'auto.offset.reset' = 'earliest';
```

> 💡 You can also configure this setting at the KSQL server level if desired.

Once you're finished and want to revert to the default behavior, simply run:

```sql
UNSET 'auto.offset.reset';
```

---

### Creating a Table

Creating a table follows a similar syntax to creating a stream. Here's how to create a `pages` table:

```sql
CREATE TABLE pages (
  uri VARCHAR,
  description VARCHAR,
  created VARCHAR
) WITH (
  KAFKA_TOPIC = 'com.udacity.streams.pages',
  VALUE_FORMAT = 'JSON',
  KEY = 'uri'
);
```

**Note:** The `KEY` field is new here. It specifies the column that uniquely identifies each record.
Tables in KSQL retain only the **latest** value for a given key, not the entire history.

---

### Creating a Table from a Query

Tables can also be created from queries, just like streams. For example, to create a table of all pages whose URLs start with the letter `a`:

```sql
CREATE TABLE a_pages AS
  SELECT * FROM pages
  WHERE uri LIKE 'http://www.a%';
```

---

### Describing Tables and Streams

KSQL provides useful introspection tools. To inspect a table or stream, use the `DESCRIBE` command:

```sql
DESCRIBE pages;
```

**Example Output:**

```
Name                 : PAGES
 Field       | Type
-----------------------------------------
 ROWTIME     | BIGINT           (system)
 ROWKEY      | VARCHAR(STRING)  (system)
 URI         | VARCHAR(STRING)
 DESCRIPTION | VARCHAR(STRING)
 CREATED     | VARCHAR(STRING)
-----------------------------------------
For runtime statistics and query details run: DESCRIBE EXTENDED <Stream|Table>;
```

This helps you understand the schema and types of each column in your table or stream.

---

### Deleting a Table

To delete a table, you must first terminate the query that created it.

1. **Find the query:**

```sql
SHOW QUERIES;
```

Sample output:

```
 Query ID         | Kafka Topic | Query String
---------------------------------------------------------------
 CTAS_A_PAGES_1   | A_PAGES     | CREATE TABLE a_pages AS ...
---------------------------------------------------------------
```

2. **Terminate the query and drop the table:**

```sql
TERMINATE CTAS_A_PAGES_1;
DROP TABLE a_pages;
```

