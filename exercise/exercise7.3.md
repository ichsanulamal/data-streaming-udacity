## Querying in KSQL

Ad-hoc querying is one of KSQL’s greatest strengths. Below are some examples demonstrating its capabilities.

---

### Basic Filtering

We've already seen filtering during table creation, but let's revisit a basic filtering example:

```sql
SELECT uri, number
  FROM clickevents
  WHERE number > 100
    AND uri LIKE 'http://www.k%';
```

This query filters for records where `number` is greater than 100 and the URI starts with `http://www.k`.

---

### Scalar Functions

KSQL provides a variety of built-in [scalar functions](https://docs.confluent.io/current/ksql/docs/developer-guide/syntax-reference.html#scalar-functions) that can transform data inline.

Here's an example using `SUBSTRING` and `UCASE`:

```sql
SELECT UCASE(SUBSTRING(uri, 12))
  FROM clickevents
  WHERE number > 100
    AND uri LIKE 'http://www.k%';
```

#### Explanation:

* `SUBSTRING(uri, 12)` removes the first 11 characters of the URI (e.g., `http://www.`).
* `UCASE(...)` converts the resulting string to uppercase.

This transformation is useful for normalizing or preparing data for further processing.

---

### Terminating Queries

> ⚠️ **Important:** `SELECT` queries in KSQL are not persistent.

When you run a `SELECT` query, it will stream results continuously. Once you interrupt it (e.g., with `CTRL+C`), the query stops and must be recreated to run again.

If you want query results to persist over time, **create a Stream or Table** instead.

