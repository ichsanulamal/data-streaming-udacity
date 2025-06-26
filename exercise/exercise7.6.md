## 🔗 KSQL JOINs

KSQL supports various types of `JOIN` operations. In this demonstration, we’ll explore how to join the `clickevents` **stream** with the `pages` **table**.

> ⚠️ **Important**:
> You **cannot** join a **Table** to a **Stream** in KSQL.
> You may **only** join a **Stream** to a **Table**.

---

### 🟢 LEFT OUTER JOIN (Default)

In KSQL, as in most SQL dialects, the default `JOIN` is a `LEFT OUTER JOIN`.

Let's perform a `LEFT OUTER JOIN` from the `clickevents` stream to the `pages` table:

```sql
CREATE STREAM clickevent_pages AS
  SELECT ce.uri, ce.email, ce.timestamp, ce.number, p.description, p.created
  FROM clickevents ce
  JOIN pages p ON ce.uri = p.uri;
```

---

### 🔵 INNER JOIN

KSQL also supports `INNER JOIN` between **Streams and Tables**:

```sql
CREATE STREAM clickevent_pages_inner AS
  SELECT ce.uri, ce.email, ce.timestamp, ce.number, p.description, p.created
  FROM clickevents ce
  INNER JOIN pages p ON ce.uri = p.uri;
```

---

### 🔴 FULL OUTER JOIN

Finally, let’s look at the `FULL OUTER JOIN` — the third type of join supported by KSQL:

```sql
CREATE STREAM clickevent_pages_outer AS
  SELECT ce.uri, ce.email, ce.timestamp, ce.number, p.description, p.created
  FROM clickevents ce
  FULL OUTER JOIN pages p ON ce.uri = p.uri;
```

However, this query **will fail** with the following error:

```
> Full outer joins between streams and tables (stream: left, table: right) are not supported.
```

> ❌ **Note**:
> `FULL OUTER JOIN` is **only supported** when joining:
>
> * A **Stream to a Stream**
> * A **Table to a Table**
