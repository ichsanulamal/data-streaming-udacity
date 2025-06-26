## 📊 KSQL Aggregates

KSQL provides a number of useful aggregate functions, such as `MAX`, `MIN`, `SUM`, `COUNT`, and more.

In this guide, we'll explore how to use these functions to create aggregated **tables** from KSQL queries.

---

### 🔢 `SUM`

Let’s start by summarizing `clickevents` by `uri` using the `SUM` function:

```sql
SELECT uri, SUM(number)
FROM clickevents
GROUP BY uri;
```

This query returns a continuous output stream where each row represents the **total sum** of `number` for each `uri` observed so far.

> 💡 **Note:** The output continuously updates. This is expected — it reflects a **table** that updates in real-time. You can persist this as a table and query it periodically if preferred.

---

### 📊 `HISTOGRAM`

The `HISTOGRAM` function allows you to count how many times each distinct value appears.

Let’s build on the previous example to include both the total `number` and the frequency of each `uri`:

```sql
SELECT uri,
       SUM(number) AS total_number,
       HISTOGRAM(uri) AS num_uri
FROM clickevents
GROUP BY uri;
```

This returns both the **cumulative total** and the **distribution** of `uri` values seen.

---

### 🏆 `TOPK`

The `TOPK` function identifies the top **K** most frequent or largest values within a window.

Here’s how to define a **tumbling window** of 30 seconds and select the top 5 values of `number` per `uri`:

```sql
SELECT uri,
       TOPK(number, 5)
FROM clickevents
WINDOW TUMBLING (SIZE 30 SECONDS)
GROUP BY uri;
```

> ⏳ **Tumbling Windows:** These are non-overlapping windows. Every 30 seconds, a new window starts. The output will reset and reflect the new top 5 values for each window interval.
