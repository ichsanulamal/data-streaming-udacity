## Hopping and Tumbling Windows

In this demonstration, we’ll explore how to create **Tables with windowing enabled** in ksqlDB.

---

### Tumbling Windows

Tumbling windows are non-overlapping, fixed-size time windows. Let's create a tumbling window over the `clickevents` stream, with a window size of **30 seconds**:

```sql
CREATE STREAM clickevents_tumbling AS
  SELECT * FROM clickevents
  WINDOW TUMBLING (SIZE 30 SECONDS);
```

---

### Hopping Windows

Hopping windows are fixed-size windows that **overlap**. Now we'll create a **hopping windowed table** that groups events in 30-second windows, advancing every 5 seconds:

```sql
CREATE TABLE clickevents_hopping AS
  SELECT uri FROM clickevents
  WINDOW HOPPING (SIZE 30 SECONDS, ADVANCE BY 5 SECONDS)
  WHERE uri LIKE 'http://www.b%'
  GROUP BY uri;
```

This configuration creates overlapping windows. If you query this table, you will see the associated **window start and end times** for each group.

---

### Session Windows

Session windows group events based on periods of **inactivity**. Let’s create a session window of **5 minutes** to group events that happen close in time:

```sql
CREATE TABLE clickevents_session AS
  SELECT uri FROM clickevents
  WINDOW SESSION (5 MINUTES)
  WHERE uri LIKE 'http://www.b%'
  GROUP BY uri;
```

This setup will group together related user interactions that occur within a 5-minute inactivity gap.

