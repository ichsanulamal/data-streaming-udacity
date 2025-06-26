## Exercise: Putting it All Together

Over the past few exercises, we have been exploring user clickstream data for an e-commerce website. In this example, we will pull our disparate data sources together.

By the end of this exercise, you will be able to see a summary, per user, of the total purchase and click amounts, in addition to the user's information. This kind of summary table is an important frequent type of analysis performed in e-commerce settings.

---

### 1. Create a table of user data and purchases, as well as a stream of click events

```sql
CREATE TABLE users (
  username VARCHAR,
  email VARCHAR,
  phone_number VARCHAR,
  address VARCHAR
) WITH (
  KAFKA_TOPIC = 'com.udacity.streams.users',
  VALUE_FORMAT = 'JSON',
  KEY = 'username'
);

CREATE STREAM clickevents (
  username VARCHAR,
  email VARCHAR,
  timestamp VARCHAR,
  uri VARCHAR,
  number INTEGER
) WITH (
  KAFKA_TOPIC = 'com.udacity.streams.clickevents',
  VALUE_FORMAT = 'JSON',
  KEY = 'username'
);

CREATE TABLE purchases (
  username VARCHAR,
  currency VARCHAR,
  amount INTEGER
) WITH (
  KAFKA_TOPIC = 'com.udacity.streams.purchases',
  VALUE_FORMAT = 'JSON',
  KEY = 'username'
);
```

---

### 2. Create a join table of user purchases with user data

```sql
CREATE TABLE user_purchases WITH (PARTITIONS = 10) AS
SELECT
  u.username AS username,
  p.amount AS purchase_amount,
  u.email AS email,
  u.phone_number AS phone_number,
  u.address AS address
FROM purchases p
JOIN users u
  ON u.username = p.username;
```

---

### 3. Create a stream joining user purchases to user clicks

```sql
CREATE STREAM user_purchases_clicks WITH (PARTITIONS = 10) AS
SELECT
  up.username AS username,
  up.purchase_amount AS purchase_amount,
  c.number AS num_clicks,
  up.email AS email,
  up.phone_number AS phone_number,
  up.address AS address
FROM clickevents c
JOIN user_purchases up
  ON up.email = c.email;
```

---

### 4. Build our aggregated output

```sql
CREATE TABLE user_activity AS
SELECT
  upc.username,
  upc.email,
  upc.phone_number,
  upc.address,
  SUM(upc.purchase_amount) AS total_purchase_value,
  SUM(num_clicks) AS total_clicks
FROM user_purchases_clicks upc
GROUP BY
  upc.username,
  upc.email,
  upc.phone_number,
  upc.address;
```

---

Once you have finished creating the table, make sure to query it to ensure that data is being produced.
