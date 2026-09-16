# Day 8 – Advanced Deduplication Using Window Functions

## 📌 Objective

The objective of Day 8 is to understand how to handle **duplicate customer records** and identify the **latest record for each customer** using PySpark Window Functions.

In real-world data pipelines, the same customer can appear multiple times because of updates or multiple incoming batches.

To solve this, we use:

* Window Functions
* `PARTITION BY`
* `ORDER BY`
* `ROW_NUMBER()`
* `UpdatedAt`
* `BatchId`

The latest record for each customer is retained in the Silver layer.

---

## 🏗️ Project Flow

```text id="z5y5u2"
Incoming Customer Updates
          ↓
Bronze Layer
bronze.day8_customer_updates
          ↓
Duplicate Detection
          ↓
Window Function
          ↓
PARTITION BY CustomerId
          ↓
ORDER BY UpdatedAt DESC
          ↓
ROW_NUMBER()
          ↓
Keep row_num = 1
          ↓
Silver Layer
silver.day8_customers
```

---

## 📂 Tables Used

### Bronze Table

```text id="t5m8y2"
bronze.day8_customer_updates
```

Stores incoming customer update records, including duplicate Customer IDs.

### Silver Table

```text id="m2e4u8"
silver.day8_customers
```

Stores the latest record for each Customer ID after deduplication.

---

## 1. Create Incoming Customer Data

Customer data was created with multiple records for the same Customer IDs to simulate repeated updates.

```python id="3o8v8m"
customers_day8 = [
    (1, "Arjun", "Chennai", 29, "arjun@gmail.com", "2026-09-15 09:00:00"),
    (2, "Priya", "Bangalore", 26, "priya@gmail.com", "2026-09-15 09:05:00"),
    (1, "Arjun", "Bangalore", 30, "arjun@gmail.com", "2026-09-15 10:00:00"),
    (3, "Michael", "Hyderabad", 34, "michael@gmail.com", "2026-09-15 10:10:00"),
    (1, "Arjun", "Hyderabad", 31, "arjun@gmail.com", "2026-09-15 11:00:00"),
    (4, "Neha", "Coimbatore", 31, "neha@gmail.com", "2026-09-15 11:15:00"),
    (2, "Priya", "Chennai", 27, "priya@gmail.com", "2026-09-15 11:30:00"),
    (5, "Vikram", "Madurai", 38, "vikram@gmail.com", "2026-09-15 11:45:00")
]

columns_day8 = [
    "CustomerId",
    "CustomerName",
    "City",
    "Age",
    "Email",
    "UpdatedAt"
]

df_day8 = spark.createDataFrame(
    customers_day8,
    columns_day8
)

display(df_day8)
```

The data intentionally contains multiple updates for:

```text id="5o6v5d"
CustomerId 1 → 3 records
CustomerId 2 → 2 records
```

---

## 2. Store Data in Bronze

The incoming data was stored as a Delta table.

```python id="l6uh2s"
df_day8.write.format("delta").mode("overwrite").saveAsTable(
    "bronze.day8_customer_updates"
)
```

---

## 3. Check Bronze Data

```sql id="l2l3kw"
SELECT *
FROM bronze.day8_customer_updates
ORDER BY CustomerId, UpdatedAt;
```

This displays the incoming records in Customer ID and update-time order.

---

## 4. Detect Duplicate Customer IDs

Duplicate Customer IDs were identified using `GROUP BY` and `HAVING`.

```sql id="q5f6v7"
SELECT
    CustomerId,
    COUNT(*) AS RecordCount
FROM bronze.day8_customer_updates
GROUP BY CustomerId
HAVING COUNT(*) > 1;
```

The result identifies customers that have more than one record.

---

## 5. Create Window Specification

A Window Specification was created by partitioning the data by `CustomerId` and ordering the records by `UpdatedAt` in descending order.

```python id="2j6j8p"
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

window_spec = (
    Window
    .partitionBy("CustomerId")
    .orderBy(col("UpdatedAt").desc())
)
```

### Explanation

`partitionBy("CustomerId")`

Groups records belonging to the same customer.

`orderBy(UpdatedAt.desc())`

Places the most recent customer update first.

---

## 6. Apply ROW_NUMBER()

`ROW_NUMBER()` was used to assign a ranking to each record within each Customer ID.

```python id="0n4m4k"
ranked_df = (
    df_day8
    .withColumn(
        "row_num",
        row_number().over(window_spec)
    )
)

display(ranked_df)
```

For each Customer ID:

```text id="1w0h7q"
row_num = 1 → Latest record
row_num = 2 → Previous record
row_num = 3 → Older record
```

---

## 7. Keep the Latest Record

Only records with `row_num = 1` were retained.

```python id="4z5j5v"
latest_df = (
    ranked_df
    .filter(col("row_num") == 1)
    .drop("row_num")
)

display(latest_df)
```

This produces one latest record for every Customer ID.

For example:

```text id="k5a9n4"
CustomerId 1 → Hyderabad, Age 31
CustomerId 2 → Chennai, Age 27
```

---

## 8. Save Latest Records to Silver

```python id="6r8y9t"
latest_df.write.format("delta").mode("overwrite").saveAsTable(
    "silver.day8_customers"
)
```

---

## 9. Verify Silver Data

```sql id="9u7h2m"
SELECT *
FROM silver.day8_customers
ORDER BY CustomerId;
```

The Silver table contains only the latest record for each Customer ID.

---

## 10. Verify Duplicate Removal

To confirm that the Silver table contains no duplicate Customer IDs:

```sql id="6q2w8k"
SELECT
    CustomerId,
    COUNT(*) AS RecordCount
FROM silver.day8_customers
GROUP BY CustomerId
HAVING COUNT(*) > 1;
```

If no rows are returned, the deduplication was successful.

---

# Advanced Deduplication

## 11. Create Batch Data

An additional batch was created to handle situations where two records have the same `UpdatedAt` value.

```python id="7f5j3s"
customers_batch = [
    (1, "Arjun", "Chennai", 29, "arjun@gmail.com", "2026-09-15 09:00:00", 1),
    (2, "Priya", "Bangalore", 26, "priya@gmail.com", "2026-09-15 09:05:00", 1),
    (1, "Arjun", "Bangalore", 30, "arjun@gmail.com", "2026-09-15 10:00:00", 2),
    (3, "Michael", "Hyderabad", 34, "michael@gmail.com", "2026-09-15 10:10:00", 2),
    (1, "Arjun", "Hyderabad", 31, "arjun@gmail.com", "2026-09-15 11:00:00", 3),
    (1, "Arjun", "Chennai", 32, "arjun@gmail.com", "2026-09-15 11:00:00", 4)
]

batch_columns = [
    "CustomerId",
    "CustomerName",
    "City",
    "Age",
    "Email",
    "UpdatedAt",
    "BatchId"
]

batch_df = spark.createDataFrame(
    customers_batch,
    batch_columns
)

display(batch_df)
```

Customer 1 has two records with the same `UpdatedAt` but different `BatchId` values.

---

## 12. Use UpdatedAt and BatchId for Ranking

The Window Specification was enhanced to use both `UpdatedAt` and `BatchId`.

```python id="6n5x4d"
window_batch_spec = (
    Window
    .partitionBy("CustomerId")
    .orderBy(
        col("UpdatedAt").desc(),
        col("BatchId").desc()
    )
)
```

This means:

1. The latest `UpdatedAt` is selected first.
2. If two records have the same `UpdatedAt`, the higher `BatchId` is selected first.

---

## 13. Rank the Batch Records

```python id="9v6k2p"
ranked_batch_df = (
    batch_df
    .withColumn(
        "row_num",
        row_number().over(window_batch_spec)
    )
)

display(ranked_batch_df)
```

---

## 14. Select the Latest Batch Record

```python id="2d7x8m"
filtered_batch_df = (
    ranked_batch_df
    .filter(col("row_num") == 1)
    .drop("row_num")
)

display(filtered_batch_df)
```

This provides the latest record for each Customer ID while also handling ties in `UpdatedAt`.

---

## 🔄 Final Day 8 Architecture

```text id="v4m2j7"
                 Incoming Customer Updates
                           ↓
             bronze.day8_customer_updates
                           ↓
                 Duplicate Detection
                           ↓
                   Window Function
                           ↓
              PARTITION BY CustomerId
                           ↓
          ORDER BY UpdatedAt DESC
                           ↓
                    ROW_NUMBER()
                           ↓
                  row_num = 1
                           ↓
                silver.day8_customers
                           ↓
                 Latest Customer Data
```

### Advanced Flow

```text id="1x7c5a"
Customer Updates
       ↓
CustomerId Partition
       ↓
UpdatedAt DESC
       ↓
BatchId DESC
       ↓
ROW_NUMBER()
       ↓
Keep row_num = 1
       ↓
Latest Record
```

---

## 📚 Key Learnings

* Advanced deduplication
* PySpark Window Functions
* `Window.partitionBy()`
* `Window.orderBy()`
* `ROW_NUMBER()`
* Identifying duplicate Customer IDs
* Selecting the latest record
* Using `UpdatedAt` for record ordering
* Using `BatchId` to resolve timestamp ties
* Creating a clean Silver table
* Handling multiple updates for the same customer

---

## 🏆 Day 8 Outcome

Successfully implemented **advanced customer deduplication using PySpark Window Functions**.

The pipeline can:

* Receive multiple customer updates.
* Identify duplicate Customer IDs.
* Rank records using `UpdatedAt`.
* Select the latest record for each customer.
* Handle records with the same timestamp using `BatchId`.
* Store the deduplicated data in the Silver layer.

### RetailMart Data Engineering Progress

```text id="5f2c8q"
Day 1 → Spark & DataFrames Basics
Day 2 → CSV Ingestion → Bronze
Day 3 → Data Cleaning → Silver
Day 4 → Joins & Business Transformations → Gold
Day 5 → Delta Lake Operations & Time Travel
Day 6 → MERGE / Upsert & Incremental Processing
Day 7 → Data Quality & Quarantine
Day 8 → Advanced Deduplication & Window Functions
