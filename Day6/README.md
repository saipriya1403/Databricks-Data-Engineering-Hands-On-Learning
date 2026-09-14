# Day 6 – Delta Lake MERGE / Upsert & Incremental Processing

## 📌 Objective

The objective of Day 6 is to understand **incremental data processing** using Delta Lake `MERGE`.

In this task, incoming customer records are compared with the existing Silver customer table.

* If the `CustomerId` already exists → **UPDATE** the existing customer.
* If the `CustomerId` does not exist → **INSERT** the new customer.
* Incoming data is first stored in the **Bronze layer**.
* Delta Lake `MERGE` is then used to perform the upsert operation.

---

## 🏗️ Project Flow

```text
Incoming Customer Updates
          ↓
Bronze Layer
bronze.day6_customer_updates
          ↓
Delta MERGE
          ↓
Silver Layer
silver.customers
          ↓
Updated / Inserted Customer Data
```

---

## 📂 Tables Used

### Existing Silver Table

```text
silver.customers
```

This table contains the cleaned customer data created during the previous days of the project.

### Incoming Bronze Table

```text
bronze.day6_customer_updates
```

This table stores the new and changed customer records received as an incremental batch.

---

## 1. Check Existing Customer Data

The existing Silver customer table was checked before processing the incoming records.

```sql
SELECT *
FROM silver.customers
ORDER BY CustomerId;
```

---

## 2. Create Incoming Customer Data

The incoming customer batch contains both existing and new customers.

```python
incoming_customers = [
    (2, "John", "john@gmail.com", "Bangalore", 29),
    (4, "David", "david@gmail.com", "Chennai", 40),
    (9, "Vinoth", "vinoth@gmail.com", "Chennai", 27)
]

columns = [
    "CustomerId",
    "CustomerName",
    "Email",
    "City",
    "Age"
]

incoming_df = spark.createDataFrame(
    incoming_customers,
    columns
)

display(incoming_df)
```

### Incoming Records

| CustomerId | CustomerName | Email                                       | City      | Age |
| ---------: | ------------ | ------------------------------------------- | --------- | --: |
|          2 | John         | [john@gmail.com](mailto:john@gmail.com)     | Bangalore |  29 |
|          4 | David        | [david@gmail.com](mailto:david@gmail.com)   | Chennai   |  40 |
|          9 | Vinoth       | [vinoth@gmail.com](mailto:vinoth@gmail.com) | Chennai   |  27 |

---

## 3. Store Incoming Data in Bronze

The incoming data was stored as a Delta table in the Bronze layer.

```python
incoming_df.write.format("delta").mode("overwrite").saveAsTable(
    "bronze.day6_customer_updates"
)
```

The Bronze table was then verified:

```sql
SELECT *
FROM bronze.day6_customer_updates;
```

---

## 4. Perform Delta MERGE

The incoming Bronze data was merged with the existing Silver customer table using `CustomerId` as the matching key.

```sql
MERGE INTO silver.customers AS target
USING bronze.day6_customer_updates AS source

ON target.CustomerId = source.CustomerId

WHEN MATCHED THEN
  UPDATE SET
    target.CustomerName = source.CustomerName,
    target.Email = source.Email,
    target.City = source.City,
    target.Age = source.Age

WHEN NOT MATCHED THEN
  INSERT (
    CustomerId,
    CustomerName,
    Email,
    City,
    Age
  )
  VALUES (
    source.CustomerId,
    source.CustomerName,
    source.Email,
    source.City,
    source.Age
  );
```

---

## 5. MERGE Logic

The `MERGE` operation performs two main actions.

### WHEN MATCHED

If the `CustomerId` already exists in `silver.customers`, the existing record is updated.

In this batch:

```text
CustomerId 2 → UPDATE
CustomerId 4 → UPDATE
```

### WHEN NOT MATCHED

If the `CustomerId` does not exist in the Silver table, a new record is inserted.

In this batch:

```text
CustomerId 9 → INSERT
```

Therefore:

```text
Existing Customer
       ↓
CustomerId matches?
   ↓             ↓
 YES            NO
  ↓              ↓
UPDATE          INSERT
```

---

## 6. Verify the MERGE Result

After the MERGE operation, the Silver table was checked again.

```sql
SELECT *
FROM silver.customers
ORDER BY CustomerId;
```

This verifies that:

* Existing customers were updated.
* The new customer was inserted.
* The Silver table now contains the latest customer information.

---

## 7. Understand WHEN MATCHED

`WHEN MATCHED` is used when the incoming record has the same key as an existing record.

For example:

```text
CustomerId = 2
CustomerId = 4
```

already existed in the target table.

Therefore, their customer information was updated using the incoming Bronze data.

---

## 8. Understand WHEN NOT MATCHED

`WHEN NOT MATCHED` is used when an incoming record does not have a matching key in the target table.

For example:

```text
CustomerId = 9
```

was a new customer, so the record was inserted into `silver.customers`.

---

## 9. Full Load vs Incremental Load

### Full Load

A Full Load processes the complete dataset every time.

```text
Complete Source Data
        ↓
Target Table
```

### Incremental Load

An Incremental Load processes only new or changed records.

```text
New / Changed Data
        ↓
MERGE
        ↓
Target Table
```

The Day 6 implementation demonstrates **incremental processing** using Delta Lake `MERGE`.

---

## 10. Batch Date

A batch date can be used to identify when an incremental data batch was received or processed.

Example incoming batch:

```python
incoming_customers_2 = [
    (6, "Kumar", "Chennai", 35, "2026-08-15"),
    (2, "John", "Chennai", 30, "2026-08-15")
]

columns_2 = [
    "CustomerId",
    "CustomerName",
    "City",
    "Age",
    "BatchDate"
]

incoming_df_2 = spark.createDataFrame(
    incoming_customers_2,
    columns_2
)

display(incoming_df_2)
```

The `BatchDate` helps identify the date associated with a particular incoming data batch.

---

## 11. General MERGE Pattern

The general Delta Lake MERGE pattern is:

```text
Target Table
     +
Source Table
     ↓
Matching Condition
     ↓
 ┌───────────────┐
 │               │
MATCHED     NOT MATCHED
 │               │
UPDATE          INSERT
```

This pattern is commonly used for incremental data processing and upsert operations.

---

## 12. Product MERGE

A Product MERGE exercise is optional additional practice.

The main Day 6 implementation focuses on **customer incremental processing**, so no separate Product MERGE was required for this project.

---

## 13. Final Verification

The final Silver customer table can be checked using:

```sql
SELECT *
FROM silver.customers
ORDER BY CustomerId;
```

Delta Lake transaction history can also be checked using:

```sql
DESCRIBE HISTORY silver.customers;
```

This helps track operations performed on the Delta table, including MERGE operations.

---

## 🔄 Final Day 6 Architecture

```text
                 Incoming Customer Data
                          │
                          ▼
              bronze.day6_customer_updates
                          │
                          ▼
                    Delta MERGE
                          │
              ┌───────────┴───────────┐
              │                       │
        WHEN MATCHED            WHEN NOT MATCHED
              │                       │
           UPDATE                  INSERT
              │                       │
              └───────────┬───────────┘
                          ▼
                  silver.customers
                          │
                          ▼
                     Gold Layer
```

---

## 📚 Key Learnings

* Understanding incremental data processing
* Full Load vs Incremental Load
* Understanding Upsert operations
* Using Delta Lake `MERGE`
* Understanding `WHEN MATCHED`
* Understanding `WHEN NOT MATCHED`
* Updating existing records
* Inserting new records
* Storing incoming data in the Bronze layer
* Using Batch Date for incremental batches
* Checking Delta Lake transaction history
* Understanding the Bronze → Silver → Gold architecture

---

## 🏆 Day 6 Outcome

Successfully implemented an **incremental customer data pipeline** using Delta Lake MERGE.

The pipeline can:

* Receive incoming customer updates.
* Store them in the Bronze layer.
* Identify existing customers using `CustomerId`.
* Update existing customer records.
* Insert new customer records.
* Maintain the processed data in the Silver layer.
* Track Delta Lake operations through table history.

### RetailMart Data Engineering Progress

```text
Day 1 → DataFrame & Basic Spark
Day 2 → CSV Ingestion → Bronze
Day 3 → Data Cleaning → Silver
Day 4 → Joins & Business Transformations → Gold
Day 5 → Delta Lake Operations & Time Travel
Day 6 → MERGE / Upsert & Incremental Processing
```

**Next: Day 7 – Data Quality, Constraints, NULL Handling & Duplicate Detection**
