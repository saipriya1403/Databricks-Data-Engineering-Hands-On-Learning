# Day 05 - Delta Lake Fundamentals & ACID Transactions

## 📌 Project: RetailMart Data Engineering Platform

### 🎯 Objective

The objective of Day 5 is to understand **Delta Lake fundamentals** and practice how Delta tables support reliable data operations such as:

* Append
* Update
* Delete
* Transaction History
* Time Travel
* Restore
* Schema Enforcement
* ACID Transactions

This day continues from **Day 4**, where customer data was stored in the Silver layer.

---

## 🏗️ Architecture

```text
Day 4

silver.customers
silver.products
silver.orders
       ↓
   Joins & Transformations
       ↓
gold.order_details
gold.customer_revenue


Day 5

silver.customers
       ↓
silver.day5_customers
       ↓
    Delta Table
       ↓
Append → Update → Delete
       ↓
History → Time Travel → Restore
       ↓
Schema Enforcement
```

---

## 🛠️ Technologies Used

* Databricks
* PySpark
* Spark SQL
* Delta Lake
* Delta Tables

---

## 📂 Starting Point

Day 5 uses the customer data created during **Day 4**.

Instead of modifying the original `silver.customers` table, a separate table was created for practicing Delta Lake operations:

```text
silver.day5_customers
```

This keeps the original Silver table safe while allowing different Delta Lake operations to be tested.

---

# 🔹 Step 1 - Check Existing Silver Table

The existing customer table was inspected using:

```sql
DESCRIBE DETAIL silver.customers;
```

This was used to check the table metadata and format before starting the Day 5 Delta Lake practice.

---

# 🔹 Step 2 - Create Delta Table

The Silver customer table was read and written as a Delta table:

```python
customers_df = spark.table("silver.customers")

customers_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver.day5_customers")
```

The resulting table is:

```text
silver.day5_customers
```

The table initially contained the customer records from the Silver layer.

---

# 🔹 Step 3 - Verify Delta Table

The table was verified using:

```sql
SELECT * 
FROM silver.day5_customers
ORDER BY CustomerId;
```

Metadata was also checked using:

```sql
DESCRIBE DETAIL silver.day5_customers;
```

---

# 🔹 Step 4 - Delta Table History

Delta Lake maintains a transaction history for table operations.

The history was checked using:

```sql
DESCRIBE HISTORY silver.day5_customers;
```

This provides information about operations performed on the table, including:

* Version
* Operation
* Operation metrics
* Isolation level
* Engine information

---

# 🔹 Step 5 - Append Data

A new customer was created and appended to the Delta table.

```python
new_customer = [(11, "Nithya", "Chennai", 30)]

columns = ["CustomerId", "CustomerName", "City", "Age"]

new_df = spark.createDataFrame(new_customer, columns)

display(new_df)

new_df.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("silver.day5_customers")
```

This demonstrated how new records can be added to an existing Delta table.

---

# 🔹 Step 6 - Update Data

An existing customer record was updated using Spark SQL.

```sql
UPDATE silver.day5_customers
SET City = 'Coimbatore'
WHERE CustomerId = 1;
```

The table history was then checked to observe the update transaction.

---

# 🔹 Step 7 - Delete Data

The newly added customer was deleted from the Delta table:

```sql
DELETE FROM silver.day5_customers
WHERE CustomerId = 11;
```

The operation was verified using:

```sql
DESCRIBE HISTORY silver.day5_customers;
```

---

# 🔹 Step 8 - Time Travel

Delta Lake allows access to previous versions of a table.

First, the table history was checked:

```sql
DESCRIBE HISTORY silver.day5_customers;
```

The required previous version was identified from the history.

That version was then queried using:

```sql
SELECT *
FROM silver.day5_customers
VERSION AS OF <actual_version>
ORDER BY CustomerId;
```

This allowed the previous state of the customer table to be viewed without changing the current table.

---

# 🔹 Step 9 - Restore Previous Version

After practicing Time Travel, the table was restored to the required previous version.

```sql
RESTORE TABLE silver.day5_customers
TO VERSION AS OF <actual_version>;
```

The restore operation was confirmed through:

```sql
DESCRIBE HISTORY silver.day5_customers;
```

The transaction history showed a new **RESTORE** operation.

---

# 🔹 Step 10 - Schema Enforcement

Delta Lake provides schema enforcement to prevent incompatible data from being written into a table.

A test DataFrame with an incompatible `CustomerId` data type was created:

```python
bad_data = [
    ("ABC", "Test Customer", "Chennai", 30)
]

bad_df = spark.createDataFrame(bad_data, columns)

display(bad_df)
bad_df.printSchema()
```

The DataFrame was then attempted to be appended:

```python
bad_df.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("silver.day5_customers")
```

The operation was rejected because the `CustomerId` type did not match the existing Delta table schema.

This demonstrated **schema enforcement**.

---

# 🔹 Step 11 - Final Verification

The final Delta table and transaction history were verified using:

```sql
DESCRIBE DETAIL silver.day5_customers;
```

```sql
DESCRIBE HISTORY silver.day5_customers;
```

```sql
SELECT *
FROM silver.day5_customers
ORDER BY CustomerId;
```

The transaction history recorded the Delta operations performed during the exercise, including the restore operation.

---

# 🧠 Delta Lake Mental Model

A Delta table can be understood as:

```text
Delta Table
│
├── Parquet Data Files
│
└── _delta_log
       │
       ├── Table Versions
       ├── Transactions
       └── Metadata
```

The transaction log allows Delta Lake to maintain reliable table versions and support features such as:

* ACID transactions
* Time Travel
* Update
* Delete
* Restore
* Schema enforcement
* Transaction history

---

# 🔐 ACID Transactions

Delta Lake provides ACID transaction support.

### Atomicity

A transaction is completed successfully or not applied.

### Consistency

Data remains consistent with the table schema and rules.

### Isolation

Concurrent operations are managed using transaction isolation.

### Durability

Committed changes are recorded and retained as table versions.

---

# 📊 Medallion Architecture Progress

The RetailMart project now follows this progression:

```text
Day 1
DataFrame & Spark Fundamentals
        ↓
Day 2
CSV → Bronze
        ↓
Day 3
Bronze → Silver
        ↓
Day 4
Silver → Joins & Transformations → Gold
        ↓
Day 5
Silver → Delta Lake
        ↓
Append → Update → Delete
        ↓
History → Time Travel → Restore
        ↓
Schema Enforcement
```

---

# 🎯 Key Learnings

By completing Day 5, I practiced:

* Creating Delta tables
* Writing data using Delta format
* Appending new records
* Updating existing records
* Deleting records
* Viewing Delta transaction history
* Understanding table versions
* Using Time Travel
* Restoring previous table versions
* Understanding schema enforcement
* Understanding ACID transactions
* Understanding the role of the Delta transaction log

---

# ✅ Outcome

Successfully completed **Day 5 - Delta Lake Fundamentals & ACID Transactions** using the RetailMart customer data.

The exercise demonstrated how Delta Lake provides a reliable layer for managing changing data while maintaining transaction history, historical versions, recovery capabilities, and schema consistency.

---

## 🚀 Next Step

**Day 6 - Delta Lake MERGE & Upsert**

The next stage will build on the Delta table created in Day 5 and introduce **MERGE/Upsert operations** for handling new and existing records efficiently.
