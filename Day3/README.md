# Day 03 - Silver Layer Data Cleaning

## Project

**RetailMart Data Engineering Platform**

## Objective

The objective of Day 3 is to build the **Silver layer** of the RetailMart data pipeline by reading customer data from the Bronze table, applying data cleaning and validation, removing duplicate and incomplete records, and storing the cleaned data as a Silver table.

This day continues directly from **Day 2**, where the customer CSV file was ingested into the `bronze.customers` table.

---

## Data Pipeline

```text
Day 2
CSV File
   ↓
Databricks Volume
   ↓
PySpark DataFrame
   ↓
bronze.customers
   │
   │
   │ Day 3
   ↓
Data Cleaning & Validation
   ↓
Remove Duplicates
   ↓
Handle Null Values
   ↓
silver.customers
   ↓
SQL Analysis
```

---

## Technologies Used

* Databricks
* PySpark
* Python
* Spark SQL
* Unity Catalog Volume
* DataFrame API

---

## 1. Read Data from Bronze Layer

The Day 3 process starts by reading the table created during Day 2.

```python
df = spark.table("bronze.customers")

display(df)
```

The Bronze table contains the customer data ingested from the CSV file.

---

## 2. Inspect the Bronze Data

The schema and record count were checked before performing any transformations.

```python
df.printSchema()
```

```python
df.count()
```

The customer data contains the following columns:

| Column       | Data Type |
| ------------ | --------- |
| CustomerId   | Integer   |
| CustomerName | String    |
| Email        | String    |
| City         | String    |
| Age          | Integer   |

---

## 3. Data Cleaning

The customer data was standardized before loading it into the Silver layer.

### Standardize Customer Name

```python
from pyspark.sql.functions import col, trim, initcap

df = df.withColumn(
    "CustomerName",
    trim(initcap(col("CustomerName")))
)
```

`trim()` removes unnecessary spaces and `initcap()` standardizes the capitalization of names.

### Standardize City

```python
df = df.withColumn(
    "City",
    trim(initcap(col("City")))
)
```

This ensures that values such as `chennai` are standardized to `Chennai`.

### Clean Email

```python
df = df.withColumn(
    "Email",
    trim(col("Email"))
)
```

---

## 4. Validate Age Data Type

The `Age` column was explicitly cast to an integer.

```python
df = df.withColumn(
    "Age",
    col("Age").cast("int")
)
```

The schema was then verified:

```python
df.printSchema()
```

---

## 5. Remove Duplicate Records

Duplicate customer IDs were checked and removed.

```python
df = df.dropDuplicates(["CustomerId"])
```

A duplicate check was then performed:

```python
df.groupBy("CustomerId") \
    .count() \
    .filter(col("count") > 1) \
    .show()
```

The result showed no remaining duplicate `CustomerId` values.

---

## 6. Handle Null Values

Records containing null values in required columns were removed.

```python
df = df.filter(
    col("CustomerId").isNotNull() &
    col("CustomerName").isNotNull() &
    col("Email").isNotNull() &
    col("City").isNotNull() &
    col("Age").isNotNull()
)
```

After filtering, the final DataFrame contained **8 valid records**.

```python
df.count()
```

Output:

```text
8
```

---

## 7. Create Silver Schema

A separate schema was created for the cleaned Silver layer.

```sql
CREATE SCHEMA IF NOT EXISTS silver;
```

---

## 8. Write Data to Silver Layer

The cleaned DataFrame was stored as the `silver.customers` table.

```python
df.write \
    .mode("overwrite") \
    .saveAsTable("silver.customers")
```

The Silver layer now contains cleaned and validated customer data.

---

## 9. Verify Silver Data

The Silver table was queried using Spark SQL.

```sql
SELECT *
FROM silver.customers;
```

---

## 10. SQL Analysis

### Customers Older Than 30

```sql
SELECT *
FROM silver.customers
WHERE Age > 30;
```

This query identifies customers whose age is greater than 30.

### Customer Count by City

```sql
SELECT
    City,
    COUNT(CustomerId) AS CustomerCount
FROM silver.customers
GROUP BY City;
```

This provides the number of customers in each city.

---

## 11. Select Required Columns

A DataFrame containing only the required customer columns was created.

```python
df_filter = df.select(
    "CustomerId",
    "CustomerName",
    "City"
)

df_filter.show()
```

This produced a simplified view containing:

```text
CustomerId
CustomerName
City
```

---

## Bronze vs Silver

| Bronze Layer              | Silver Layer                          |
| ------------------------- | ------------------------------------- |
| Raw ingested data         | Cleaned data                          |
| Source from CSV ingestion | Source from Bronze table              |
| May contain duplicates    | Duplicate IDs removed                 |
| May contain null values   | Required null records removed         |
| Original formatting       | Standardized formatting               |
| Initial data storage      | Validated data for further processing |

---

## Key Learnings

* How to read an existing Spark table using `spark.table()`
* How to clean data using PySpark DataFrame functions
* How `trim()` and `initcap()` can standardize string data
* How to cast columns to the required data type
* How to identify and remove duplicate records
* How to filter records containing null values
* How to create a separate Silver schema
* How to save a cleaned DataFrame as a table
* How to perform SQL analysis on the Silver layer
* Understanding the purpose of Bronze and Silver layers in a data pipeline

---

## Outcome

Successfully transformed the **Day 2 Bronze customer data** into a cleaned **Silver layer**.

The Day 3 pipeline performs:

```text
bronze.customers
      ↓
Data Cleaning
      ↓
Data Standardization
      ↓
Duplicate Removal
      ↓
Null Validation
      ↓
silver.customers
```

This establishes the **Bronze → Silver** stage of the RetailMart Data Engineering pipeline and prepares the data for future transformations and business-level processing.
