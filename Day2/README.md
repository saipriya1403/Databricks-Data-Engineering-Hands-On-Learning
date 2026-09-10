# Day 2 – CSV Data Ingestion Using Databricks

## 📌 Objective

Ingest customer data from a CSV file stored in a Databricks Volume, perform basic data exploration and validation using PySpark, and store the ingested data in the Bronze layer.

## 🛠️ Technologies Used

* Databricks
* PySpark
* Python
* SQL
* Delta Table

## 📂 Dataset

The input dataset is a customer CSV file containing the following columns:

* CustomerId
* CustomerName
* Email
* City
* Age

## 🔄 Data Ingestion Flow

```text
CSV File
   ↓
Databricks Volume
   ↓
PySpark DataFrame
   ↓
Data Exploration & Validation
   ↓
Bronze Layer
   ↓
bronze.customers
```

## 💻 Implementation

### 1. Read CSV File

The CSV file was read from the Databricks Volume using PySpark.

```python
df = spark.read.csv(
    "/Volumes/dataengineering/default/customer",
    header=True,
    inferSchema=True
)

display(df)
```

### 2. Select Required Columns

```python
df.select("CustomerName", "City").show()
```

### 3. Filter Customers from Chennai

```python
df.filter(df.City == "Chennai").show()
```

### 4. Filter Customers Above Age 30

```python
df.filter(df.Age > 30).display()
```

### 5. Find Distinct Customer Names

```python
df.select("CustomerName").distinct().display()
```

## 🔍 Data Quality Checks

The following checks were performed:

### Row Count

```python
df.count()
```

### Schema Validation

```python
df.printSchema()
```

### Null Value Check

```python
from pyspark.sql.functions import col, sum

df.select([
    sum(col(c).isNull().cast("int")).alias(c)
    for c in df.columns
]).display()
```

## 🥉 Bronze Layer

A Bronze schema was created to store the raw ingested customer data.

```sql
CREATE SCHEMA IF NOT EXISTS bronze;
```

The DataFrame was then written to a Bronze table:

```python
df.write.mode("overwrite").saveAsTable("bronze.customers")
```

The table was verified using:

```sql
SHOW TABLES IN bronze;
```

And the data was queried using:

```sql
SELECT * FROM bronze.customers;
```

## 📚 What I Learned

* How to read CSV files from a Databricks Volume using PySpark.
* How to create and work with PySpark DataFrames.
* How to perform basic filtering and column selection.
* How to check schema, row count, and null values.
* How to create a Bronze schema.
* How to store ingested data as a table in the Bronze layer.
* Basic understanding of the **Medallion Architecture**.

## ✅ Day 2 Outcome

Successfully ingested customer CSV data into Databricks and stored it in the **Bronze layer** as `bronze.customers`.
