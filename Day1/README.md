# Day 1 – Databricks Setup and DataFrame Basics

## 📌 Objective

The objective of Day 1 was to get familiar with the Databricks environment and perform basic data engineering operations using PySpark.

## 🛠️ Technologies Used

* Databricks
* PySpark
* Python
* SQL

## 🚀 Tasks Performed

### 1. Databricks Environment Setup

Created a Databricks notebook and verified the environment by displaying a welcome message and the current timestamp.

```python
print("Welcome to RetailMart Data Engineering")
```

```sql
SELECT current_timestamp();
```

### 2. Created Customer Data

Created a sample customer dataset using Python and converted it into a PySpark DataFrame.

```python
customers = [
    (1, "Meera Pandey", "Chennai", 29),
    (2, "Leela kumar", "Delhi", 34),
    (3, "Priya Jerson", "Bangalore", 41),
    (4, "Arjun Varma", "Coimbatore", 25)
]

columns = ["CustomerId", "Name", "City", "Age"]

df = spark.createDataFrame(customers, columns)

display(df)
```

### 3. DataFrame Exploration

Performed basic DataFrame operations:

* Displayed the data
* Viewed the records using `show()`
* Checked the DataFrame schema
* Generated descriptive statistics
* Checked the number of records

```python
df.show()
df.printSchema()
df.describe().show()
df.count()
```

### 4. Stored Data as a Table

Saved the DataFrame as a Databricks table using overwrite mode.

```python
df.write.mode("overwrite").saveAsTable("customers")
```

### 5. Queried the Table Using SQL

Verified the stored data using SQL.

```sql
SELECT *
FROM customers;
```

## 📊 Data Columns

| Column     | Description                |
| ---------- | -------------------------- |
| CustomerId | Unique customer identifier |
| Name       | Customer name              |
| City       | Customer city              |
| Age        | Customer age               |

## 📚 What I Learned

* Basics of the Databricks notebook environment
* Creating PySpark DataFrames
* Defining DataFrame columns
* Exploring DataFrame data
* Checking schema and descriptive statistics
* Counting DataFrame records
* Writing a DataFrame as a table
* Querying Databricks tables using SQL

## ✅ Day 1 Outcome

Successfully created a customer DataFrame in Databricks, performed basic data exploration, stored the data as a table, and queried the table using SQL.
