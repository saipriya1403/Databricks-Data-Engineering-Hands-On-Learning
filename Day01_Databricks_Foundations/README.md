# Day 01 - Databricks Foundations

## Project
**RetailMart Data Engineering Platform**

## Objective
Learn the fundamentals of Databricks, Apache Spark, PySpark DataFrames, Spark SQL, Delta Lake, and the Databricks Lakehouse architecture.

## Concepts Learned

- Databricks Workspace
- Apache Spark
- Databricks Lakehouse
- Databricks Notebooks
- Compute
- Catalog and Schema
- Managed Tables
- Delta Tables
- PySpark DataFrames
- Spark SQL

## Tasks Completed

- Created a Databricks notebook
- Executed Python code
- Executed Spark SQL
- Created a PySpark DataFrame
- Explored the DataFrame using:
  - `show()`
  - `printSchema()`
  - `describe()`
  - `count()`
- Created a managed Delta table
- Queried the table using Spark SQL

## Dataset

The customer dataset contains:

| CustomerId | Name | City | Age |
|---|---|---|---|
| 1 | Meera Nair | Chennai | 29 |
| 2 | Rahul Verma | Delhi | 34 |
| 3 | Priya Menon | Bangalore | 41 |
| 4 | Arjun Iyer | Coimbatore | 25 |

## Architecture

```text
Python Data
     ↓
Spark DataFrame
     ↓
Managed Delta Table
     ↓
Databricks Catalog

## Key Learning.
##Databricks - Databricks is a cloud data and AI platform that provides capabilities for data engineering, analytics, and machine learning.

### Apache Spark -  Apache Spark is a distributed processing engine used to process large datasets across multiple machines.

### DataFrame  - A Spark DataFrame is a distributed collection of data organized into named columns.

### Lakehouse - A Lakehouse combines capabilities of data lakes and data warehouses, providing scalable storage and reliable data management.

### Delta Table - A Delta table uses Delta Lake to provide features such as ACID transactions, schema enforcement, and time travel.

### Managed Table - A managed table is registered in the catalog and its underlying storage is managed by Databricks.

## Outcome
 Successfully created a Spark DataFrame and managed Delta table using Databricks and queried the data using Spark SQL.               
   
