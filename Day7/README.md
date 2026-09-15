# Day 7 – Data Quality, NULL Handling, Duplicate Detection & Quarantine

## 📌 Objective

The objective of Day 7 is to implement **data quality checks** on incoming customer data.

In this task, the incoming customer data contains different quality issues such as:

* NULL values
* Duplicate Customer IDs
* Empty customer names
* Missing email or city
* Invalid age values

The data is validated using predefined rules.

Valid records are stored in the **Silver layer**, while invalid records are moved to a **quarantine table** for further investigation.

---

## 🏗️ Project Flow

```text
Incoming Customer Data
          ↓
Bronze Layer
bronze.day7_customer_quality
          ↓
     Data Quality Checks
          ↓
    ┌─────┴─────┐
    ↓           ↓
 Valid Data   Invalid Data
    ↓           ↓
 Silver      Quarantine
```

---

## 📂 Tables Used

### Bronze Table

```text
bronze.day7_customer_quality
```

Contains the incoming customer data before quality validation.

### Silver Table

```text
silver.day7_customers
```

Contains records that passed the defined data-quality rules.

### Quarantine Table

```text
silver.day7_customer_quarantine
```

Contains records that failed one or more data-quality checks.

---

## 1. Create Incoming Customer Data

A sample customer dataset was created containing both valid and invalid records.

The dataset was intentionally created with issues such as:

* Duplicate Customer ID
* NULL Customer ID
* NULL Customer Name
* NULL Email
* NULL City
* Empty Customer Name
* Negative Age
* Age greater than 120

```python
customers_day7 = [
    (1, "Arjun", "arjun@gmail.com", "Chennai", 29),
    (2, "Priya", "priya@gmail.com", "Bangalore", 26),
    (3, "Michael", "michael@gmail.com", "Hyderabad", 34),
    (4, "Neha", "neha@gmail.com", "Coimbatore", 31),
    (5, "Vikram", "vikram@gmail.com", "Madurai", 38),
    (6, "Ananya", "ananya@gmail.com", "Salem", 24),
    (7, "Rahul", "rahul@gmail.com", "Trichy", 32),
    (7, "Rahul", "rahul@gmail.com", "Trichy", 32),
    (8, None, "sneha@gmail.com", "Erode", 27),
    (9, "Karthik", None, "Tirunelveli", 36),
    (10, "Divya", "divya@gmail.com", None, 28),
    (11, "Nithya", "nithya@gmail.com", "Chennai", -5),
    (12, " ", "blank@gmail.com", "Chennai", 30),
    (13, "Surya", "surya@gmail.com", "Chennai", 150),
    (None, "Meena", "meena@gmail.com", "Chennai", 29)
]

columns = [
    "CustomerId",
    "CustomerName",
    "Email",
    "City",
    "Age"
]

df = spark.createDataFrame(customers_day7, columns)

display(df)
```

---

## 2. Store Data in Bronze

The incoming data was stored as a Delta table in the Bronze layer.

```python
df.write.format("delta").mode("overwrite").saveAsTable(
    "bronze.day7_customer_quality"
)
```

The Bronze table was verified using:

```sql
SELECT *
FROM bronze.day7_customer_quality;
```

---

## 3. NULL Value Check

A SQL query was used to identify NULL values in important columns.

```sql
SELECT
    COUNT(*) AS TotalRecords,
    SUM(CASE WHEN CustomerId IS NULL THEN 1 ELSE 0 END) AS NullCustomerId,
    SUM(CASE WHEN CustomerName IS NULL THEN 1 ELSE 0 END) AS NullCustomerName,
    SUM(CASE WHEN Email IS NULL THEN 1 ELSE 0 END) AS NullEmail,
    SUM(CASE WHEN City IS NULL THEN 1 ELSE 0 END) AS NullCity,
    SUM(CASE WHEN Age IS NULL THEN 1 ELSE 0 END) AS NullAge
FROM bronze.day7_customer_quality;
```

This provides an overview of NULL values present in the incoming dataset.

---

## 4. Duplicate Detection

`CustomerId` should be unique.

Duplicate Customer IDs were identified using:

```sql
SELECT
    CustomerId,
    COUNT(*) AS RecordCount
FROM bronze.day7_customer_quality
WHERE CustomerId IS NOT NULL
GROUP BY CustomerId
HAVING COUNT(*) > 1;
```

This identified duplicate customer records.

---

## 5. Invalid Age Detection

The valid age range was defined as **0 to 120**.

Records outside this range were identified using:

```sql
SELECT *
FROM bronze.day7_customer_quality
WHERE Age < 0
   OR Age > 120;
```

This detected records with invalid age values.

---

## 6. Empty Customer Name Detection

NULL and empty values were checked separately.

```sql
SELECT *
FROM bronze.day7_customer_quality
WHERE CustomerName IS NULL
   OR TRIM(CustomerName) = '';
```

This identifies both NULL customer names and names containing only whitespace.

---

## 7. Data Quality Rules

The following rules were applied to the customer data:

```text
Rule 1: CustomerId cannot be NULL

Rule 2: CustomerId must be unique

Rule 3: CustomerName cannot be NULL or empty

Rule 4: Email cannot be NULL

Rule 5: City cannot be NULL or empty

Rule 6: Age must be between 0 and 120
```

---

## 8. Create Clean Silver Data

PySpark filters were used to retain only records that satisfy the data-quality rules.

```python
from pyspark.sql.functions import col, trim

clean_df = (
    df
    .filter(col("CustomerId").isNotNull())
    .filter(col("CustomerName").isNotNull())
    .filter(trim(col("CustomerName")) != "")
    .filter(col("Email").isNotNull())
    .filter(col("City").isNotNull())
    .filter(trim(col("City")) != "")
    .filter((col("Age") >= 0) & (col("Age") <= 120))
    .dropDuplicates(["CustomerId"])
)

display(clean_df)
```

---

## 9. Save Clean Data to Silver

The validated customer records were stored in:

```text
silver.day7_customers
```

```python
clean_df.write.format("delta").mode("overwrite").saveAsTable(
    "silver.day7_customers"
)
```

The final Silver data was verified using:

```sql
SELECT *
FROM silver.day7_customers
ORDER BY CustomerId;
```

---

## 10. Create Invalid Records

Records that failed the quality checks were identified separately.

```python
invalid_df = (
    df
    .filter(
        col("CustomerId").isNull()
        | col("CustomerName").isNull()
        | (trim(col("CustomerName")) == "")
        | col("Email").isNull()
        | col("City").isNull()
        | (trim(col("City")) == "")
        | (col("Age") < 0)
        | (col("Age") > 120)
    )
)

display(invalid_df)
```

---

## 11. Store Invalid Records in Quarantine

Invalid records were stored separately instead of being permanently deleted.

```python
invalid_df.write.format("delta").mode("overwrite").saveAsTable(
    "silver.day7_customer_quarantine"
)
```

The quarantine table was verified using:

```sql
SELECT *
FROM silver.day7_customer_quarantine;
```

---

## 12. Detect Duplicate Customer Records

Duplicate Customer IDs were identified using PySpark.

```python
duplicate_customer_ids = (
    df
    .filter(col("CustomerId").isNotNull())
    .groupBy("CustomerId")
    .count()
    .filter(col("count") > 1)
)

display(duplicate_customer_ids)
```

The actual duplicate records were then retrieved:

```python
duplicate_df = (
    df
    .join(duplicate_customer_ids, on="CustomerId", how="inner")
    .drop("count")
)

display(duplicate_df)
```

---

## 13. Add Duplicate Records to Quarantine

The duplicate records were appended to the quarantine table.

```python
duplicate_df.write.format("delta").mode("append").saveAsTable(
    "silver.day7_customer_quarantine"
)
```

The final quarantine table was checked using:

```sql
SELECT *
FROM silver.day7_customer_quarantine
ORDER BY CustomerId;
```

---

## 🔄 Final Data Quality Architecture

```text
                 Incoming Customer Data
                          ↓
             bronze.day7_customer_quality
                          ↓
                 Data Quality Checks
                          ↓
              ┌──────────┴──────────┐
              ↓                     ↓
        Valid Records          Invalid Records
              ↓                     ↓
    silver.day7_customers   silver.day7_customer_quarantine
              ↓
         Clean Customer Data
```

---

## 📚 Key Learnings

* Data quality validation
* NULL value detection
* Empty value detection
* Duplicate detection
* Data validation rules
* PySpark filtering
* SQL-based quality checks
* Removing duplicate records
* Creating a clean Silver table
* Creating a quarantine table
* Handling invalid records separately
* Bronze → Silver data-quality processing

---

## 🏆 Day 7 Outcome

Successfully implemented a **customer data-quality pipeline** for the RetailMart project.

The pipeline identifies invalid and duplicate customer records, keeps valid records in the Silver layer, and separates problematic records into a quarantine table for further investigation.

### RetailMart Data Engineering Progress

```text
Day 1 → Spark & DataFrames
Day 2 → CSV Ingestion → Bronze
Day 3 → Data Cleaning → Silver
Day 4 → Joins & Business Transformations → Gold
Day 5 → Delta Lake Operations & Time Travel
Day 6 → MERGE / Upsert & Incremental Processing
Day 7 → Data Quality & Quarantine
```

**Next: Day 8 – Continue the RetailMart Data Engineering Pipeline**
