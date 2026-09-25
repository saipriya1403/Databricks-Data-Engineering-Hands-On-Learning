# Day 17 – Streaming + SCD Type 2

## Objective

Implement a simple **SCD Type 2** process using customer data and Auto Loader streaming.

The goal is to maintain customer history when existing customer information changes.

---

## Tools Used

* Databricks
* PySpark
* Delta Lake
* Auto Loader
* SQL
* Python

---

## Project Flow

```text
Initial CSV
     ↓
Bronze Customer Table
     ↓
SCD Type 2 Silver Table
     ↓
Update CSV
     ↓
Auto Loader Streaming
     ↓
Bronze Update Table
     ↓
Update SCD Type 2 Silver Table
```

---

## Step 1 – Initial Customer Data

Created `customers_initial.csv`.

Sample data:

```text
CustomerId,CustomerName,City,Age,UpdatedAt
501,Arun,Chennai,30,2026-09-24 09:00:00
502,Priya,Bangalore,27,2026-09-24 09:10:00
503,Karthik,Salem,35,2026-09-24 09:20:00
```

The file was stored in:

```text
/Volumes/dataengineering/default/day17_customer_files/
```

---

## Step 2 – Bronze Table

Created:

```text
bronze.day17_customers
```

Columns:

```text
CustomerId
CustomerName
City
Age
UpdatedAt
```

The initial customer records were loaded into the Bronze table.

---

## Step 3 – Initial SCD Type 2 Table

Created:

```text
silver.day17_customers_scd2
```

Columns:

```text
CustomerId
CustomerName
City
Age
ValidFrom
ValidTo
IsCurrent
```

For the initial records:

* `ValidFrom` = `UpdatedAt`
* `ValidTo` = NULL
* `IsCurrent` = TRUE

---

## Step 4 – Customer Update File

Created `customers_updates.csv`.

```text
CustomerId,CustomerName,City,Age,UpdatedAt
501,Arun,Coimbatore,31,2026-09-25 09:00:00
502,Priya,Bangalore,28,2026-09-25 09:10:00
504,Meena,Madurai,29,2026-09-25 09:20:00
```

The update file contains:

* Customer 501 – City and Age changed
* Customer 502 – Age changed
* Customer 504 – New customer

---

## Step 5 – Auto Loader Streaming

Used Auto Loader to read the customer CSV files as a streaming DataFrame.

```python
updates_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .schema(customer_schema)
    .load(
        "/Volumes/dataengineering/default/day17_customer_files/"
    )
)
```

The streaming DataFrame successfully read the customer data.

---

## Step 6 – Bronze Update Table

The streaming data was written to:

```text
bronze.day17_customer_updates
```

The update records were then used to update the SCD Type 2 table.

---

## Step 7 – Close Old Records

For existing customers, the previous version was closed.

The old record was updated with:

```text
ValidTo = new UpdatedAt
IsCurrent = false
```

This preserves the customer's previous information.

---

## Step 8 – Insert New Versions

The new customer versions were inserted with:

```text
ValidFrom = UpdatedAt
ValidTo = NULL
IsCurrent = true
```

New customer 504 was also inserted as a current record.

---

## Final Result

The final SCD Type 2 table contains 6 records.

```text
501  Arun     Chennai       30  → old version
501  Arun     Coimbatore    31  → current version

502  Priya    Bangalore     27  → old version
502  Priya    Bangalore     28  → current version

503  Karthik  Salem         35  → current version

504  Meena    Madurai       29  → current version
```

---

## SCD Type 2 Behavior

| Customer | Old Version     | New Version        | Current          |
| -------- | --------------- | ------------------ | ---------------- |
| 501      | Chennai, Age 30 | Coimbatore, Age 31 | New version      |
| 502      | Age 27          | Age 28             | New version      |
| 503      | No change       | No new version     | Existing version |
| 504      | New customer    | —                  | New record       |

---

## Key Concepts Learned

* Auto Loader
* Structured Streaming
* Bronze and Silver layers
* Delta tables
* SCD Type 2
* Historical record tracking
* `ValidFrom`
* `ValidTo`
* `IsCurrent`
* Updating old records
* Inserting new versions
* Handling new customers

---

## Tables Created

### Bronze

```text
bronze.day17_customers
bronze.day17_customer_updates
```

### Silver

```text
silver.day17_customers_scd2
```

---

## Conclusion

Day 17 demonstrated a simple **Streaming + SCD Type 2** customer pipeline.

Customer history is preserved by keeping old records and creating a new current record whenever customer information changes.
