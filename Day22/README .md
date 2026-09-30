# Day 22 – Incremental Data Processing

## Objective

Learn how to process only new customer records without inserting duplicate records into the Silver table.

## Flow

New Data
↓
Compare with Existing Data
↓
Select Only New Records
↓
Append to Silver Table
↓
Verify No Duplicates

## Step 1 – Create Silver Table

Created the Day 22 Silver table from the existing Day 20 customer data.

Table:

```text
silver.day22_customers
```

Initial customers: 5

## Step 2 – Create New Customer Data

Created two new customers:

```text
CustomerId | CustomerName | City
6           | Suresh       | Salem
7           | Divya        | Trichy
```

## Step 3 – Find New Records

Used a `left_anti` join to compare the incoming records with existing customers.

```python
new_records = (
    new_customers_df
    .join(
        existing_customers,
        on="CustomerId",
        how="left_anti"
    )
)
```

The result contained only customers that were not already present.

## Step 4 – Append New Records

Added the new customers to the Silver table.

```python
new_records.write.mode("append").saveAsTable(
    "silver.day22_customers"
)
```

The Silver table now contains 7 customers.

## Step 5 – Test Duplicate Prevention

Ran the same incoming data again.

Since customers 6 and 7 already existed, the `left_anti` join returned 0 records.

This confirmed that existing customers were not inserted again.

## Step 6 – Final Verification

Verified the total and unique customer count.

```sql
SELECT
    COUNT(*) AS TotalCustomers,
    COUNT(DISTINCT CustomerId) AS UniqueCustomers
FROM silver.day22_customers;
```

Expected result:

```text
TotalCustomers = 7
UniqueCustomers = 7
```

## Key Concepts Learned

* Incremental data processing
* Identifying new records
* `left_anti` join
* Append only new records
* Duplicate prevention
* Silver layer processing

## Final Table

```text
silver.day22_customers
```

Day 22 demonstrates a simple incremental processing pattern where only new customer records are added to the Silver layer.
