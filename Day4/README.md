# Day 04 - Multi-Table ETL & Joins

## Project: RetailMart Data Engineering Platform

### Objective

The objective of Day 4 is to extend the RetailMart Data Engineering pipeline from a single customer dataset to a **multi-table ETL workflow**.

In this phase, customer, product, and order data are stored in the Silver layer. These tables are joined using their respective keys, business transformations are applied, and the resulting business-ready datasets are stored in the Gold layer.

---

## Architecture

```text
Bronze
   │
   ▼
Silver
   │
   ├── silver.customers
   ├── silver.products
   └── silver.orders
          │
          ▼
    Join & Transformation
          │
          ▼
        Gold
       ├── gold.order_details
       └── gold.customer_revenue
```

---

## Technologies Used

* Databricks
* PySpark
* Python
* Spark SQL
* Medallion Architecture
* DataFrame API

---

## Prerequisites

Before starting Day 4, the following table should already exist from Day 3:

```text
silver.customers
```

Day 4 builds on the cleaned customer data created in the previous stage.

---

# 1. Read Silver Customers

The cleaned customer data from Day 3 is loaded from the Silver layer.

```python
customers_df = spark.table("silver.customers")

display(customers_df)
```

The customer table contains information such as:

* CustomerId
* CustomerName
* Email
* City
* Age

---

# 2. Create Product Data

A product dataset is created using PySpark.

```python
products = [
    (101, "Laptop", "Electronics", 55000),
    (102, "Mobile", "Electronics", 25000),
    (103, "Headphones", "Accessories", 3000),
    (104, "Keyboard", "Accessories", 2000),
    (105, "Monitor", "Electronics", 12000)
]

product_columns = [
    "ProductId",
    "ProductName",
    "Category",
    "Price"
]

products_df = spark.createDataFrame(
    products,
    product_columns
)

display(products_df)
```

The product DataFrame is inspected using:

```python
products_df.printSchema()
```

---

# 3. Store Products in Silver

The Silver schema is created if it does not already exist.

```sql
CREATE SCHEMA IF NOT EXISTS silver;
```

The product DataFrame is stored as:

```python
products_df.write \
    .mode("overwrite") \
    .saveAsTable("silver.products")
```

The table can be verified using:

```sql
SELECT *
FROM silver.products;
```

---

# 4. Create Order Data

An order dataset is created containing relationships between customers and products.

```python
orders = [
    (1001, 1, 101, 1, "2026-09-01"),
    (1002, 2, 102, 2, "2026-09-01"),
    (1003, 3, 103, 1, "2026-09-02"),
    (1004, 1, 104, 2, "2026-09-02"),
    (1005, 4, 105, 1, "2026-09-03"),
    (1006, 5, 101, 1, "2026-09-03"),
    (1007, 6, 103, 2, "2026-09-04"),
    (1008, 7, 102, 1, "2026-09-04"),
    (1009, 8, 105, 2, "2026-09-05")
]

order_columns = [
    "OrderId",
    "CustomerId",
    "ProductId",
    "Quantity",
    "OrderDate"
]

orders_df = spark.createDataFrame(
    orders,
    order_columns
)

display(orders_df)
```

The schema and record count are checked:

```python
orders_df.printSchema()
orders_df.count()
```

---

# 5. Store Orders in Silver

The orders are stored in the Silver layer.

```python
orders_df.write \
    .mode("overwrite") \
    .saveAsTable("silver.orders")
```

The table can be verified using:

```sql
SELECT *
FROM silver.orders;
```

The Silver layer now contains three related tables:

```text
silver.customers
silver.products
silver.orders
```

---

# 6. Data Model

The three tables are connected through common keys.

### Customers

| Column       | Description                |
| ------------ | -------------------------- |
| CustomerId   | Unique customer identifier |
| CustomerName | Customer name              |
| Email        | Customer email             |
| City         | Customer city              |
| Age          | Customer age               |

### Products

| Column      | Description               |
| ----------- | ------------------------- |
| ProductId   | Unique product identifier |
| ProductName | Product name              |
| Category    | Product category          |
| Price       | Product price             |

### Orders

| Column     | Description                        |
| ---------- | ---------------------------------- |
| OrderId    | Unique order identifier            |
| CustomerId | Customer associated with the order |
| ProductId  | Product associated with the order  |
| Quantity   | Number of products ordered         |
| OrderDate  | Date of the order                  |

### Relationships

```text
Customers.CustomerId
        │
        ▼
Orders.CustomerId

Products.ProductId
        │
        ▼
Orders.ProductId
```

---

# 7. Read All Silver Tables

The three Silver tables are loaded into DataFrames.

```python
customers_df = spark.table("silver.customers")
products_df = spark.table("silver.products")
orders_df = spark.table("silver.orders")
```

---

# 8. Join Orders with Customers

Orders are joined with customers using `CustomerId`.

```python
order_customer_df = orders_df.join(
    customers_df,
    orders_df.CustomerId == customers_df.CustomerId,
    "inner"
)

display(order_customer_df)
```

An inner join is used to keep orders that have a matching customer.

---

# 9. Join with Products

The customer-order data is then joined with the product data using `ProductId`.

```python
order_details_df = order_customer_df.join(
    products_df,
    order_customer_df.ProductId == products_df.ProductId,
    "inner"
)

display(order_details_df)
```

The resulting DataFrame combines information from:

* Customers
* Orders
* Products

---

# 10. Select Required Business Columns

Only the required columns are selected for the final order details dataset.

```python
order_details_df = order_details_df.select(
    orders_df.OrderId,
    customers_df.CustomerId,
    customers_df.CustomerName,
    customers_df.City,
    products_df.ProductId,
    products_df.ProductName,
    products_df.Category,
    products_df.Price,
    orders_df.Quantity,
    orders_df.OrderDate
)

display(order_details_df)
```

---

# 11. Calculate Order Amount

A business transformation is applied to calculate the total amount for each order.

### Business Rule

```text
OrderAmount = Price × Quantity
```

PySpark implementation:

```python
from pyspark.sql.functions import col

order_details_df = order_details_df.withColumn(
    "OrderAmount",
    col("Price") * col("Quantity")
)

display(order_details_df)
```

This creates a new `OrderAmount` column.

---

# 12. Create Gold Schema

The Gold schema is created for business-ready datasets.

```sql
CREATE SCHEMA IF NOT EXISTS gold;
```

---

# 13. Create Gold Order Details

The transformed order-level data is stored in the Gold layer.

```python
order_details_df.write \
    .mode("overwrite") \
    .saveAsTable("gold.order_details")
```

Verify the table:

```sql
SELECT *
FROM gold.order_details;
```

The `gold.order_details` table provides a combined view of customer, product, and order information.

---

# 14. Calculate Customer Revenue

Customer-level revenue is calculated by grouping the order details by customer.

```python
from pyspark.sql.functions import sum

customer_revenue_df = order_details_df.groupBy(
    "CustomerId",
    "CustomerName"
).agg(
    sum("OrderAmount").alias("TotalRevenue")
)

display(customer_revenue_df)
```

This produces a business summary showing the total revenue generated by each customer.

---

# 15. Create Gold Customer Revenue Table

The customer revenue summary is stored in the Gold layer.

```python
customer_revenue_df.write \
    .mode("overwrite") \
    .saveAsTable("gold.customer_revenue")
```

Verify the table:

```sql
SELECT *
FROM gold.customer_revenue;
```

---

# 16. Business Queries

### View orders above a specific amount

```sql
SELECT *
FROM gold.order_details
WHERE OrderAmount > 20000;
```

### View customers by total revenue

```sql
SELECT *
FROM gold.customer_revenue
ORDER BY TotalRevenue DESC;
```

### View revenue by customer

```sql
SELECT
    CustomerName,
    TotalRevenue
FROM gold.customer_revenue
ORDER BY TotalRevenue DESC;
```

---

# Final Data Architecture

After completing Day 4, the RetailMart pipeline contains:

```text
                    RetailMart
                        │
                        ▼
                     Bronze
                        │
                        ▼
                     Silver
                        │
          ┌─────────────┼─────────────┐
          │             │             │
          ▼             ▼             ▼
      Customers      Products       Orders
          │             │             │
          └─────────────┼─────────────┘
                        │
                        ▼
                  Inner Joins
                        │
                        ▼
              Business Transformation
                        │
                        ▼
                      Gold
                   ┌────┴────┐
                   │         │
                   ▼         ▼
            order_details  customer_revenue
```

---

# Concepts Learned

Through Day 4, the following concepts were practiced:

* Multi-table ETL
* PySpark DataFrame operations
* Reading tables using `spark.table()`
* Creating DataFrames
* Inner joins
* Join conditions
* Primary/foreign key relationships
* Selecting required columns
* `withColumn()`
* Business transformations
* Calculating order amount
* `groupBy()`
* Aggregations using `sum()`
* Creating Gold tables
* Business-oriented data modeling
* Medallion Architecture

---

# Medallion Architecture Progress

The RetailMart project has now progressed through the following stages:

```text
Day 1
Databricks & PySpark Fundamentals
        │
        ▼
Day 2
CSV → Bronze
        │
        ▼
Day 3
Bronze → Silver
        │
        ▼
Day 4
Silver → Multi-Table Join → Gold
```

---

# Outcome

By completing Day 4, the RetailMart Data Engineering pipeline was extended from a single customer dataset to a multi-table data model.

The implementation:

* Created product and order datasets
* Stored products and orders in the Silver layer
* Used the existing cleaned customer data from Day 3
* Joined Customers, Products, and Orders
* Applied business transformations
* Calculated order-level revenue
* Created `gold.order_details`
* Calculated customer-level revenue
* Created `gold.customer_revenue`

This demonstrates how multiple cleaned datasets can be integrated and transformed into business-ready data in the Gold layer.
