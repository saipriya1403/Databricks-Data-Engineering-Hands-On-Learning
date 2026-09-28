# Databricks notebook source
# MAGIC %md
# MAGIC ### Create the Gold schema

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW SCHEMAS;

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Create simple Silver customer data

# COMMAND ----------

customers = [
    (1, "Arun", "Chennai"),
    (2, "Kumar", "Bangalore"),
    (3, "Priya", "Chennai"),
    (4, "Ravi", "Coimbatore"),
    (5, "Meena", "Madurai")
]

customer_columns = [
                "CustomerId",
                "CustomerName",
                "City"
]

customers_df = spark.createDataFrame(
                customers,
                customer_columns
            )

display(customers_df)


# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

customers_df.write.mode("overwrite").saveAsTable(
        "silver.day20_customers"
        
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver.day20_customers
# MAGIC ORDER BY CustomerId;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Silver Product data

# COMMAND ----------

products = [
    (101, "Laptop", "Electronics", 60000),
    (102, "Mobile", "Electronics", 30000),
    (103, "Headphones", "Accessories", 3000),
    (104, "Keyboard", "Accessories", 2000),
    (105, "Monitor", "Electronics", 15000)
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


# COMMAND ----------

products_df.write.mode("overwrite").saveAsTable(
        "silver.day20_products"
        
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver.day20_products
# MAGIC ORDER BY ProductId;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Silver Order data

# COMMAND ----------

orders = [
    (1001, 1, 101, 1, "2026-09-01"),
    (1002, 2, 102, 2, "2026-09-01"),
    (1003, 3, 103, 3, "2026-09-02"),
    (1004, 1, 104, 2, "2026-09-02"),
    (1005, 4, 105, 1, "2026-09-03"),
    (1006, 5, 101, 1, "2026-09-03"),
    (1007, 2, 103, 2, "2026-09-04"),
    (1008, 3, 102, 1, "2026-09-04")
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


# COMMAND ----------

orders_df.write.mode("overwrite").saveAsTable(
        "silver.day20_orders"
        
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver.day20_orders
# MAGIC ORDER BY OrderId;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Gold Order Details

# COMMAND ----------

from pyspark.sql import functions as F

silver_orders = spark.table("silver.day20_orders")
silver_products = spark.table("silver.day20_products")

order_details = (
    silver_orders
        .join(
            silver_products,
            on="ProductId",
            how="inner"
        )
        .withColumn(
                    "OrderAmount",
                    F.col("Quantity") * F.col("Price")
        )
        )

display(order_details)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add Customer Information

# COMMAND ----------

silver_customers = spark.table("silver.day20_customers")

gold_order_details = (
    order_details
        .join(
        silver_customers,
        on="CustomerId", 
        how="inner"
        )
        .select(
            "OrderId",
            "CustomerId",
            "CustomerName",
            "City",
            "ProductId",
            "ProductName",
            "Category",
            "Quantity",
            "Price",
            "OrderAmount",
            "OrderDate"
            )
           )

display(gold_order_details)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save the Gold Order Details table

# COMMAND ----------

gold_order_details.write.mode("overwrite").saveAsTable(
        "gold.day20_order_details"
        
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM gold.day20_order_details
# MAGIC ORDER BY OrderId;

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Create Customer Revenue Analytics

# COMMAND ----------

gold_order_details = spark.table("gold.day20_order_details")

customer_revenue = (
    gold_order_details
        .groupBy(
        "CustomerId",
        "CustomerName"
       )
        .agg(
            F.countDistinct("OrderId").alias("TotalOrders"),
            F.sum("OrderAmount").alias("TotalRevenue"),
            F.avg("OrderAmount").alias("AverageOrderValue")
      )
     )

display(customer_revenue)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save Customer Revenue to Gold

# COMMAND ----------

customer_revenue.write.mode("overwrite").saveAsTable(
        "gold.day20_customer_revenue"
        
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM gold.day20_customer_revenue
# MAGIC ORDER BY CustomerId;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Overall KPI Summary

# COMMAND ----------

overall_kpis = (
    gold_order_details
        .agg(
            F.countDistinct("OrderId").alias("TotalOrders"),
            F.sum("OrderAmount").alias("TotalRevenue"),
            F.sum("Quantity").alias("TotalUnitsSold")
       )
       )

display(overall_kpis)


# COMMAND ----------

overall_kpis.write.mode("overwrite").saveAsTable(
        "gold.day20_kpi_summary"
        
)