# Databricks notebook source
# MAGIC %md
# MAGIC ### Create the  Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver.day22_customers AS
# MAGIC SELECT *
# MAGIC FROM silver.day20_customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver.day22_customers;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add New Customer Data

# COMMAND ----------

new_customers = [
        (6, "Suresh", "Salem"),
        (7, "Divya", "Trichy")
            ]

columns = ["CustomerId", "CustomerName", "City"]

new_customers_df = spark.createDataFrame(
        new_customers,
        columns
)

display(new_customers_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Find Only the New Customers

# COMMAND ----------

existing_customers = spark.table("silver.day22_customers")

new_records = (
    new_customers_df
    .join(
        existing_customers,
        on="CustomerId",
        how="left_anti"
        )
)

display(new_records)

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Write Only the New Customers

# COMMAND ----------

new_records.write.mode("append").saveAsTable(
        "silver.day22_customers"
        
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver.day22_customers
# MAGIC ORDER BY CustomerId;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test Incremental Processing

# COMMAND ----------

existing_customers = spark.table("silver.day22_customers")

new_records = (
    new_customers_df
    .join(
        existing_customers,
        on="CustomerId",
        how="left_anti"
    )
)

display(new_records)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Final Verification

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS TotalCustomers,
# MAGIC     COUNT(DISTINCT CustomerId) AS UniqueCustomers
# MAGIC FROM silver.day22_customers;