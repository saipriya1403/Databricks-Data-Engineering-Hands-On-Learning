# Databricks notebook source
# MAGIC %md
# MAGIC ### Check your existing Day 4 customer table

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL silver.customers;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create the Day 5 practice table

# COMMAND ----------

customers_df = spark.table("silver.customers")

customers_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver.day5_customers")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from silver.day5_customers
# MAGIC order by CustomerId;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL silver.day5_customers;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check Delta History

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history silver.day5_customers;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add a New Customer

# COMMAND ----------

new_customer = [
        (11, "Nithya", "Chennai", 30)
        ]

columns = [
          "CustomerId",
           "CustomerName",
           "City",
           "Age"
]

new_df = spark.createDataFrame(
        new_customer,
        columns
)
display(new_df)

# COMMAND ----------

new_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable("silver.day5_customers")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver.day5_customers
# MAGIC ORDER BY CustomerId;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check History Again

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY silver.day5_customers;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Update an Existing Customer

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE silver.day5_customers
# MAGIC SET City = 'Coimbatore'
# MAGIC WHERE CustomerId = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *FROM silver.day5_customers
# MAGIC WHERE CustomerId = 1;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check History After UPDATE

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY silver.day5_customers;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delete a Customer

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM silver.day5_customers
# MAGIC WHERE CustomerId = 11;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver.day5_customers
# MAGIC ORDER BY CustomerId;

# COMMAND ----------

# MAGIC %md
# MAGIC ### check history

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY silver.day5_customers;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Time Travel

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY silver.day5_customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver.day5_customers VERSION AS OF 2
# MAGIC ORDER BY CustomerId;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Restore a Previous Version

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY silver.day5_customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE TABLE silver.day5_customers
# MAGIC TO VERSION AS OF 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver.day5_customers
# MAGIC ORDER BY CustomerId;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema Enforcement

# COMMAND ----------

bad_data = [
        ("ABC", "Test Customer", "Chennai", 30)
        ]

bad_df = spark.createDataFrame(
        bad_data,
        columns
)

display(bad_df)


# COMMAND ----------

bad_df.printSchema()

# COMMAND ----------

bad_df.write \
        .format("delta") \
            .mode("append") \
                .saveAsTable("silver.day5_customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inspect Delta Metadata

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL silver.day5_customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY silver.day5_customers;

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
