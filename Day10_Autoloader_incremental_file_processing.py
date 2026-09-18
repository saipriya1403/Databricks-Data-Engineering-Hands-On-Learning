# Databricks notebook source
# MAGIC %md
# MAGIC ### Define the customer schema

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

customer_schema = StructType([
    StructField("CustomerId", IntegerType(), True),
    StructField("CustomerName", StringType(), True),
    StructField("City", StringType(), True),
    StructField("Age", IntegerType(), True)
])

# COMMAND ----------

customer_schema

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read the 6 CSV files using Auto Loader

# COMMAND ----------

df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .schema(customer_schema)
    .option("rescuedDataColumn", "_rescued_data")
    .option(
    "cloudFiles.schemaLocation",
    "/Volumes/dataengineering/default/day10_customer_files/_schema/"
                                                    )
    .load("/Volumes/dataengineering/default/day10_customer_files/")
                                                        )


# COMMAND ----------

# MAGIC %md
# MAGIC ### Write to Bronze

# COMMAND ----------

query = (
        df.writeStream
        .format("delta")
        .outputMode("append")
        .option(
                "checkpointLocation",
                "/Volumes/dataengineering/default/day10_customer_files/_checkpoints/"
                )
                .trigger(availableNow=True)
                .toTable("bronze.day10_customers"))
                                                






# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM bronze.day10_customers
# MAGIC ORDER BY CustomerId;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check the rescued records

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     CustomerId,
# MAGIC     CustomerName,
# MAGIC     City,
# MAGIC     Age,
# MAGIC     _rescued_data
# MAGIC FROM bronze.day10_customers
# MAGIC WHERE _rescued_data IS NOT NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC ### check the NULL values

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS TotalRecords,
# MAGIC     SUM(CASE WHEN CustomerId IS NULL THEN 1 ELSE 0 END) AS NullCustomerId,
# MAGIC     SUM(CASE WHEN CustomerName IS NULL THEN 1 ELSE 0 END) AS NullCustomerName,
# MAGIC     SUM(CASE WHEN City IS NULL THEN 1 ELSE 0 END) AS NullCity,
# MAGIC     SUM(CASE WHEN Age IS NULL THEN 1 ELSE 0 END) AS NullAge
# MAGIC FROM bronze.day10_customers;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create the invalid records

# COMMAND ----------

bronze_df = spark.read.table("bronze.day10_customers")

print("Is streaming:", bronze_df.isStreaming)

# COMMAND ----------

from pyspark.sql.functions import col

invalid_df = bronze_df.filter(
    col("CustomerId").isNull()
    | col("CustomerName").isNull()
    | col("City").isNull()
    | col("Age").isNull()
    | col("_rescued_data").isNotNull()
)

display(invalid_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create the valid records

# COMMAND ----------

valid_df = bronze_df.filter(
        col("CustomerId").isNotNull()
            & col("CustomerName").isNotNull()
                & col("City").isNotNull()
                    & col("Age").isNotNull()
                        & col("_rescued_data").isNull()
                        )

display(valid_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Save the invalid records into a quarantine table.

# COMMAND ----------

invalid_df.write \
        .format("delta") \
            .mode("overwrite") \
                .saveAsTable("silver.day10_customer_quarantine")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver.day10_customer_quarantine
# MAGIC ORDER BY CustomerId;

# COMMAND ----------

# MAGIC %md
# MAGIC ### save the valid records into the Silver table.

# COMMAND ----------

valid_df.write \
        .format("delta") \
            .mode("overwrite") \
                .saveAsTable("silver.day10_customers")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver.day10_customers
# MAGIC ORDER BY CustomerId;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify Silver and Quarantine counts

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS ValidRecords
# MAGIC FROM silver.day10_customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS InvalidRecords
# MAGIC FROM silver.day10_customer_quarantine;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add a new CSV file

# COMMAND ----------

query = (
        df.writeStream
        .format("delta")
        .outputMode("append")
        .option(
                "checkpointLocation",
                "/Volumes/dataengineering/default/day10_customer_files/_checkpoints/"
                                        )
        .trigger(availableNow=True)
        .toTable("bronze.day10_customers")
                                                )


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM bronze.day10_customers
# MAGIC WHERE CustomerId >= 125
# MAGIC ORDER BY CustomerId;