# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC DROP TABLE IF EXISTS silver.day17_customers_scd2;
# MAGIC DROP TABLE IF EXISTS bronze.day17_customers;

# COMMAND ----------

from pyspark.sql.functions import col

initial_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(
           "/Volumes/dataengineering/default/day17_customer_files/customers_initial.csv"
            )
                                )

initial_df = initial_df.withColumn(
                                   "UpdatedAt",
                                    col("UpdatedAt").cast("timestamp")
                                        )

display(initial_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create the Bronze table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE bronze.day17_customers (
# MAGIC     CustomerId INT,
# MAGIC     CustomerName STRING,
# MAGIC     City STRING,
# MAGIC     Age INT,
# MAGIC     UpdatedAt TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

initial_df.write \
        .mode("append") \
            .saveAsTable("bronze.day17_customers")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM bronze.day17_customers
# MAGIC ORDER BY CustomerId;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create the SCD Type 2 Silver table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE silver.day17_customers_scd2 (
# MAGIC     CustomerId INT,
# MAGIC     CustomerName STRING,
# MAGIC     City STRING,
# MAGIC     Age INT,
# MAGIC     ValidFrom TIMESTAMP,
# MAGIC     ValidTo TIMESTAMP,
# MAGIC     IsCurrent BOOLEAN
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

from pyspark.sql.functions import col, lit

initial_scd2_df = (
    initial_df
        .withColumn("ValidFrom", col("UpdatedAt"))
        .withColumn("ValidTo", lit(None).cast("timestamp"))
        .withColumn("IsCurrent", lit(True))
        .select(
                "CustomerId",
                "CustomerName",
                "City",
                "Age",
                "ValidFrom",
                "ValidTo",
                "IsCurrent"
                )
                )

initial_scd2_df.write \
                       .mode("append") \
                        .saveAsTable("silver.day17_customers_scd2")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM silver.day17_customers_scd2
# MAGIC ORDER BY CustomerId;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read the update CSV as a stream

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType

customer_schema = StructType([
    StructField("CustomerId", IntegerType(), True),
        StructField("CustomerName", StringType(), True),
            StructField("City", StringType(), True),
                StructField("Age", IntegerType(), True),
                    StructField("UpdatedAt", TimestampType(), True)
                    ])

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

print("Is streaming:", updates_stream.isStreaming)

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Start the stream with a checkpoint

# COMMAND ----------

query = (
        updates_stream.writeStream
        .format("delta")
        .option(
                "checkpointLocation",
                 "/Volumes/dataengineering/default/day17_customer_files/_checkpoint/"
                )
                .trigger(availableNow=True)
                .toTable("bronze.day17_customer_updates")
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Check the streaming Bronze table

# COMMAND ----------

display(
        dbutils.fs.ls(
                "/Volumes/dataengineering/default/day17_customer_files/"
                    )
                    )


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM bronze.day17_customer_updates
# MAGIC ORDER BY CustomerId;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Close the old versions

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC MERGE INTO silver.day17_customers_scd2 AS target
# MAGIC USING (
# MAGIC     SELECT
# MAGIC         CustomerId,
# MAGIC         UpdatedAt
# MAGIC     FROM bronze.day17_customer_updates
# MAGIC    ) AS source
# MAGIC     ON target.CustomerId = source.CustomerId
# MAGIC     AND target.IsCurrent = true
# MAGIC
# MAGIC     WHEN MATCHED
# MAGIC     AND source.UpdatedAt > target.ValidFrom
# MAGIC     THEN UPDATE SET
# MAGIC     target.ValidTo = source.UpdatedAt,
# MAGIC     target.IsCurrent = false;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Insert the new versions

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC INSERT INTO silver.day17_customers_scd2
# MAGIC SELECT
# MAGIC     CustomerId,
# MAGIC     CustomerName,
# MAGIC     City,
# MAGIC     Age,
# MAGIC     UpdatedAt AS ValidFrom,
# MAGIC     NULL AS ValidTo,
# MAGIC     true AS IsCurrent
# MAGIC FROM bronze.day17_customer_updates
# MAGIC WHERE UpdatedAt > (
# MAGIC                     SELECT COALESCE(MAX(ValidFrom), TIMESTAMP('1900-01-01'))
# MAGIC                     FROM silver.day17_customers_scd2
# MAGIC                     WHERE silver.day17_customers_scd2.CustomerId =
# MAGIC                     bronze.day17_customer_updates.CustomerId
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Verify the final SCD Type 2 table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     CustomerId,
# MAGIC     CustomerName,
# MAGIC     City,
# MAGIC     Age,
# MAGIC     ValidFrom,
# MAGIC     ValidTo,
# MAGIC     IsCurrent
# MAGIC FROM silver.day17_customers_scd2
# MAGIC ORDER BY CustomerId, ValidFrom;
