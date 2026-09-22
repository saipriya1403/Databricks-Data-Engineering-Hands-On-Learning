# Databricks notebook source
dbutils.fs.mkdirs(
        "/Volumes/dataengineering/default/day11_customer_files/day14_customer_files/"
        )


# COMMAND ----------

display(
        dbutils.fs.ls(
                "/Volumes/dataengineering/default/day11_customer_files/"
                    )
                    )


# COMMAND ----------

# MAGIC %md
# MAGIC ### Create the Silver table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.day14_customers (
# MAGIC         CustomerId INT,
# MAGIC         CustomerName STRING,
# MAGIC         City STRING,
# MAGIC         Age INT,
# MAGIC         UpdatedAt TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create the Quarantine table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.day14_customer_quarantine (
# MAGIC         CustomerId INT,
# MAGIC         CustomerName STRING,
# MAGIC         City STRING,
# MAGIC         Age INT,
# MAGIC         UpdatedAt STRING,
# MAGIC         dq_reason STRING,
# MAGIC         quarantined_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Create the Auto Loader schema

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

customer_schema = StructType([
    StructField("CustomerId", IntegerType(), True),
    StructField("CustomerName", StringType(), True),
    StructField("City", StringType(), True),
    StructField("Age", IntegerType(), True),
    StructField("UpdatedAt", StringType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create the Auto Loader stream

# COMMAND ----------

bronze_stream = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("rescuedDataColumn", "_rescued_data")
        .schema(customer_schema)
        .option(
                "cloudFiles.schemaLocation",
                "/Volumes/dataengineering/default/day11_customer_files/_day14_schema/"
                )
                .load(
                    "/Volumes/dataengineering/default/day11_customer_files/day14_customer_files/"
                    )
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Write the Bronze table

# COMMAND ----------

bronze_query = (
        bronze_stream.writeStream
            .format("delta")
            .outputMode("append")
            .option(
                    "checkpointLocation",
                    "/Volumes/dataengineering/default/day11_customer_files/_day14_bronze_checkpoints/"
                    )
                    .trigger(availableNow=True)
                    .toTable("bronze.day14_customers")
)

bronze_query.awaitTermination()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Check the Bronze data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM bronze.day14_customers
# MAGIC ORDER BY CustomerId;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS record_count
# MAGIC FROM bronze.day14_customers;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Start the Data Quality processing
# MAGIC Convert UpdatedAt to timestamp
# MAGIC
# MAGIC Check whether each record is valid
# MAGIC
# MAGIC Create a dq_reason
# MAGIC
# MAGIC Separate valid and invalid records
# MAGIC
# MAGIC Send invalid records to Quarantine
# MAGIC
# MAGIC Send valid records toward Silver

# COMMAND ----------

bronze_df = spark.read.table("bronze.day14_customers")

display(bronze_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create the valid records

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp

valid_df = (
    bronze_df
        .withColumn("UpdatedAt", to_timestamp(col("UpdatedAt")))
        .filter(
                col("CustomerId").isNotNull()
                & col("CustomerName").isNotNull()
                & col("City").isNotNull()
                & col("Age").isNotNull()
                & (col("Age") > 0)
                & col("UpdatedAt").isNotNull()
                )
                )

display(valid_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create the invalid records

# COMMAND ----------

invalid_df = (
        bronze_df
            .withColumn("UpdatedAt", to_timestamp(col("UpdatedAt")))
            .filter(
                    col("CustomerId").isNull()
                    | col("CustomerName").isNull()
                    | col("City").isNull()
                    | col("Age").isNull()
                    | (col("Age") <= 0)
                    | col("UpdatedAt").isNull()
                    )
            )

display(invalid_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ###  Add reason to invalid records

# COMMAND ----------

from pyspark.sql.functions import when, lit

invalid_df = invalid_df.withColumn(
    "dq_reason",
        when(col("CustomerName").isNull(), lit("CustomerName is NULL"))
        .when(col("City").isNull(), lit("City is NULL"))
        .when(col("Age") <= 0, lit("Age must be greater than 0"))
        .otherwise(lit("Invalid data"))
                    )

display(invalid_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Save invalid records to Quarantine

# COMMAND ----------

invalid_df = invalid_df.select(
        "CustomerId",
        "CustomerName",
        "City",
        "Age",
        "UpdatedAt",
        "dq_reason"
)


# COMMAND ----------

from pyspark.sql.functions import current_timestamp

invalid_df = invalid_df.withColumn(
    "quarantined_at",
        current_timestamp()
        )

# COMMAND ----------

invalid_df.write.mode("append").saveAsTable(
        "silver.day14_customer_quarantine"
        )


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver.day14_customer_quarantine
# MAGIC ORDER BY CustomerId;

# COMMAND ----------

valid_df = valid_df.select(
        "CustomerId",
            "CustomerName",
                "City",
                    "Age",
                        "UpdatedAt"
                        )


# COMMAND ----------

valid_df.write.mode("append").saveAsTable(
        "silver.day14_customers"
        )


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver.day14_customers
# MAGIC ORDER BY CustomerId;

# COMMAND ----------

