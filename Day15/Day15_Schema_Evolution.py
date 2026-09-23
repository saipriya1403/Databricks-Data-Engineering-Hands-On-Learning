# Databricks notebook source
dbutils.fs.mkdirs(
        "/Volumes/dataengineering/default/day11_customer_files/day15_customer_files/"
        )


# COMMAND ----------

# MAGIC %md
# MAGIC ### Create the schema

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
# MAGIC ### Read the file using Auto Loader

# COMMAND ----------

bronze_stream = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .schema(customer_schema)
        .load(
            "/Volumes/dataengineering/default/day11_customer_files/day15_customer_files/"
)
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Write the data to the Bronze table

# COMMAND ----------

bronze_query = (
        bronze_stream.writeStream
        .format("delta")
        .outputMode("append")
        .option(
                "checkpointLocation",
                "/Volumes/dataengineering/default/day11_customer_files/_day15_bronze_checkpoints/"
                )
                .trigger(availableNow=True)
                .toTable("bronze.day15_customers")
)

bronze_query.awaitTermination()


# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check the Bronze table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM bronze.day15_customers
# MAGIC ORDER BY CustomerId;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check the record count

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS total_customers
# MAGIC FROM bronze.day15_customers;
