# Databricks notebook source
display(
        dbutils.fs.ls(
                "/Volumes/dataengineering/default/day16_customer_files/"
                    )
                    )


# COMMAND ----------

# MAGIC %md
# MAGIC ###  Read the initial CSV

# COMMAND ----------

initial_df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(
              "/Volumes/dataengineering/default/day16_customer_files/customers_initial.csv"
                                    )
                                    )

display(initial_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Create the Bronze table

# COMMAND ----------

initial_df.write \
        .mode("overwrite") \
            .format("delta") \
                .saveAsTable("bronze.day16_customers")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM bronze.day16_customers
# MAGIC ORDER BY CustomerId;

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Create the SCD Type 1 table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.day16_customers_scd1 (
# MAGIC     CustomerId INT,
# MAGIC     CustomerName STRING,
# MAGIC     City STRING,
# MAGIC     Age INT,
# MAGIC     UpdatedAt TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

initial_df.write \
        .mode("overwrite") \
            .saveAsTable("silver.day16_customers_scd1")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver.day16_customers_scd1
# MAGIC ORDER BY CustomerId;

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Read the update CSV

# COMMAND ----------

updates_df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(
                "/Volumes/dataengineering/default/day16_customer_files/customers_updates.csv"
                                    )
                                    )

display(updates_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Perform the SCD Type 1 MERGE

# COMMAND ----------

from delta.tables import DeltaTable

silver_scd1 = DeltaTable.forName(
    spark,
        "silver.day16_customers_scd1"
)

(
        silver_scd1.alias("target")
        .merge(
                updates_df.alias("source"),
                "target.CustomerId = source.CustomerId"
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver.day16_customers_scd1
# MAGIC ORDER BY CustomerId;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create the SCD Type 2 table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.day16_customers_scd2 (
# MAGIC     CustomerId INT,
# MAGIC     CustomerName STRING,
# MAGIC     City STRING,
# MAGIC     Age INT,
# MAGIC     UpdatedAt TIMESTAMP,
# MAGIC     StartDate TIMESTAMP,
# MAGIC     EndDate TIMESTAMP,
# MAGIC     IsCurrent BOOLEAN
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepare the initial SCD Type 2 data

# COMMAND ----------

from pyspark.sql.functions import col, lit

initial_scd2_df = (
    initial_df
        .withColumn("StartDate", col("UpdatedAt").cast("timestamp"))
            .withColumn("EndDate", lit(None).cast("timestamp"))
                .withColumn("IsCurrent", lit(True))
                )

display(initial_scd2_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load the initial SCD Type 2 data

# COMMAND ----------

initial_scd2_df.write \
        .mode("overwrite") \
            .saveAsTable("silver.day16_customers_scd2")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver.day16_customers_scd2
# MAGIC ORDER BY CustomerId;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Close the old SCD Type 2 records

# COMMAND ----------

from delta.tables import DeltaTable

silver_scd2 = DeltaTable.forName(
    spark,
        "silver.day16_customers_scd2")
(
                silver_scd2.alias("target")
                    .merge(
                            updates_df.alias("source"),
                                    "target.CustomerId = source.CustomerId AND target.IsCurrent = true")
                .whenMatchedUpdate(
                            condition="""
                                        target.CustomerName <> source.CustomerName
                                                    OR target.City <> source.City
                                                                OR target.Age <> source.Age
                                                                        """,
                
                set={
                                "EndDate": "source.UpdatedAt",
                                            "IsCurrent": "false"
                }).execute() )            
        



# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver.day16_customers_scd2
# MAGIC ORDER BY CustomerId, StartDate;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Insert the new SCD Type 2 versions

# COMMAND ----------

from pyspark.sql.functions import col,lit
new_versions_df = (
        updates_df
        .withColumn("StartDate", col("UpdatedAt").cast("timestamp"))
        .withColumn("EndDate", lit(None).cast("timestamp"))
        .withColumn("IsCurrent", lit(True))
)

new_versions_df.write \
                        .mode("append") \
                            .saveAsTable("silver.day16_customers_scd2")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Show only the current customer records

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver.day16_customers_scd2
# MAGIC WHERE IsCurrent = true
# MAGIC ORDER BY CustomerId;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check the history of customer 401

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     CustomerId,
# MAGIC     CustomerName,
# MAGIC     City,
# MAGIC     Age,
# MAGIC     StartDate,
# MAGIC     EndDate,
# MAGIC     IsCurrent
# MAGIC FROM silver.day16_customers_scd2
# MAGIC WHERE CustomerId = 401
# MAGIC ORDER BY StartDate;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver.day16_customers_scd1
# MAGIC ORDER BY CustomerId;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver.day16_customers_scd2
# MAGIC ORDER BY CustomerId, StartDate;

# COMMAND ----------

# MAGIC %md
# MAGIC