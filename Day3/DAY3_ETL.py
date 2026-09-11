# Databricks notebook source
# Read data from Bronze layer

df = spark.table("bronze.customers")

display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.count()

# COMMAND ----------

from pyspark.sql.functions import col, trim, initcap

df = df.withColumn(
    "CustomerName",
        trim(initcap(col("CustomerName")))
        )

df = df.withColumn(
            "City",
                trim(initcap(col("City")))
                )

df = df.withColumn(
                    "Email",
                        trim(col("Email"))
                        )

display(df)

# COMMAND ----------

df = df.withColumn(
        "Age",
            col("Age").cast("int")
            )

df.printSchema()


# COMMAND ----------

from pyspark.sql.functions import sum

df.select([
    sum(col(c).isNull().cast("int")).alias(c)
        for c in df.columns
]).display()

# COMMAND ----------

df = df.dropDuplicates(["CustomerId"])

# COMMAND ----------

df.groupBy("CustomerId") \
        .count() \
            .filter(col("count") > 1) \
                .show()

# COMMAND ----------

df = df.filter(
        col("CustomerId").isNotNull() &
            col("CustomerName").isNotNull() &
                col("Email").isNotNull() &
                    col("City").isNotNull() &
                        col("Age").isNotNull()
                        )


# COMMAND ----------

df.printSchema()
df.count()

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists silver;

# COMMAND ----------

df.write \
        .mode("overwrite") \
            .saveAsTable("silver.customers")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver.customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver.customers
# MAGIC WHERE Age > 30;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     City,
# MAGIC         COUNT(CustomerId) AS CustomerCount
# MAGIC         FROM silver.customers
# MAGIC         GROUP BY City;

# COMMAND ----------

df_filter = df.select(
        "CustomerId",
            "CustomerName",
                "City"
                )

df_filter.display()
