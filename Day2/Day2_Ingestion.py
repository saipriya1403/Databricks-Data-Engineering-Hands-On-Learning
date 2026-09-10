# Databricks notebook source
df = spark.read.csv('/Volumes/dataengineering/default/customer',header= True, inferSchema= True)
display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.count()

# COMMAND ----------

from pyspark.sql.functions import col, sum

df.select([
    sum(col(c).isNull().cast("int")).alias(c)
        for c in df.columns
        ]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Data Inspection

# COMMAND ----------

df.select('CustomerName','City').show()

# COMMAND ----------

# MAGIC %md
# MAGIC Filter

# COMMAND ----------

df.filter(df.City =="Chennai").show()

# COMMAND ----------

df.filter(df.Age > 30).display()

# COMMAND ----------

df.select("CustomerName").distinct().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## SAVE TO BRONZE

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists bronze;
# MAGIC

# COMMAND ----------


df.write.mode("overwrite").saveAsTable("bronze.customers")
     

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.customers;
