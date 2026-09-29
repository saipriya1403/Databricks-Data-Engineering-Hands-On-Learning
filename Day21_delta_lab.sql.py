# Databricks notebook source
# MAGIC %md
# MAGIC ### Create the Practice Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.day21_delta_lab AS
# MAGIC SELECT *
# MAGIC FROM gold.day20_order_details;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM gold.day21_delta_lab;

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Check Delta Table History

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY gold.day21_delta_lab;

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Update One Order

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM gold.day21_delta_lab
# MAGIC WHERE OrderId = 1001;

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE gold.day21_delta_lab
# MAGIC SET OrderAmount = OrderAmount + 100
# MAGIC WHERE OrderId = 1001;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM gold.day21_delta_lab
# MAGIC WHERE OrderId = 1001;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY gold.day21_delta_lab;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Time Travel: See the Previous Value

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM gold.day21_delta_lab VERSION AS OF 0
# MAGIC WHERE OrderId = 1001;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM gold.day21_delta_lab
# MAGIC WHERE OrderId = 1001;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delete One Order

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM gold.day21_delta_lab
# MAGIC WHERE OrderId = 1002;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM gold.day21_delta_lab
# MAGIC WHERE OrderId = 1002;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY gold.day21_delta_lab;

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Time Travel the Deleted Record

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM gold.day21_delta_lab VERSION AS OF 2
# MAGIC WHERE OrderId = 1002;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Insert a New Record

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO gold.day21_delta_lab
# MAGIC VALUES (
# MAGIC     1009,
# MAGIC     5,
# MAGIC     'Meena',
# MAGIC     'Madurai',
# MAGIC     101,
# MAGIC     'Laptop',
# MAGIC     'Electronics',
# MAGIC     1,
# MAGIC     60000,
# MAGIC     60000,
# MAGIC     '2026-09-06'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM gold.day21_delta_lab
# MAGIC WHERE OrderId = 1009;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY gold.day21_delta_lab;

# COMMAND ----------

# MAGIC %md
# MAGIC ### OPTIMIZE

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE gold.day21_delta_lab;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY gold.day21_delta_lab;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Z-Ordering

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE gold.day21_delta_lab
# MAGIC ZORDER BY (CustomerId);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM gold.day21_delta_lab
# MAGIC WHERE CustomerId = 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ### VACUUM

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM gold.day21_delta_lab RETAIN 168 HOURS;

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC