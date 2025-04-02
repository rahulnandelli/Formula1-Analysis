# Databricks notebook source
# MAGIC %md
# MAGIC #### Explore DBFS Root
# MAGIC 1. List All the folders in DBFS Root
# MAGIC 2. Interact with DBFS File Browser
# MAGIC 3. Upload File to DBFS Root
# MAGIC

# COMMAND ----------

display(dbutils.fs.ls('/'))

# COMMAND ----------

display(dbutils.fs.ls('/FileStore/tables'))
#dbfs:/FileStore/tables/circuits.csv

# COMMAND ----------

display(spark.read.csv('/FileStore/tables/circuits.csv'))

# COMMAND ----------

