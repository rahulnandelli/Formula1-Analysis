# Databricks notebook source
# MAGIC %md
# MAGIC ### Access dataframes using sql
# MAGIC #### Objectives
# MAGIC 1. Create temporary Views on dataframes
# MAGIC 2. Access the view from SQL cell
# MAGIC 3. Access the view from python cell
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

race_results_df.createOrReplaceTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from v_race_results
# MAGIC where race_year = 2020

# COMMAND ----------

p_race_year = 2020

# COMMAND ----------

race_result_2019_df = spark.sql(f"select * from v_race_results where race_year = {p_race_year}")

# COMMAND ----------

display(race_result_2019_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Global Temporary views
# MAGIC 1. create global temporary view on databasae
# MAGIC 2. access the view from the sql shell
# MAGIC 3. access the view from the python shell
# MAGIC 4. access the view from another notebook

# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView("gv_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in global_temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.gv_race_results;

# COMMAND ----------

spark.sql("select * from global_temp.gv_race_results").show()

# COMMAND ----------

