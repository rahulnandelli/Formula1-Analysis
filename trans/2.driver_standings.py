# Databricks notebook source
# MAGIC %md
# MAGIC ### Produce Driver Standings

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC Find race years for which the data is to be reprocessed

# COMMAND ----------

race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results") \
.filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

df_column_to_list

# COMMAND ----------

def df_column_to_list(input_df, column_name):
    df_row_list = input_df.select(column_name) \
                          .distinct() \
                          .collect()
    column_value_list = [row[column_name] for row in df_row_list]
    return column_value_list

race_year_list = df_column_to_list(race_results_df, "race_year")


# COMMAND ----------

    from pyspark.sql.functions import col
    
    race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results") \
    .filter(col("race_year").isin(race_year_list))

# COMMAND ----------

from pyspark.sql.functions import sum,when,count,col

# COMMAND ----------

driver_standings_df = race_results_df.groupBy("race_year","driver_name","driver_nationality").agg(sum("points").alias("total_points"),count(when(col("position")== 1,True)).alias("wins"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc,rank,asc

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = driver_standings_df.withColumn("rank",rank().over(driver_rank_spec))

# COMMAND ----------

merge_condition = "tgt.driver_name = src.driver_name and tgt.race_year = src.race_year"
merge_delta_data(final_df,'f1_presentation','driver_standings',presentation_folder_path,merge_condition,'race_year')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_presentation.driver_standings
# MAGIC where race_year = 2021;

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_year,count(1) from f1_presentation.driver_standings
# MAGIC  group by race_year
# MAGIC  order by race_year desc;

# COMMAND ----------

