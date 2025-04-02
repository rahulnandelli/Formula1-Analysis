# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC Ingest drivers.json file

# COMMAND ----------

# MAGIC %md
# MAGIC Step-1 Read the json file using spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructField,StructType,IntegerType,StringType,DataType

# COMMAND ----------

name_schema = StructType(fields=[StructField("forename",StringType(), True),StructField("surname",StringType(), True)])

# COMMAND ----------

drivers_schema = StructType(fields=[StructField("driverId",IntegerType(), False),StructField("driverRef",StringType(),True),StructField("number",IntegerType(),True),StructField("code",StringType(),True),StructField("name",name_schema),StructField("dob",StringType(),True),StructField("nationality",StringType(),True),StructField("url",StringType(),True)])

# COMMAND ----------

drivers_df = spark.read.schema(drivers_schema).json(f"{raw_folder_path}/{v_file_date}/drivers.json")

# COMMAND ----------

# MAGIC %md
# MAGIC Step-2 Rename the columns and add new columns
# MAGIC 1. driverID renamed to driver_id
# MAGIC 2. driverREF renamed to driver_ref
# MAGIC 3. ingestion date added
# MAGIC 4. name added with concatenation of forename and surname 

# COMMAND ----------

from pyspark.sql.functions import col, concat, lit

# COMMAND ----------

drivers_with_ingestion_date_df = add_ingestion_date(drivers_df)

# COMMAND ----------

driver_with_columns_df = drivers_with_ingestion_date_df.withColumnRenamed("driverId","driver_id").withColumnRenamed("driverRef","driver_ref").withColumn("name",concat(col("name.forename"),lit(" "),col("name.surname"))).withColumn("data_source", lit(v_data_source)).withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md 
# MAGIC Step-3 Drop the unwanted Columns
# MAGIC 1. name.forename
# MAGIC 2. name.surname
# MAGIC 3. url
# MAGIC

# COMMAND ----------

drivers_final_df = driver_with_columns_df.drop(col("url"))

# COMMAND ----------

# MAGIC %md
# MAGIC Step-4 Write outut to processed container in parquet format

# COMMAND ----------

drivers_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.drivers;

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

