-- Databricks notebook source
-- MAGIC %md
-- MAGIC Create External Locations Required for this Project
-- MAGIC 1. Bronze
-- MAGIC 2. Silver
-- MAGIC 3. Gold

-- COMMAND ----------

create external location if not exists dbcourseucextdatalake_bronze
url 'abfss://bronze@dbcourseucextdatalake.dfs.core.windows.net/'
with (storage credential `dbcourse-ext-storage-credential`);

-- COMMAND ----------

desc external location dbcourseucextdatalake_bronze;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC %fs
-- MAGIC ls "abfss://bronze@dbcourseucextdatalake.dfs.core.windows.net/"

-- COMMAND ----------

create external location if not exists dbcourseucextdatalake_silver
url 'abfss://silver@dbcourseucextdatalake.dfs.core.windows.net/'
with (storage credential `dbcourse-ext-storage-credential`);

-- COMMAND ----------

create external location if not exists dbcourseucextdatalake_gold
url 'abfss://gold@dbcourseucextdatalake.dfs.core.windows.net/'
with (storage credential `dbcourse-ext-storage-credential`);

-- COMMAND ----------

