-- Databricks notebook source
-- MAGIC %md
-- MAGIC Drop all the tables

-- COMMAND ----------

drop database if exists f1_processed cascade;

-- COMMAND ----------

create database if not exists f1_processed
location "/mnt/f1datalake44/processed";

-- COMMAND ----------

drop database if exists f1_presentation cascade;

-- COMMAND ----------

create database if not exists f1_processed
location "/mnt/f1datalake44/presentation";

-- COMMAND ----------

