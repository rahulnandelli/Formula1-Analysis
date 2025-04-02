-- Databricks notebook source
create database if not exists f1_presentation
location "/mnt/f1datalake44/presentation"

-- COMMAND ----------

DROP DATABASE f1_presentation CASCADE;

CREATE DATABASE f1_presentation 
LOCATION 'dbfs:/mnt/f1datalake44/presentation';

-- COMMAND ----------

