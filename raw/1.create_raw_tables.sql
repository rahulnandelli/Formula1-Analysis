-- Databricks notebook source
create database if not exists f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create Circuits Table

-- COMMAND ----------


drop table if exists f1_raw.circuits;
create table if not exists f1_raw.circuits(
  circuitId int,
  circuitRef string,
  name string,
  location string,
  country string,
  lat double,
  lng double,
  alt int,
  url  string
)
USING csv
options(path "/mnt/f1datalake44/raw/circuits.csv",header = True) 

-- COMMAND ----------

select * from f1_raw.circuits;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #### Create races table

-- COMMAND ----------

drop table if exists f1_raw.races;
create table if not exists f1_raw.races(
  year int,
  round int,
  circuitId int,
  name string,
  date DATE,
  time string,
  url  string
)
USING csv
options(path "/mnt/f1datalake44/raw/races.csv",header = True) 

-- COMMAND ----------

select * from f1_raw.races;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create Tables for JSON Files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create Constructors Table
-- MAGIC -  Single Line JSON
-- MAGIC -  simple structure

-- COMMAND ----------

drop table if exists f1_raw.constructors;
create table if not exists f1_raw.constructors(
  constructorId INT, 
  constructorRef STRING, 
  name STRING, 
  nationality STRING, 
  url STRING
)
using json
options(path "/mnt/f1datalake44/raw/constructors.json")



-- COMMAND ----------

select * from f1_raw.constructors;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create drivers Table
-- MAGIC - Single Line JSON
-- MAGIC - Complex structure

-- COMMAND ----------

drop table if exists f1_raw.drivers;
create table if not exists f1_raw.drivers(
  driverId INT,
  driverRef STRING,
  number INT,
  code STRING,
  name STRUCT < forename:STRING, surname:STRING >,
  dob date,
  nationality STRING,
  url STRING)
  using json
  options(path "/mnt/f1datalake44/raw/drivers.json")


-- COMMAND ----------

select * from f1_raw.drivers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create Results Table
-- MAGIC - Single Line JSON
-- MAGIC - Simple Structure
-- MAGIC

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;
CREATE TABLE IF NOT EXISTS f1_raw.results(
resultId INT,
raceId INT,
driverId INT,
constructorId INT,
number INT,grid INT,
position INT,
positionText STRING,
positionOrder INT,
points INT,
laps INT,
time STRING,
milliseconds INT,
fastestLap INT,
rank INT,
fastestLapTime STRING,
fastestLapSpeed FLOAT,
statusId STRING)
USING json
OPTIONS(path "/mnt/f1datalake44/raw/results.json")

-- COMMAND ----------

select * from f1_raw.results;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #### Create pit stops table
-- MAGIC - Multi Line JSON
-- MAGIC - Simple Structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;
CREATE TABLE IF NOT EXISTS f1_raw.pit_stops(
driverId INT,
duration STRING,
lap INT,
milliseconds INT,
raceId INT,
stop INT,
time STRING)
USING json
OPTIONS(path "/mnt/f1datalake44/raw/pit_stops.json", multiLine true)

-- COMMAND ----------

select * from f1_raw.pit_stops;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create tables for list of files

-- COMMAND ----------

-- MAGIC  %md
-- MAGIC ##### Create Lap Times Table
-- MAGIC - CSV file
-- MAGIC - Multiple files

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;
CREATE TABLE IF NOT EXISTS f1_raw.lap_times(
raceId INT,
driverId INT,
lap INT,
position INT,
time STRING,
milliseconds INT
)
USING csv
options(path "/mnt/f1datalake44/raw/lap_times")

-- COMMAND ----------

select * from f1_raw.lap_times;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create Qualifying Table
-- MAGIC - JSON file
-- MAGIC - MultiLine JSON
-- MAGIC - Multiple files

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
constructorId INT,
driverId INT,
number INT,
position INT,
q1 STRING,
q2 STRING,
q3 STRING,
qualifyId INT,
raceId INT)
USING json
options(path "/mnt/f1datalake44/raw/qualifying", multiLine true)

-- COMMAND ----------

select * from f1_raw.qualifying;

-- COMMAND ----------

desc extended f1_raw.qualifying;

-- COMMAND ----------

