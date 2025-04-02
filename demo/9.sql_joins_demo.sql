-- Databricks notebook source
use f1_presentation;

-- COMMAND ----------

describe driver_standings

-- COMMAND ----------

create or replace temp view v_driver_standings_2018
as 
select race_year, driver_name, team, total_points, wins, rank
from driver_standings
where race_year = 2018;

-- COMMAND ----------

select * from v_driver_standings_2018;

-- COMMAND ----------

create or replace temp view v_driver_standings_2020
as 
select race_year, driver_name, team, total_points, wins, rank
from driver_standings
where race_year = 2020;

-- COMMAND ----------

select * from v_driver_standings_2020;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Inner Join

-- COMMAND ----------

select *
  from v_driver_standings_2018 d_2018
  join v_driver_standings_2020 d_2020
  on (d_2018.driver_name = d_2020.driver_name) 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Left Join

-- COMMAND ----------

select *
  from v_driver_standings_2018 d_2018
  left join v_driver_standings_2020 d_2020
  on (d_2018.driver_name = d_2020.driver_name) 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Right Join

-- COMMAND ----------

select *
  from v_driver_standings_2018 d_2018
  right join v_driver_standings_2020 d_2020
  on (d_2018.driver_name = d_2020.driver_name) 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Full Join

-- COMMAND ----------

select *
  from v_driver_standings_2018 d_2018
  full join v_driver_standings_2020 d_2020
  on (d_2018.driver_name = d_2020.driver_name) 

-- COMMAND ----------

-- MAGIC %md Semi Join
-- MAGIC (Inner Join Only the data from the left table)

-- COMMAND ----------

select *
  from v_driver_standings_2018 d_2018
  semi join v_driver_standings_2020 d_2020
  on (d_2018.driver_name = d_2020.driver_name) 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Anti Join

-- COMMAND ----------

select *
  from v_driver_standings_2018 d_2018
  anti join v_driver_standings_2020 d_2020
  on (d_2018.driver_name = d_2020.driver_name) 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Cross Join

-- COMMAND ----------

select *
  from v_driver_standings_2018 d_2018
  cross join v_driver_standings_2020 d_2020 

-- COMMAND ----------

