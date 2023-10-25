-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Lesson Objectives
-- MAGIC ##### 1. Spark SQL documentation
-- MAGIC ##### 2. Create Database Demo
-- MAGIC ##### 3. Data tab in the UI
-- MAGIC ##### 4. SHOW command
-- MAGIC ##### 5. DESCRIBE command
-- MAGIC ##### 6. Find the current Database

-- COMMAND ----------

create database if not exists demo;

-- COMMAND ----------

show databases

-- COMMAND ----------

describe database demo

-- COMMAND ----------

describe database extended demo

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

  SHOW TABLES IN DEMO;


-- COMMAND ----------

USE demo;

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Lesson Objectives
-- MAGIC ##### 1. Create managed table using Python
-- MAGIC ##### 2. Create managed table using SQL
-- MAGIC ##### 3. Effect of dropping a managed table
-- MAGIC ##### 4. Describe table
-- MAGIC

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

use demo;
show tables;

-- COMMAND ----------

DESC EXTENDED race_results_python

-- COMMAND ----------

SELECT * FROM
  demo.race_results_python
  WHERE race_year = 2020;

-- COMMAND ----------

CREATE TABLE demo.race_results_SQL
AS
SELECT * FROM
  demo.race_results_python
  WHERE race_year = 2020;

-- COMMAND ----------

describe extended demo.race_results_SQL

-- COMMAND ----------

DROP TABLE demo.race_results_SQL

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Lesson Objectives
-- MAGIC ##### 1. Create External table using Python
-- MAGIC ##### 2. Create External table using SQL
-- MAGIC ##### 3. Effect of dropping a managed table
-- MAGIC ##### 4. Describe table
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").option("path",f"{presentation_folder_path}/race_results_py").saveAsTable("demo.race_results_py")

-- COMMAND ----------

use demo;
show tables;

-- COMMAND ----------

DESC EXTENDED demo.race_results_py

-- COMMAND ----------

SELECT * FROM
  demo.race_results_py
  WHERE race_year = 2020;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS demo.race_results_ext_sql
(
  race_year INT,
  race_name STRING,
  race_date TIMESTAMP,
  circuit_location STRING,
  driver_name STRING,
  driver_number INT,
  driver_nationality STRING,
  team STRING,
  grid INT,
  fastest_lap INT,
  race_time STRING,
  points FLOAT,
  position INT,
  created_date TIMESTAMP
)
USING PARQUET 
Location "/mnt/formula1adlspractice/presentation/race_results_ext_sql"

-- COMMAND ----------

show tables in demo

-- COMMAND ----------

INSERT INTO demo.race_results_ext_sql
SELECT * FROM demo.race_results_py

-- COMMAND ----------

select * from demo.race_results_ext_sql

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Views on tables
-- MAGIC ##### 1. Create Temp view
-- MAGIC ##### 2. Create Global Temp view
-- MAGIC ##### 3. Create permanent view
-- MAGIC
-- MAGIC

-- COMMAND ----------

select current_database();

-- COMMAND ----------

CREATE OR Replace TEMP VIEW v_race_results
AS 
SELECT * 
  FROM demo.race_results_python
  WHERE race_year = 2018;

-- COMMAND ----------

select * 
from v_race_results

-- COMMAND ----------

CREATE OR Replace global TEMP VIEW gv_race_results
AS 
SELECT * 
  FROM demo.race_results_python
  WHERE race_year = 2012;

-- COMMAND ----------

select * from global_temp.gv_race_results

-- COMMAND ----------

SHOW  TABLES IN GLOBAL_TEMP;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####### permanent view
-- MAGIC permanent view is registerd in hive meta store and it is permanent even if you restart or detach your cluster you will still be able to access the view from any cluster.

-- COMMAND ----------

CREATE OR Replace  VIEW pv_race_results
AS 
SELECT * 
  FROM demo.race_results_python
  WHERE race_year = 2000;

-- COMMAND ----------

show tables;

-- COMMAND ----------

