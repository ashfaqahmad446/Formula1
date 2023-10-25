-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### Create circuits table

-- COMMAND ----------

DROP table if exists f1_raw.circuits;
CREATE TABLE IF NOT EXISTS f1_raw.circuits(circuitId INT,
circuitRef STRING,
name STRING,
location STRING,
country STRING,
lat DOUBLE,
lng DOUBLE,
alt INT,
url STRING
)
using csv
OPTIONS (path "/mnt/formula1adlspractice/raw/circuits.csv", header true)

-- COMMAND ----------

show tables in f1_raw;

-- COMMAND ----------

select * from f1_raw.circuits;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ###### Create races table 

-- COMMAND ----------

create table if not exists f1_raw.races (
raceId int,
year Int,
round Int,
circuitId Int,
name string,
date date,
time String,
url String 
)
using csv
options (path "/mnt/formula1adlspractice/raw/races.csv", header true)

-- COMMAND ----------

select * from f1_raw.races;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create tables for JSON files

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##### create constructors table
-- MAGIC ######.Single Line JSON
-- MAGIC ######.Simple Structure

-- COMMAND ----------

create table if not exists f1_raw.constructors(
constructorId int,
constructorRef string,
name STRING,
nationality string,
url string
)
using json
options(path "/mnt/formula1adlspractice/raw/constructors.json", header true)

-- COMMAND ----------

select * from f1_raw.constructors;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##### create drivers table
-- MAGIC ######.Single Line JSON
-- MAGIC ######.complex Structure

-- COMMAND ----------

DROP table if exists f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers(
  driverId INT, 
  driverRef STRING, 
  number INT,
  code string,
  name STRUCT <forename: string, surname: string>, 
  nationality STRING, 
  url STRING
)using JSON
options (path "/mnt/formula1adlspractice/raw/drivers.json", header true)

-- COMMAND ----------

select * from f1_raw.drivers;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##### create races table
-- MAGIC ######.Single Line JSON
-- MAGIC ######.Simple Structure

-- COMMAND ----------

create table if not exists f1_raw.results(
resultId Int, 
raceId Int,
driverId Int,
number Int,
grid Int,
position Int,
positionText String,
positionOrder Int,
points Float,
laps Int,
time String,
milliseconds Int,
fastestLap Int,
rank Int,
fastestLapTime String,
fastestLapSpeed Float,
statusId String
)
using json
options (path "/mnt/formula1adlspractice/raw/results.json", header true)

-- COMMAND ----------

select * from f1_raw.results;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##### create pit stops table
-- MAGIC ######.Multi Line JSON
-- MAGIC ######.Simple Structure

-- COMMAND ----------

drop table if exists f1_raw.pit_stops;
create table if not exists f1_raw.pit_stops(
driverId int,
duration string,
lap int,
milliseconds int,
raceId int,
stop int,
time string
)
using json
options (path "/mnt/formula1adlspractice/raw/pit_stops.json", multiLine true)

-- COMMAND ----------

select * from f1_raw.pit_stops;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##### create tables for list of files
-- MAGIC ######.CSV file
-- MAGIC ######.Multiple files

-- COMMAND ----------

Drop table if exists f1_raw.lap_times;
CREATE TABLE IF NOT EXISTS f1_raw.lap_times(
raceId Int,
driverId Int,
lap Int,
position String,
time String,
milliseconds Int
)
USING CSV
Options(path "path "/mnt/formula1adlspractice/raw/lap_times")

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##### create Qualifying tables
-- MAGIC ######.Multiline JSON
-- MAGIC ######.Multiple files

-- COMMAND ----------

Drop table if exists f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
qualifyId Int,
raceId Int,
driverId Int,
constructorId Int,
number Int,
position String,
q1 String,
q2 String,
q3 String,
qualifyinId INT,
raceId INT
)
USING CSV
Options(path "path "/mnt/formula1adlspractice/raw/qualifying", multiLine true)

-- COMMAND ----------

