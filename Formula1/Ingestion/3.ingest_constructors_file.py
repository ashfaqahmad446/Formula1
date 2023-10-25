# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest results.json file
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step1 - Read the JSON file using spark dataframe reader
# MAGIC ###### We have nested objects in this Jason file so we will be using different schema

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType

# COMMAND ----------

results_schema = StructType( fields= [StructField("resultId", IntegerType(), False) , 
                                     StructField("raceId", IntegerType(), True) ,
                                     StructField("driverId", IntegerType(), True) ,
                                     StructField("number", IntegerType(), True) ,
                                     StructField("grid", IntegerType(), True) ,
                                     StructField("position", IntegerType(), True) ,
                                     StructField("positionText", StringType(), True) ,
                                     StructField("positionOrder", IntegerType(), True) ,
                                     StructField("points", FloatType(), True) ,
                                     StructField("laps", IntegerType(), True) ,
                                     StructField("time", StringType(), True) ,
                                     StructField("milliseconds", IntegerType(), True) ,
                                     StructField("fastestLap", IntegerType(), True) ,
                                     StructField("rank", IntegerType(), True) ,
                                     StructField("fastestLapTime", StringType(), True) ,
                                     StructField("fastestLapSpeed", FloatType(), True) ,
                                     StructField("statusId", StringType(), True)

])

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1adlspractice/raw

# COMMAND ----------

results_df = spark.read \
.schema(results_schema) \
.json("/mnt/formula1adlspractice/raw/results.json")

# COMMAND ----------

results_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename columns and add ingestion date
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

results_with_columns_df = results_df.withColumnRenamed("resultId", "result_id") \
                                    .withColumnRenamed("raceId", "race_id") \
                                    .withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("constructorId", "constructor_id") \
                                    .withColumnRenamed("positionText", "position_text") \
                                    .withColumnRenamed("positionOrder", "position_order") \
                                    .withColumnRenamed("fastestLap", "fastest_lap") \
                                    .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
                                    .withColumnRenamed("fastestLapSpeed", "fastes_lap_speed") \
                                    .withColumn("ingestion_date", current_timestamp()) 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Drop unwanted columns
# MAGIC
# MAGIC

# COMMAND ----------

results_final_df = results_with_columns_df.drop("statusId")

# COMMAND ----------

# MAGIC %md
# MAGIC #### step 4 - Write the output to parquet file

# COMMAND ----------

results_final_df.write.mode("overwrite").parquet("/mnt/formula1adlspractice/processed/results")

# COMMAND ----------

display(spark.read.parquet(""))