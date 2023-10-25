# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest Races.csv file
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step1 - Read the CSV file using spark dataframe reader

# COMMAND ----------

dbutils.widgets.text("p_data_source")
v_data_source = dbutils.widgets.git("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1adlspractice/raw

# COMMAND ----------

races_df = spark.read.option("header", True).csv(f'{raw_folder_path}/races.csv')

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType


# COMMAND ----------

races_schema = StructType(fields= [StructField("raceId", IntegerType(), False),
                                      StructField("year", IntegerType(), True),
                                      StructField("round", IntegerType(), True),
                                      StructField("circuitId", IntegerType(), True),
                                      StructField("name", StringType(), True),
                                      StructField("date", DateType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("url", StringType(), True)   

])

# COMMAND ----------

races_df = spark.read \
    .option("header", True) \
    .schema(races_schema) \
    .csv(f"{raw_folder_path}/races.csv")


# COMMAND ----------

races_df.show(n = 10)

# COMMAND ----------

display(races_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, col, lit, concat

# COMMAND ----------

races_with_timestamp_df = races_df.withColumn("ingestion_Date", current_timestamp()) \
                                  .withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss')) 


# COMMAND ----------

display(races_with_timestamp_df)

# COMMAND ----------

races_with_timestamp_df.printSchema()

# COMMAND ----------

races_with_timestamp_df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC #####Select only the required columns
# MAGIC 1. The required columns can be selected by the following four different styles
# MAGIC

# COMMAND ----------

races_selected_df = races_with_timestamp_df.select("raceId","year", "round", "circuitId", "name", "ingestion_Date", "race_timestamp")

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

races_selected_df = races_with_timestamp_df.select(col("raceId"),col("year").alias("race_year"), col("round"),col("circuitId"), 
                                          col("name"), col("ingestion_Date"), col("race_timestamp")
)

# COMMAND ----------

display(races_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Rename columns as required

# COMMAND ----------

races_final_df = races_selected_df.withColumnRenamed("raceId","race_Id")\
                    .withColumnRenamed("circuitId","circuit_Id")
                    .withColumn("data_source", v_data_source)
                    
                    

# COMMAND ----------

display(races_final_df)

# COMMAND ----------

races_final_df.write.mode("overwrite").partitionBy('race_year').parquet(f"{processed_folder_path}/races")

# COMMAND ----------

# MAGIC %md
# MAGIC #### write the output in parquet format

# COMMAND ----------

