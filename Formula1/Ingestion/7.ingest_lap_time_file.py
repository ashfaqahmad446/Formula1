# Databricks notebook source
# MAGIC %md 
# MAGIC #### Ingest lap_time folder

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read CSV file using spark API
# MAGIC ##### we will be reading multiple CSV files in a folder (read folder)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

lap_time_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("lap", IntegerType(), True),
                                    StructField("position", StringType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True)
                                   ])

# COMMAND ----------

lap_time_df = spark.read \
.schema(lap_time_schema) \
.csv("/mnt/formula1adlspractice/raw/lap_times/lap_times_split*.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp 

# COMMAND ----------

final_df = lap_time_df.withColumnRenamed("raceId", "race_id") \
                      .withColumnRenamed("driverId", "driver_id") \
                      .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Write to output to processed container in parquet format

# COMMAND ----------

final_df.write.mode("overwrite").parquet("/mnt/formula1adlspractice/processed/lap_times")

# COMMAND ----------

