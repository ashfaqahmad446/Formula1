# Databricks notebook source
# MAGIC %md 
# MAGIC #### Ingest pit_stop.jason file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read Jason file using spark API
# MAGIC ##### we will be reading multiline Jason file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

pit_stop_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("stop", StringType(), True),
                                    StructField("lap", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("duration", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True)
                                   ])

# COMMAND ----------

pit_stop_df = spark.read \
.schema(pit_stop_schema) \
.option("multiline", True) \
.json("/mnt/formula1adlspractice/raw/pit_stops.json")

# COMMAND ----------

display(pit_stop_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp 

# COMMAND ----------

final_df = pit_stop_df.withColumnRenamed("raceId", "race_id") \
                      .withColumnRenamed("driverId", "driver_id") \
                      .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Step 3 - Drop unwanted columns

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Write to output to processed container in parquet format

# COMMAND ----------

final_df.write.mode("overwrite").partitionBy('race_id').parquet("/mnt/formula1adlspractice/processed/pit_stops")