# Databricks notebook source
# MAGIC %md 
# MAGIC #### Ingest qualifying jason files  
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read jason file using spark API
# MAGIC ##### we will be reading multiple jason files with multiline code in a folder (read folder)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                    StructField("raceId", IntegerType(), True),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("constructorId", IntegerType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("position", StringType(), True),
                                    StructField("q1", StringType(), True),
                                    StructField("q2", StringType(), True),
                                    StructField("q3", StringType(), True)
                                   ])

# COMMAND ----------

qualifying_df = spark.read \
.schema(qualifying_schema) \
.option("multiline", True) \
.json("/mnt/formula1adlspractice/raw/qualifying/qualifying_split*.json")

# COMMAND ----------

display(qualifying_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp 

# COMMAND ----------

final_df = qualifying_df.withColumnRenamed("qualifyId", "qualify_id") \
                      .withColumnRenamed("raceId", "race_id") \
                      .withColumnRenamed("driverId", "driver_id") \
                      .withColumnRenamed("constructorId", "constructor_id") \
                      .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Write to output to processed container in parquet format

# COMMAND ----------

final_df.write.mode("overwrite").parquet("/mnt/formula1adlspractice/processed/qualifying")

# COMMAND ----------

dbutils.notebook.exit('success')