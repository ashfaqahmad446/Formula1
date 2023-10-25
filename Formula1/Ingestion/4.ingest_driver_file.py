# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest constructor.json file
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step1 - Read the JSON file using spark dataframe reader

# COMMAND ----------

constructor_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1adlspractice/raw

# COMMAND ----------

constructor_df = spark.read \
.schema(constructor_schema) \
.json("/mnt/formula1adlspractice/raw/constructors.json")

# COMMAND ----------

constructor_df.printSchema()

# COMMAND ----------

display(constructor_df)

# COMMAND ----------

constructor_dropped_df = constructor_df.drop('url')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

constructor_final_df = constructor_df.withColumnRenamed("constructorId", "Constructor_Id") \
                                     .withColumnRenamed("constructorRef", "Constructor_Ref") \
                                     .withColumn("ingestion_date", current_timestamp()) 

# COMMAND ----------

# MAGIC %md
# MAGIC #### step 4 - Write the output to parquet file

# COMMAND ----------

constructor_final_df.write.mode("overwrite").parquet("/mnt/formula1adlspractice/processed/constructor")