# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest circuits.csv file
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step1 - Read the CSV file using spark dataframe reader

# COMMAND ----------

dbutils.widgets.help()

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

circuits_df = spark.read.option("header", True).csv(f"{raw_folder_path}/circuits.csv")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType


# COMMAND ----------

circuits_schema = StructType(fields= [StructField("circuitId", IntegerType(), False),
                                      StructField("circuitRef", StringType(), False),
                                      StructField("name", StringType(), False),
                                      StructField("location", StringType(), False),
                                      StructField("country", StringType(), False),
                                      StructField("lat", DoubleType(), False),
                                      StructField("lng", DoubleType(), False),
                                      StructField("alt", DoubleType(), False),
                                      StructField("url", StringType(), False)   

])

# COMMAND ----------

circuits_df = spark.read \
    .option("header", True) \
    .schema(circuits_schema) \
    .csv(f'{raw_folder_path}/circuits.csv')


# COMMAND ----------

circuits_df.show(n = 10)

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

circuits_df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC #####Select only the required columns
# MAGIC 1. The required columns can be selected by the following four different styles
# MAGIC

# COMMAND ----------

circuits_selected_df = circuits_df.select("circuitId","circuitRef", "name", "location", "country", "lat", "lng", "alt")

# COMMAND ----------

circuits_selected_df = circuits_df.select(circuits_df.circuitId,circuits_df.circuitRef, circuits_df.name, circuits_df.location, circuits_df.country, circuits_df.lat, circuits_df.lng, circuits_df.alt)

# COMMAND ----------

circuits_selected_df = circuits_df.select(circuits_df["circuitId"],circuits_df["circuitRef"], circuits_df["name"], circuits_df["location"],
                                          circuits_df["country"], circuits_df["lat"], circuits_df["lng"], circuits_df["alt"])

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuits_selected_df = circuits_df.select(col("circuitId"),col("circuitRef"), col("name"),col("location"), 
                                          col("country"), col("lat"), col("lng"), col("alt")
)

# COMMAND ----------

display(circuits_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Rename columns as required

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId","circuit_Id")\
                    .withColumnRenamed("circuitRef","circuit_Ref")\
                    .withColumnRenamed("lat","latitude")\
                    .withColumnRenamed("lng","longitude")\
                    .withColumnRenamed("alt","altitude")
                    .withColumn("data_source", lit(v_data_source))

                    

# COMMAND ----------

display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Add ingestion date to the dataframe

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

circuits_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

dbutils.notebook.exit("success")