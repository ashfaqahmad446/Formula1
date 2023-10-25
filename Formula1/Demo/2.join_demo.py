# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races")

# COMMAND ----------

races_filtered_races = races_df.filter("race_year = 2019 and round <=5")

# COMMAND ----------

races_filtered_races = races_df.where( (races_df["race_year"] == 2019) &(races_df["round"] <= 5))

# COMMAND ----------

display(races_filtered_races)

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_Id == races_df.circuit_Id, "inner") \
    .select(circuits_df.circuits_name, circuits_df.location, circuits_df.country, races_df.races_name,races_df.round)

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_Id == races_df.circuit_Id, "left") \
    .select(circuits_df.circuits_name, circuits_df.location, circuits_df.country, races_df.races_name,races_df.round)

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_Id == races_df.circuit_Id, "right") \
    .select(circuits_df.circuits_name, circuits_df.location, circuits_df.country, races_df.races_name,races_df.round)

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_Id == races_df.circuit_Id, "full") \
    .select(circuits_df.circuits_name, circuits_df.location, circuits_df.country, races_df.races_name,races_df.round)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Semi join, Anti join and cross join
# MAGIC ###### in semi join and anti join specify columns only from left in the select statement

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_Id == races_df.circuit_Id, "Semi") \
    .select(circuits_df.circuits_name, circuits_df.location, circuits_df.country)

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_Id == races_df.circuit_Id, "anti") 

# COMMAND ----------

race_circuits_df = races_df.crossjoin(circuits_df)