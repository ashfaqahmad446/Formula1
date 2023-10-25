# Databricks notebook source
v_result = dbutils.notebook.run("1.ingest_circuits_file", 0, {"p_data_source":"Ergast API"})

# COMMAND ----------

v_result = dbutils.notebook.run("2.ingest_Races_file", 0, {"p_data_source":"Ergast API"})

# COMMAND ----------

v_result = dbutils.notebook.run("3.ingest_constructors_file", 0, {"p_data_source":"Ergast API"})

# COMMAND ----------

v_result = dbutils.notebook.run("4.ingest_driver_file", 0, {"p_data_source":"Ergast API"})

# COMMAND ----------

v_result = dbutils.notebook.run("5.inges_results_file", 0, {"p_data_source":"Ergast API"})

# COMMAND ----------

v_result = dbutils.notebook.run("6.ingest_pits_stop_file", 0, {"p_data_source":"Ergast API"})

# COMMAND ----------

v_result = dbutils.notebook.run("7.ingest_lap_time", 0, {"p_data_source":"Ergast API"})

# COMMAND ----------

v_result = dbutils.notebook.run("8.ingest_qualifying_file", 0, {"p_data_source":"Ergast API"})