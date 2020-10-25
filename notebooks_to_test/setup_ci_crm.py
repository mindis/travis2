# Databricks notebook source
# MAGIC %run ../setup_general

# COMMAND ----------

dict_libs['jaydebeapi'] =''

for lib,version in dict_libs.items():
  dbutils.library.installPyPI( lib , version )

# COMMAND ----------

# MAGIC %run ./../_modules/sources_mssql
