# Databricks notebook source
# MAGIC %md Skriptet henter inn:
# MAGIC 1. Importer
# MAGIC 2. Setup
# MAGIC 
# MAGIC som er nødvendige for å kjøre alle Turnit-jobber som kjører i Databricks (per mars 2020), spesifikt DAGen "sales_nightly_init_turnit_parallel_nbpool". 

# COMMAND ----------

#####################
### Import Turnit ###
#####################

### Required to execute the DAG: "sales_nightly_init_turnit_parallel_nbpool"

### Sharepoint:                                                                                                                # Turnit: Mapping av Linjenavn og Linjenummer mot en mappingtabell i Sharepoint
import requests
from requests.auth import HTTPBasicAuth
from requests_ntlm import HttpNtlmAuth

# Redshift tilkobling:                                                                                                         # Turnit: tilkobling = psycopg2.connect(dbname = env('REDSHIFT_DATABASE'), host =  env('REDSHIFT_SERVER'),port = env('REDSHIFT_PORT'))
import psycopg2       
from psycopg2.extras import DictCursor                                                                                         

# Annet: 
import random                                                                                                                  #Turnit: Random tall på slutten av temp-tabeller ved upserting

# COMMAND ----------

####################
### Setup Turnit ###
####################

# COMMAND ----------

# (Turnit) Other: Spesifikt mthList-funksjonen

# COMMAND ----------

# MAGIC %run ../_modules/other

# COMMAND ----------

# (Turnit) Sharepoint: Spesifikt mappe Linjenummer og Linjenavn

# COMMAND ----------

# MAGIC %run ../_modules/sources_sharepoint

# COMMAND ----------

# (Turnit) Query functions: Data load from Turnit Postgresql database

# COMMAND ----------

# MAGIC %run ../_modules/sources_postgresql_db
