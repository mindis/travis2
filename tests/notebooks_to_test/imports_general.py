# Databricks notebook source
# MAGIC %md Skriptet henter inn generelle importer for:
# MAGIC 1. General
# MAGIC 2. General Spark libraries
# MAGIC 
# MAGIC Disse er felles for de aller fleste jobber som kjører i Databricks (per mars 2020)

# COMMAND ----------

######################
### Import General ###
######################

### Required to execute %run ../setup_general
from __future__ import print_function
import logging
import pandas as pd
from six import itervalues

### Used in Redshift upsert
import random
import psycopg2

### os and json are standard, general libraries that are used a lot.
import os
import json

### 1. Date:                                                                                                  # Turnit: Definering av tidsparametere
from dateutil import tz
from dateutil.relativedelta import relativedelta
from datetime import datetime, date, timedelta

####################
### Import Spark ###
####################

from pyspark.sql import *                                                                                                     # Turnit: i add_metadata-funksjonen trengs current_timestamp-funksjonen, i definering av datatyper i en dataframe trengs .types o.l.
from pyspark.sql import Column as col
from pyspark.sql.types import * 
from pyspark.sql import DataFrame                                                                                             ### Required to execute Slack-cmd in the DAGs: 

### Må endre praksis: 
### Importerer * nå da alle funksjoner avhenger av dette. 
### Fremtidig løsning: Burde endre til å importere med: "import pyspark.sql.functions as F" 
### og endre alle kall på funksjoner til f.eks: F.current_timestamp osv.

### :( 
from pyspark.sql.functions import *    # <---- breaks python built-in functions like "max" , "round", etc

### :) 
import pyspark.sql.functions as F

### For error handling:
import py4j
