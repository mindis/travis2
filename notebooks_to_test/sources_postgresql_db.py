# Databricks notebook source
def turnit_jdbc_url(environment):
  jdbcPort = 4821  
  if environment == 'PROD':
    jdbcHostname = env('TURNIT_DATABASE_HOST_PROD')
    jdbcDatabase = 'nettbuss_live'
  else:
    jdbcHostname = env('TURNIT_DATABASE_HOST')
    jdbcDatabase = 'nettbuss_testing'

  return "jdbc:postgresql://{0}:{1}/{2}".format(jdbcHostname, jdbcPort, jdbcDatabase)

# COMMAND ----------

### Metode for å hente ut hele tabeller fra Turnit databasen
def turnit_table(table, environment):
    jdbc_url = turnit_jdbc_url(environment)
    return spark.read.format('jdbc').options(driver='org.postgresql.Driver', url=jdbc_url, dbtable=table, sslmode='require', user=env('TURNIT_DATABASE_USER'), password=env('TURNIT_DATABASE_PASSWORD')).load()

# COMMAND ----------

### Sender en spørring til Turnit databasen
def turnit_query(query, environment):
  try:    
        _get_sources(query)
  except Exception as e:
        lp("Exception in turnit_query: {}".format(e) )
        
  return turnit_table('({}) CTE'.format(query), environment)
