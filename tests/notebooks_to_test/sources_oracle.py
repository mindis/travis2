# Databricks notebook source
### Felles parametersetting for oracle jdbc connection strings
### Systemet må være angitt i _modules/secrets, og ha formatet 
def oracle_jdbc_url(system):
    credentials = env('ORACLE_{}'.format(system))
    return 'jdbc:oracle:thin:{USERNAME}/{PASSWORD}@{HOSTNAME}:{JDBC_PORT}:{SID}'.format(**credentials)

### Felles metode for å lese 
def oracle_table(system, tablename, schema=None):
    jdbc_url = oracle_jdbc_url(system)
    if schema:
      return spark.read.format('jdbc').options(driver='oracle.jdbc.driver.OracleDriver', url=jdbc_url, dbtable=tablename, customSchema=schema).load()
    else:
      return spark.read.format('jdbc').options(driver='oracle.jdbc.driver.OracleDriver', url=jdbc_url, dbtable=tablename).load()

### En spørring kan sendes til jdbc ved å wrappe den i en CTE og sende spørringen som argumentet 'dbtable'
def oracle_query(system, query, schema=None):
	return oracle_table(system, "({}) CTE".format(query), schema)

# COMMAND ----------

### Henter ut maxverdi for en gitt datokolonne fra Oracle på format YYYY-MM-DDTHH24:MI:SS 
### Brukes til deltalasting av Notebooks i DAG
def oracle_get_max_timestamp(system,column, table, hours=None):
  query = ""
  if hours:
    query = "SELECT REPLACE(TO_CHAR(max({}) + ({})/24,'YYYY-MM-DD HH24:MI:SS'),' ','T') AS MAX_TIME FROM {}".format(column, hours, table)
  else:
    query = "SELECT REPLACE(to_char(max({}),'YYYY-MM-DD HH24:MI:SS'),' ','T') AS MAX_TIME FROM {}".format(column, table)

  value = oracle_query(system,query).collect()[0][0]
  return value

# COMMAND ----------

### Skriver til tabell til Oracle. Oppretter hvis den ikke finnes.
def df_write_to_oracle(self, system, table_name, mode="overwrite"):
  jdbc_url = oracle_jdbc_url(system)  
  self.write.mode(mode).format('jdbc').options(driver='oracle.jdbc.driver.OracleDriver', url=jdbc_url, dbtable=table_name).save()
DataFrame.write_to_oracle = df_write_to_oracle
