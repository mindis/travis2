# Databricks notebook source
import jaydebeapi

### Felles parametersetting for mssql jdbc connection strings
### Systemet må være angitt i _modules/secrets, og ha formatet 
def mssql_jdbc_url(system):
    credentials = env('MSSQL_{}'.format(system))
    return "jdbc:sqlserver://{HOSTNAME}:{JDBC_PORT};user={USERNAME};password={PASSWORD};database={DATABASE}".format(**credentials)

### Kjører kommandoer på SQL Server (dette må gjøres med psycopg da Spark ikke har noen mekanismer for å kjøre noe annet enn read og write)
def mssql_command(system, command, print_status=False):
  if print_status: print(command)
  tilkobling = jaydebeapi.connect("com.microsoft.sqlserver.jdbc.SQLServerDriver", mssql_jdbc_url(system))
  cursor = tilkobling.cursor()
  cursor.execute(command)
  rader_pavirket = cursor.rowcount
  cursor.close()
  tilkobling.commit()
  tilkobling.close()
  if print_status: print(u"Rader påvirket: {}".format(rader_pavirket))
  return rader_pavirket
  
### Felles metode for å lese 
def mssql_table(system, tablename):
    jdbc_url = mssql_jdbc_url(system)
    return spark.read.format('jdbc').options(driver='com.microsoft.sqlserver.jdbc.SQLServerDriver', url=jdbc_url, dbtable=tablename).load()

### En spørring kan sendes til jdbc ved å wrappe den i en CTE og sende spørringen som argumentet 'dbtable'
def mssql_query(system, query):
	return mssql_table(system, '({}) CTE'.format(query))

### Skriver tabell til angitt SQL Server-tabell
def df_write_to_mssql(self, system, table_name, mode="overwrite"):
  jdbc_url = mssql_jdbc_url(system)
  self.write.mode(mode).jdbc(jdbc_url, table_name)
DataFrame.write_to_mssql = df_write_to_mssql

def df_upsert_to_mssql(self, system, table_name, upsert_key):
  temp_table_name = "{}_TEMPLOAD_{:%Y%m%d%H%M%S}_{}".format(table_name,datetime.now(), random.randint(1,9999))
  self.write_to_mssql(system, temp_table_name, mode='overwrite')
  
  ### Spesifiserer kolonnelisten for å unngå at forskjell i kolonnesortering
  ### mellom temp (lages i Spark) og target (permanent) gir problemer i lasten.
  column_list = ','.join(self.columns)
  
  ### Default schema er public. Legger det på eksplisitt ofr at eksisterende-target-sjekk under skal fungere som ønsket.
  tableschema = 'dbo'
  
  ### Sjekker om target allerede eksisterer
  check = mssql_query(system, """SELECT 1 AS X FROM information_schema.tables WHERE table_schema = '{}' AND table_name = '{}' """.format(tableschema, table_name))

  target_exists = check.select('X').distinct().count() == 1
  
  if not target_exists:
    logprint("Target {} does not exist. Doing a simple write.".format(table_name))
    df.write_to_mssql(system, table_name, mode='overwrite')
  
  else:
    logprint("Target {} already exists. Will upsert.".format(table_name))

    mssql_command(system, 
                  """SET NOCOUNT ON 
                     
                     DELETE target FROM {table_name} target 
                     JOIN {temp_table_name} temp 
                     ON target.{upsert_key} = temp.{upsert_key};
                     
                     INSERT INTO {table_name} ({column_list}) 
                     SELECT {column_list} FROM {temp_table_name};
                     """.format(table_name=table_name,
                                       temp_table_name=temp_table_name,
                                       upsert_key=upsert_key,
                                       column_list=column_list),
                  print_status=True)

DataFrame.upsert_to_mssql = df_upsert_to_mssql

# COMMAND ----------

### Henter ut maxverdi for en gitt datokolonne fra MSSQL på format YYYY-MM-DDTHH24:MI:SS 
### Brukes til deltalasting av Notebooks i DAG
def mssql_get_max_timestamp(system, column, table, hours=None):
  query = ''
  if hours:
    query = "SELECT REPLACE(CONVERT(VARCHAR,DATEADD(HOUR,{},MAX({})),20),' ','T') AS MAX_TIME FROM {}".format(hours, column, table)
  else:
    query = "SELECT REPLACE(CONVERT(VARCHAR,MAX({}),20),' ','T') AS MAX_TIME FROM  {}".format(column,table)

  value = mssql_query(system,query).collect()[0][0]
  return value
