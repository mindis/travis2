# Databricks notebook source
def rs_table_wo_metadata(table):
  
  """Returns a table (df) without metadata columns (zx_) """
  
  df = rs_table(table) 
  
  list_cols_metadata = [x for x in list(df.columns) if x.startswith('zx_')]
  
  df = df.drop(*list_cols_metadata)
  
  return df 


# COMMAND ----------

### queries the stl_load_errors table to see why the top n jobs failed (DEFAULT: 1)
def rs_load_top_error_beta(n=1):
  sqlquery = """
  
  SELECT details.* , errors.*
  FROM 
  stl_loaderror_detail AS details, 
  stl_load_errors AS errors 
WHERE 
  details.query = errors.query 
  ORDER BY starttime desc   
  
  """.format(n)

  TABLE_ERRORS = rs_query(sqlquery)###.sort_columns( ['err_reason' , 'colname' , 'raw_field_value', 'starttime'] , drop=False)
  dp_errors = TABLE_ERRORS.toPandas()
  dp_errors.index = ["*** stl_load_errors ***"]

  with pd.option_context('display.max_rows', 100, 'display.max_columns', 100, 'display.max_colwidth', -1): 
      print(dp_errors.T)
      
  

# COMMAND ----------

### queries the stl_load_errors table to see why the top n jobs failed (DEFAULT: 1)
def rs_load_top_error(n=1):
  sqlquery = """
  SELECT TOP {} *  
  FROM pg_catalog.stl_load_errors
  order by starttime desc""".format(n)

  TABLE_ERRORS = rs_query(sqlquery).sort_columns( ['err_reason' , 'colname' , 'raw_field_value', 'starttime'] , drop=False)
  dp_errors = TABLE_ERRORS.toPandas()
  dp_errors.index = ["*** pg_catalog.stl_load_errors ***"]

  with pd.option_context('display.max_rows', 100, 'display.max_columns', 100, 'display.max_colwidth', -1): 
      print(dp_errors.T)
      
  

# COMMAND ----------

def rs_exist(table_name,drop=False):

  ### Checks target existance
  target_exists = rs_command("""SELECT 1 FROM pg_tables WHERE schemaname || '.' || tablename = lower('{}');""".format(table_name)) == 1
  ###logprint( "target_exists: {}".format(target_exists) )
  if target_exists:
     logprint("Table {} exists.".format(table_name))
     if drop == True:
        rs_command("DROP TABLE " + table_name)
        logprint('Table {} dropped.'.format(table_name))
  else:
     logprint("Table {} does not exist.".format(table_name))
      
      
  return target_exists

# COMMAND ----------

### Felles parametersetting for Redshift. Kalles aldri alene, kun som en del av funksjonene under.
def dfrw_nsb_redshift(reader_or_writer):
  return reader_or_writer \
  .format("com.databricks.spark.redshift") \
  .option("url", "jdbc:redshift://{}:{}/{}?ssl=false".format(env("REDSHIFT_SERVER"), env("REDSHIFT_PORT"), env("REDSHIFT_DATABASE") ) ) \
  .option("user", env("REDSHIFT_USER") ) \
  .option("password", env("REDSHIFT_PASSWORD") ) \
  .option("aws_iam_role", env("REDSHIFT_IAM_ARN")) \
  .option("tempdir", "s3://{}".format(env("REDSHIFT_TEMP_BUCKET") )) \
  .option("tempformat","CSV")
DataFrameReader.nsb_redshift = dfrw_nsb_redshift
DataFrameWriter.nsb_redshift = dfrw_nsb_redshift

# COMMAND ----------

### Henter en tabell fra Redshift til Spark DataFrame.
def rs_table(tabellnavn, trace_lineage = True):
  try:
      write_notebook_source_and_target( [tabellnavn] ,"source")
  except Exception as e:
      lp("rs_table: exception: {}".format(e) )
  return spark.read.nsb_redshift().option("dbtable", tabellnavn).load()

# COMMAND ----------

def handle_Py4JJavaError(e):
    print('***THERE WAS A Py4JJavaError:*** \n')
    print(e.args )
    
    call_load_errors = False
    list_error = e.__str__().split("\n")
    list_error_filtered = [row for row in list_error if not row.startswith("\tat")]

    for i in range(1 , len (list_error_filtered) ):
       error = list_error_filtered[i]
       if error.startswith("Caused by"):

         print("-"*len(error) )
         print("    {} , {} ".format(str(i),error))
         print("-"*len(error) )
          
         if 'stl_load_errors' in error:
            call_load_errors = True 
    
    if call_load_errors == True:
      print("")   
      rs_load_top_error(1)
    

# COMMAND ----------

def _get_sources(query):
        ###lp("--------------TESTING DATA LINEAGE----------------------------------------------")
        list_query = query.split(" ")
        list_sources=[]
        for i in range( 0 , len (list_query) ):
            substring = list_query[i]
            if substring.startswith("public.") or substring.startswith("stage.") or substring.startswith("util.") or substring.startswith("tableau_views."):
               list_sources.append(substring)
        ###lp("list_sources: {}".format(list_sources) )
        list_sources_distinct = list(set(list_sources))
        ###lp("list_sources_distinct: {}".format(list_sources_distinct) )
        write_notebook_source_and_target(list_sources_distinct,"source")  
        ###lp("---------------------------------------------------------------------------------")
  

# COMMAND ----------

### Sender en spørring til Redshift og returnerer en DataFrame med innholdet.
def rs_query(sporring, print_query = True, trace_lineage = True):
  
  if trace_lineage == True:
      try:    
        _get_sources(sporring)
      except Exception as e:
        lp("Exception in rs_query: {}".format(e) )
  
  if print_query == True:
    lp(sporring)
  return spark.read.nsb_redshift().option("query", sporring).load()

# COMMAND ----------

def write_notebook_source_and_target(list_input, str_type):
  
  ### import as Datetime to follow class conventional notation 
  from datetime import datetime as Datetime
  
  if isinstance(list_input,str):
    raise TypeError("write_notebook_source_and_target: input should be a list")
    
  ###lp("list_input: {}".format(list_input) )
  
  list_i_stripped=[]
  for element in list_input:
        list_split_element=element.split(",")
        for i in list_split_element:
               i_stripped_raw=i.strip()                
               i_stripped=i_stripped_raw.replace("where","").replace("WHERE","").replace("\n","")
               if len(i_stripped) > 5:                  
                  list_i_stripped.append(i_stripped)  
  
  ###lp("list_i_stripped: {}".format(list_i_stripped))
  
  str_input=", ".join(list_i_stripped)
  ###lp("str_input: {}".format(str_input))
  

  name = get_notebook_name()
  STR_PATH = "{}/{}_{}.txt".format(STR_FOLDER_DATA_LINEAGE,str_type,name)
  with open(STR_PATH, 'a') as f: 
       ###f.write(name + " " + str_input)
       f.write(str_input + ", ")
       ###f.write("\n")
  ###lp("Mapped {} with {} to {}".format(name,str_input,STR_PATH))
  
          
  ### just to check what is in the file so far
  ###with open(STR_PATH, "r") as f_read:
                   ###lp("checking f_read: ")
                   ###list_written_so_far=f_read.readlines()                 
                   ###lp("write_notebook_source_and_target: list_written_so_far: {}: {}".format(str_type,list_written_so_far) )
  
  
  
  ###try:      
  STR_PATH_CSV =  "{}/{}.csv".format(STR_FOLDER_DATA_LINEAGE, name )
  import os.path
    
  if os.path.isfile(STR_PATH_CSV):
        ###lp("EXIST")
        ### 1. delta dp which will be appended to the big dp

        dp_task_i = pd.DataFrame( index = range(len(list_i_stripped) ),  columns = list_cols_task_level )###.fillna(0)
        dp_task_i["task"] = name
        dp_task_i["task_timestamp"] = Datetime.now()

        ### str_type: source or target
        dp_task_i[str_type] = list_i_stripped

        ### 2. load the main dp
        dp_all_task = pd.read_csv(STR_PATH_CSV)
        ###lp("read dp_all_task from: {}".format(STR_PATH_CSV) )


        ### 3. append
        dp_all_task=dp_all_task.append(dp_task_i)
        ###lp("FINISHED appending")

        ### 4. write the updated dp to the same csv file
        dp_all_task.to_csv(STR_PATH_CSV,index=False)
        ###dp_all_task.to_csv(STR_PATH_CSV,index=True)
        ###lp("written dp_all_task to {}".format(STR_PATH_CSV) )
        
  ###else:
   ###lp("does not exist")
  ###except Exception as e:
    ###lp("write_notebook_source_and_target: exception: {}".format(e) )

# COMMAND ----------

### Skrivelogikk: 
  ### Skriver en DataFrame til en tabell i vår Redshift-instans. OBS: Dersom tabellen finnes fra før, vil den bli overskrevet.
  ### Henter all påloggingsinfo fra miljøvariable.

### Tilleggsfunksjonalitet: 
  ### Tilrettelagt for skriving av tabeller som også inneholder kolonner med stringer lengre enn Redshift sin defaultverdi på varchar(256).
  ### Funksjonaliteten fungerer slik at den endrer Redshift sin default metadata til en gitt lengde (max_length_input) for kolonnene med navn angitt i en inputliste (list_long_strings) som finnes i inputdataframen (df). 
  ### Redshift sin absolutt maksimale lengde er 65535. Stringer lengre enn dette må splittes opp.

### Argumenter:
### Obligatorisk:
  ### tabellnavn -- navnet på tabellen som skal opprettes/overskrives
  
### Valgfritt: 
  ### mode -- insertmode. Kan være
  ###   * overwrite - sletter tabellen og skaper den på nytt (DEFAULT)
  ###   * append    - appender til eksisterende tabell
  ###   * ignore    - gjør ingenting hvis tabellen eksisterer
  ###   * error     - feiler hvis tabellen eksisterer
  ### sort -- setter en eller flere kolonner som sortkeys. Tar enten en list eller en string.
  ### diststyle -- setter distribution style for tabellen Kan være EVEN, KEY eller ALL. hvis man velger KEY må man også fylle inn distkey.
  ### distkey -- hvilken kolonne som Redshift skal distribuere etter. Bør være en man ofte spør på, og der spørringene går jevnt (kategorisk heller enn tid)
  ### list_long_strings -- liste med kolonnenavn med stringer lengre enn Redshift sin defaultverdi på varchar(256)
  ### max_length_input -- setter ønsket maksstørrelse på kolonnene som skal inneholde de lange stringene

def df_write_to_redshift(df, tabellnavn, mode='overwrite', sort=None, diststyle=None, distkey=None, list_long_strings=[], max_length_input=2000):
  
  
  
  import re
  
  ###Python 3 interprets string literals as Unicode strings, Declare your RegEx pattern as a raw string instead by prepending r
  regex=r'_temp_(\d+)'
  ### "str_tablename_cleaned" is only used in write_notebook_source_and_target
  ### the rest of the code uses just "tabellnavn"
  ###'archive.f_ci_customer_consolidation_temp_244' --->  'archive.f_ci_customer_consolidation'
  str_tablename_cleaned = re.sub( regex , "", tabellnavn )
  write_notebook_source_and_target( [str_tablename_cleaned] ,"target")
  
  
  ### if passed a string, convert to list 
  list_long_strings = str2list(list_long_strings)
  
  ### På grunn av en bug i Apache Hive skal alle tomme strenger castes til et mellomrom
  ### This behaviour is inherited from Apache Hive, so any eventual update should come from there
  ### Databricks Knowledge Base docs about this issue: https://kb.databricks.com/data/null-empty-strings.html
  for col, dtype in df.dtypes:
    if dtype == 'string':
      df = df.withColumn(col, when(df[col] == '', ' ').otherwise(df[col]))
  
  
  if list_long_strings != ['']:
      logprint('list_long_strings != ['']')
      
      # Specify the custom width of each column
      d = {}
      for x in list_long_strings: 
        d.update({x : int(max_length_input)})

      columnLengthMap = d

      # Apply each column metadata customization
      for (colName, length) in columnLengthMap.items():
        metadata = {'maxlength': length}
        df = df.withColumn(colName, df[colName].alias(colName, metadata=metadata))

      if len(list_long_strings) > 0:
        logprint('Metadata allocated for long strings')
  else: 
      logprint('list_long_strings == empty string. No metadata allocation needed.')
  
  
  logprint('Write starting to {}'.format(tabellnavn))
  # Instansierer writer:        
  writer = df.write.nsb_redshift().option('dbtable', tabellnavn).mode(mode)

  ### Legger inn en sortkey hvis noen er spesifisert
  if sort:
    if type(sort) == list: sort = ",".join(sort)
    writer = writer.option("sortkeyspec", "INTERLEAVED SORTKEY({})".format(sort))

  ### Legger inn diststyle og distkey hvis de er spesifisert
  ### NB: Dette bør man ha spesifisert på alle tabeller!
  if not diststyle and not distkey:
    logprint("Warning: You have not specified a diststyle or a distkey, defaulting to EVEN. Diststyle should be speficied to make queries against the table more efficient.")

  if not diststyle:
    diststyle = 'EVEN'
  writer = writer.option('diststyle', diststyle)
  if distkey:
    writer = writer.option('distkey', distkey)
  
  ### Try to write to Redshit, catch the Py4JJavaError exception 
  ### in order to filter the "Caused by" reason. 
  ### At the end, raise the "whole" Py4JJavaError.
  ### Any other exceptions are thrown as usual
  try:
    writer.save()
    logprint('Write completed'.format(tabellnavn))

  ### THIS SHOULD BE A METHOD ON ITS OWN IN A FUTURE RELEASE
  except py4j.protocol.Py4JJavaError as e:    
    handle_Py4JJavaError(e)
    raise
  
  
  
  except Exception as e:
    
    ### If the error string matches the strings defined in this list, it will trigger a retry 
    list_retry_errors = ['Connection refused', 
                         'could not establish the connection'
                        
                        ]
    
    if 'java.lang.NullPointerException' in str(e):
       lp("You got a NullPointerException. You can try df.test_columns() to find which column causes the error. Note: It might take too long for huge datasets")
    else:
       for str_error in list_retry_errors:
          if str_error in str(e):
             lp('{}. Will re-run after 5 minutes'.format(str_error) )
             time.sleep(300)
            
             ### Try again. Any new error (including new connection issues) will be raised
             writer.save()
      
       
    raise
    
    
    

DataFrame.write_to_redshift = df_write_to_redshift

# COMMAND ----------

### Upsertlogikk: 
  ### Skriver til en temptabell og upserter så inn i target-tabellen ved å slette alle med samme nøkkel (upsert_keys) som finnes i temp-tabellen. Deretter insertes det fra temp-tabellen til target, og til slutt slettes temp-tabellen igjen.
  ### Henter all påloggingsinfo fra miljøvariable.

### Tilleggsfunksjonalitet: 
  ### Tilrettelagt for skriving av tabeller som også inneholder kolonner med stringer lengre enn Redshift sin defaultverdi på varchar(256).
  ### Funksjonaliteten fungerer slik at den endrer Redshift sin default metadata til en gitt lengde (max_length_input) for kolonnene med navn angitt i en inputliste (list_long_strings) som finnes i inputdataframen (df). 
  ### Redshift sin absolutt maksimale lengde er 65535. Stringer lengre enn dette må splittes opp.

### Argumenter:
  ### table_name -- navnet på tabellen som skal opprettes/overskrives
  ### upsert_keys -- navnet på nøkkelen som definerer sletting for upsertlogikken
  ### list_long_strings -- liste med kolonnenavn med stringer lengre enn Redshift sin defaultverdi på varchar(256)
  ### sort -- setter en eller flere kolonner som sortkeys. Tar enten en list eller en string.
  ### diststyle -- setter distribution style for tabellen Kan være EVEN, KEY eller ALL. hvis man velger KEY må man også fylle inn distkey.
  ### distkey -- hvilken kolonne som Redshift skal distribuere etter. Bør være en man ofte spør på, og der spørringene går jevnt (kategorisk heller enn tid)
  ### max_length_input -- setter ønsket maksstørrelse på kolonnene som skal inneholde de lange stringene

def df_upsert_to_redshift(df, table_name, upsert_keys,check_consistency=True,**kwargs):
  
  ### Random tall på slutten av temp tabell for laster som skriver til samme tabell samtidig
  rand = str(random.randint(1, 999))
  
  ### Default schema er public. Legger det på eksplisitt ofr at eksisterende-target-sjekk under skal fungere som ønsket.
  if '.' not in table_name: table_name = 'public.' + table_name
  
  ### Sjekker om target allerede eksisterer
  target_exists = rs_command("""SELECT 1 FROM pg_tables WHERE schemaname || '.' || tablename = lower('{}');""".format(table_name)) == 1
  
  if not target_exists:
    logprint("Target {} does not exist. Doing a simple write.".format(table_name))
    df.write_to_redshift('{}'.format(table_name), mode='overwrite', **kwargs)
  
  else:
    logprint("Target {} already exists. Will upsert.".format(table_name))
    
    
    ### Audit consistency
    if check_consistency == True:
      comparison = SchemaComparison(df,table_name)         
      comparison.display_consistency()
    
    ### Get the schema from the table in Redshift, to ensure same column name, order and datatype
    ###df = comparison.get_schema()
    
    ### Skriver til temp-tabell først
    df.write_to_redshift('{0}_temp_{1}'.format(table_name, rand), mode='overwrite', **kwargs)
    
    ### if upsert_keys is string, convert to list so that "for k in upsert_keys" does not produce a wrong result
    if isinstance(upsert_keys, str):      
      upsert_keys = [upsert_keys]

    ### Konstruerer en WHERE-clause basert på nøklene oppgitt
    upsert_key_where_clause = ' AND '.join(['{table_name}.{k} = {table_name}_temp_{rand}.{k}'.format(table_name=table_name, rand=rand, k=k) for k in upsert_keys])

    logprint('Upserting (delete+insert) from temp table to target {}.'.format(table_name))
    rs_command("""BEGIN TRANSACTION;
                   DELETE FROM {table_name} 
                         USING {table_name}_temp_{rand}
                         WHERE {upsert_key_where_clause}; 
                   INSERT INTO {table_name} ({column_string}) SELECT {column_string} FROM {table_name}_temp_{rand};
                   DROP TABLE {table_name}_temp_{rand};
                   END TRANSACTION;""".format(table_name=table_name, 
                                              upsert_key_where_clause=upsert_key_where_clause, 
                                              rand=rand,
                                              column_string=', '.join(df.columns) ))
    
DataFrame.upsert_to_redshift = df_upsert_to_redshift

# COMMAND ----------

### Slettelogikk:
### Slett spesifikke rader i en Redshift-tabell (tilsvarer første del av Upsertlogikken): 
  ### Skriver til en temptabell og sletter alle target-rader med samme nøkkel (delete_upsert_keys) som finnes i temp-tabellen. Til slutt slettes temp-tabellen igjen.
  ### Henter all påloggingsinfo fra miljøvariable.

### Tilleggsfunksjonalitet: 
  ### Tilrettelagt for skriving av tabeller som også inneholder kolonner med stringer lengre enn Redshift sin defaultverdi på varchar(256).
  ### Funksjonaliteten fungerer slik at den endrer Redshift sin default metadata til en gitt lengde (max_length_input) for kolonnene med navn angitt i en inputliste (list_long_strings) som finnes i inputdataframen (df). 
  ### Redshift sin absolutt maksimale lengde er 65535. Stringer lengre enn dette må splittes opp.

### Argumenter:
  ### table_name -- navnet på tabellen som skal opprettes/overskrives
  ### delete_keys -- navnet på nøkkelen som definerer sletting for upsertlogikken
  ### list_long_strings -- liste med kolonnenavn med stringer lengre enn Redshift sin defaultverdi på varchar(256)
  ### sort -- setter en eller flere kolonner som sortkeys. Tar enten en list eller en string.
  ### diststyle -- setter distribution style for tabellen Kan være EVEN, KEY eller ALL. hvis man velger KEY må man også fylle inn distkey.
  ### distkey -- hvilken kolonne som Redshift skal distribuere etter. Bør være en man ofte spør på, og der spørringene går jevnt (kategorisk heller enn tid)
  ### max_length_input -- setter ønsket maksstørrelse på kolonnene som skal inneholde de lange stringene

def df_delete_rows_redshift(df, table_name, delete_keys, check_consistency=True, **kwargs):
  
  ### Sjek om df'en inneholder rader
  if df.count() == 0:
    logprint("df_remove is empty, nothing to be deleted. Exit without deleting rows.")

  else:
    logprint("df_remove is not empty. Proceed with delete process.")
  
    ### Random tall på slutten av temp tabell for laster som skriver til samme tabell samtidig
    rand = str(random.randint(1, 999))
  
    ### Default schema er public. Legger det på eksplisitt ofr at eksisterende-target-sjekk under skal fungere som ønsket.
    #if '.' not in table_name: table_name = 'public.' + table_name
  
    ### Sjekker om target allerede eksisterer
    target_exists = rs_command("""SELECT 1 FROM pg_tables WHERE schemaname || '.' || tablename = lower('{}');""".format(table_name)) == 1
  
    if not target_exists:
      logprint("Target {} does not exist. Exit without deleting rows.".format(table_name))
  
    else:
      logprint("Target {table_name} exists. Will delete rows based on the delete_keys {delete_keys}.".format(table_name=table_name, delete_keys=delete_keys))
    
    
      ### Audit consistency
      if check_consistency == True:
        comparison = SchemaComparison(df,table_name)        
        comparison.display_consistency()
      
      ### Get the schema from the table in Redshift, to ensure same column name, order and datatype
      ###df = comparison.get_schema()
    
      ### Skriver til temp-tabell først
      df.write_to_redshift('{0}_temp_{1}'.format(table_name, rand), mode='overwrite', **kwargs)
    
      ### if delete_keys is string, convert to list so that "for k in upsert_keys" does not produce a wrong result
      if isinstance(delete_keys, str):      
        delete_keys = [delete_keys]

      ### Konstruerer en WHERE-clause basert på nøklene oppgitt
      delete_keys_where_clause = ' AND '.join(['{table_name}.{k} = {table_name}_temp_{rand}.{k}'.format(table_name=table_name, rand=rand, k=k) for k in delete_keys])

      logprint('Deleting temp table rows from target {table_name} based on the delete_keys {delete_keys}.'.format(table_name=table_name, delete_keys=delete_keys))
      rs_command("""BEGIN TRANSACTION;
                     DELETE FROM {table_name} 
                           USING {table_name}_temp_{rand}
                           WHERE {delete_keys_where_clause}; 
                     END TRANSACTION;""".format(table_name=table_name, 
                                                delete_keys_where_clause=delete_keys_where_clause, 
                                                rand=rand,
                                                column_string=', '.join(df.columns) ))
      
DataFrame.delete_rows_redshift = df_delete_rows_redshift

# COMMAND ----------

### Kjører en SQL-kommando mot Redshift. 
### Bruker psycopg siden Spark kun kan kjøre write og read. 
### Returnerer antall rader påvirket av spørringen, eller antall rader returnert.
def rs_command(command, print_status=False):
  tilkobling = psycopg2.connect(dbname =   env('REDSHIFT_DATABASE'), 
                                host =     env('REDSHIFT_SERVER'), 
                                port =     env('REDSHIFT_PORT'), 
                                user =     env('REDSHIFT_USER'), 
                                password = env('REDSHIFT_PASSWORD'))
  cursor = tilkobling.cursor()
  cursor.execute(command)
  if print_status: logprint(command)
  rows_affected = cursor.rowcount
  cursor.close()
  tilkobling.commit()
  tilkobling.close()
  if print_status: logprint(u"Rows affected: {}".format(rows_affected))
  return rows_affected

# COMMAND ----------

### Henter ut maxverdi for en gitt datokolonne fra Redshift på format YYYY-MM-DDTHH24:MI:SS 
### Brukes til deltalasting av Notebooks i DAG
def rs_get_max_timestamp(column, table, hours=None, source=None):
  query = ""
  if hours:
    query = "SELECT REPLACE(TO_CHAR(DATEADD('hour',{},max({})),'YYYY-MM-DD HH24:MI:SS'),' ','T') FROM {}".format(hours, column, table)
  if source:
    query = "SELECT REPLACE(to_char(max({}),'YYYY-MM-DD HH24:MI:SS'),' ','T') FROM {} where zx_source_system ='{}' ".format(column, table, source)
  else:
    query = "SELECT REPLACE(to_char(max({}),'YYYY-MM-DD HH24:MI:SS'),' ','T') FROM {}".format(column, table)

  value = rs_query(query).collect()[0][0]
  lp("value: {}".format(value) )
  
  return value
