# Databricks notebook source
### NEW 
def df_get_schema_for_upsert(df, upsert_table_name, arg_init):    
  
  ### temp variable for better performance
  target_exists = rs_exist(upsert_table_name)
  
  if target_exists and arg_init == '1':
      drop_table(upsert_table_name)      
      #print(f'Table {upsert_table_name}. Returning the same df untouched')
      print('Table {}. Returning the same df untouched'.format(upsert_table_name))
      return df
      
  elif target_exists and arg_init != '1':
      list_column_names = get_schema_names_from_redshift(upsert_table_name)
      # Sort and keep only the same columns as in arg_upsert_table_name
      df_updated = df.sort_columns(list_column_names, drop =True)
      return df_updated
  
  else:
    print('target does not exist - returning the same df untouched')
    return df
    
DataFrame.get_schema_for_upsert = df_get_schema_for_upsert

# COMMAND ----------

def drop_table(table_name):
    rs_command("DROP TABLE {}".format(table_name))
    print('Target dropped') 

# COMMAND ----------

def get_schema(table_name):
  return rs_query(""" select top 1 * from {}""".format(table_name)).schema

# COMMAND ----------

def get_schema_names_from_redshift(table_name):
    
    df_target = rs_query(""" select top 1 * from {}""".format(table_name))
      
    return df_target.schema.names

# COMMAND ----------

def df_normalize(df):
  list_rows=df.collect()
  list_dicts=[]
  for row_i in list_rows:
    dict_row_i = row_i.asDict(recursive=True)
    dict_normalized_i = normalize_dict(dict_row_i)
    list_dicts.append(dict_normalized_i) 

  ### Fill Null with empty  to avoid "Can not merge type <class 'pyspark.sql.types.StringType'> and <class 'pyspark.sql.types.DoubleType'>"
  ### NaN is "numeric" data type: https://stackoverflow.com/questions/17534106/what-is-the-difference-between-nan-and-none
  ### Therefore, a column containing both Nan and strings, will fail when converting to spark (because spark doesnt allow 1 columns with 2 data types)
   
  
  ### Fill both None (object) and NaN ("Numeric" None) with string "empty" 
  dp = pd.DataFrame(list_dicts).fillna('empty')
  
  for c in dp.columns:
    dp[c] = dp[c].astype(str)
  
  import numpy as np 
  print(dp.head(3))
  df = spark.createDataFrame(dp) #field invoice_refund_amount: Can not merge type <class 'pyspark.sql.types.DoubleType'> and <class 'pyspark.sql.types.StringType'>
  
  ### replace all strings "empty" with the original Null ("None") 
  for c in df.columns:
     df=df.withColumn(c, when(col(c) == 'empty', lit(None)).otherwise(col(c)))
  
  
  return df

DataFrame.normalize = df_normalize

# COMMAND ----------

def df_replace_in_headers(df,old,new):
  
  ### [A,B,C]
  list_old_headers = list(df.columns)
  
  for c in list_old_headers:
    c_renamed = c.replace(old, new)
    df = df.withColumnRenamed(c, c_renamed) 
  
  ### [X,B,C]
  list_new_headers = list(df.columns)
  
  ### [X]
  list_headers_changed = list(set(list_new_headers) - set(list_old_headers))
  
  
  for c_old, c_new in zip(list_old_headers, list_new_headers):
    if c_new in list_new_headers:
      #lp(f'Renamed {c} ---> {c_new}')
      lp('Renamed {} ---> {}'.format(c,c_new))
  
  return df  

DataFrame.replace_in_headers = df_replace_in_headers

# COMMAND ----------

def df_sort_alphabetically_with_primary_key_first(df, key, print_shape =True):
  
  cols = df.columns
  list_cols_sorted_alphabetically   = sorted(cols)
  
  ### first sort, alphabetically
  df = df.sort_columns(list_cols_sorted_alphabetically, drop=False)
  
  ### second sort, with selected field first
  list_key = str2list(key)
  df_alph_primary_key = df.sort_columns(list_key, drop=False) 
  
  return df_alph_primary_key
  
DataFrame.sort_alphabetically_with_primary_key_first = df_sort_alphabetically_with_primary_key_first  

# COMMAND ----------

### Inspired on equivalent Pandas method
def df_shape(df): 
  return print((df.count(), len(df.columns)))
DataFrame.shape = df_shape  

# COMMAND ----------

def df_select_cols(df,list_cols_used):
  lp("The following source columns will be kept:")
  print("[")
  for c in list_cols_used: 
    print(c +",")
  print("]")
  print("")
  
  list_cols_dissapear = list ( set(df.columns) - set(list_cols_used) )
  lp("The following source columns will be dropped:")
  print("[")
  for c in list_cols_dissapear: 
    print(c +",")
  print("]")
  print("")
    
  return df.select(list_cols_used)

DataFrame.select_cols = df_select_cols

# COMMAND ----------

### From: https://stackoverflow.com/questions/39758045/how-to-perform-union-on-two-dataframes-with-different-amounts-of-columns-in-spark
### WORKS WELL. 
### IMPROVE NOTATION ACCORDING TO OUR STANDARDS? 
from pyspark.sql.functions import lit


def __order_df_and_add_missing_cols(df, columns_order_list, df_missing_fields):
    """ return ordered dataFrame by the columns order list with null in missing columns """
    if not df_missing_fields:  # no missing fields for the df
        return df.select(columns_order_list)
    else:
        columns = []
        for colName in columns_order_list:
            if colName not in df_missing_fields:
                columns.append(colName)
            else:
                columns.append(lit(None).alias(colName))
        return df.select(columns)


def __add_missing_columns(df, missing_column_names):
    """ Add missing columns as null in the end of the columns list """
    list_missing_columns = []
    for col in missing_column_names:
        list_missing_columns.append(lit(None).alias(col))

    return df.select(df.schema.names + list_missing_columns)


def __order_and_union_d_fs(left_df, right_df, left_list_miss_cols, right_list_miss_cols):
    """ return union of data frames with ordered columns by left_df. """
    left_df_all_cols = __add_missing_columns(left_df, left_list_miss_cols)
    right_df_all_cols = __order_df_and_add_missing_cols(right_df, left_df_all_cols.schema.names,
                                                        right_list_miss_cols)
    return left_df_all_cols.union(right_df_all_cols)


def union_d_fs(left_df, right_df):
    """ Union between two dataFrames, if there is a gap of column fields,
     it will append all missing columns as nulls """
    # Check for None input
    if left_df is None:
        raise ValueError('left_df parameter should not be None')
    if right_df is None:
        raise ValueError('right_df parameter should not be None')
        # For data frames with equal columns and order- regular union
    if left_df.schema.names == right_df.schema.names:
        return left_df.union(right_df)
    else:  # Different columns
        # Save dataFrame columns name list as set
        left_df_col_list = set(left_df.schema.names)
        right_df_col_list = set(right_df.schema.names)
        # Diff columns between left_df and right_df
        right_list_miss_cols = list(left_df_col_list - right_df_col_list)
        left_list_miss_cols = list(right_df_col_list - left_df_col_list)
        return __order_and_union_d_fs(left_df, right_df, left_list_miss_cols, right_list_miss_cols)

# COMMAND ----------

def read_json(path):
  
  if isinstance(path,str):
    list_path = [path]
    
  if isinstance(path,list):
    list_path = path  
    
  try:
    write_notebook_source_and_target(list_path,'source')
  except Exception as e:
    lp('read_json: write_notebook_source_and_target: Exception: {}'.format(e))
    
  df = spark.read.json(path)  
  logprint('Read from {}'.format(path)) 
  
  return df



# COMMAND ----------

### COPY-PASTED, NEED TO ABSTRACT THE DUPLICATED LOGIC 
def read_csv(path, header):
  try:
    write_notebook_source_and_target([path],'source')
  except Exception as e:
    lp('read_json: write_notebook_source_and_target: Exception: {}'.format(e))
    
  df = spark.read.csv(path,header=header)  
  logprint('Read from {}'.format(path)) 
  
  return df



# COMMAND ----------

# MAGIC %md
# MAGIC ###### EGRESS

# COMMAND ----------

def df_write_json(df,path,mode,partitionBy='',compression='none'):
  
  write_notebook_source_and_target([path],'target')
  
  df.write.json(path, 
                   mode=mode, 
                   ####partitionBy="train_id", 
                   compression=compression)

  logprint('Wrote to {}'.format(path)) 

DataFrame.write_json=df_write_json  





# COMMAND ----------


def df_get_fields(df , str_column , list_fields , drop = False , show = False):
    """Input : structType with nested fields
     Output: a column per nested field (similar to "explode" but slightly different)
    """

    for field in list_fields:
      df = df.withColumn ( str_column + "_" + field , col(str_column)[field])

    if drop == True:
      df = df.drop(str_column)

    if show == True:   
      display(df) 
    
    return df 

DataFrame.get_fields = df_get_fields  

# COMMAND ----------

### Oppretter kolonnen zx_etl og tilordner navn på kallende notebook.
### eksempel på kall: df.add_zx_etl()
### Laget av Matias, 2020.02.07
def df_add_zx_etl(df):
  df =df.withColumn('zx_etl', lit("Databricks.{}".format( get_notebook_name(depth = 0) ) ) )
  return df 
DataFrame.add_zx_etl = df_add_zx_etl

# COMMAND ----------

def df_rename_agg_column(df,subset="all"):
  
  """ 
  Renames headers like "sum(revenue)" to "sum_revenue"
  
  """
  
  if subset == "all":
    list_cols = df.columns
  else:
    list_cols = str2list(subset)
    ###list_cols = 
    
  for c in list_cols:
    c=str(c)
    for operation in ["sum","count","max","min"]:
      
      ### sum(
      str_operation = operation + "(" 
      
      if c.startswith(str_operation):
        str_new_name2 = c.replace( "(" , "_" ).replace( ")" , "" )  
                
        df=df.withColumnRenamed( c , str_new_name2 )
        lp("Renamed {} ---> {}".format(c,str_new_name2))
  return df       
DataFrame.rename_agg_column = df_rename_agg_column        

# COMMAND ----------

def df_test_duplicates(df,col_input,topic=''):
  
  ### kvalitetsikre for dubletter 
  groupby = Window.partitionBy(col_input).orderBy(col_input)
  df_dublett = df.withColumn('number', F.count(col_input).over(groupby))
  df_dublett = df_dublett.filter('number > 1').select(col(col_input).cast('string'),col('number').cast('string')).distinct()
  df_dublett_liste = df_dublett.filter('number > 1').select(col(col_input).cast('string'),col('number').cast('string')).distinct().collect()

  ### Tester om dataframe er tom
  dublett = 0
  if len(df_dublett.head(1)) != 0:
    dublett = 1
    logprint('Duplicates found in {}'.format(col_input))
    list_duplicates = []
    counter = 0
    for element in df_dublett_liste:
      logprint(element[0])
      list_duplicates.append('{}: {} present {} times'.format(col_input,element[0],element[1]) )
    list_duplicates_formatert = '\n\t'.join(list_duplicates)
    #message = 'dubletter:  {}{}{}Denne eposten er et automatisk varsel'.format('\n\n\t',list_duplicates_formatert,'\n\n')
    message = 'Notification sent: {}: duplicates on {}'.format(get_notebook_name(depth=2),col_input)
    print_meesage_and_send_mail(message)  
 
  
  else:
    lp("Did not find any duplicates in {}".format(col_input) )
    
DataFrame.test_duplicates = df_test_duplicates

# COMMAND ----------

def df_test_columns(df , test = 'collect' ):
  
  """
  Brute-force test to find columns which will cause a java.lang.NullPointerException when collecting the whole array of rows into driver's RAM.
  It might take too long to execute for enormous datasets.
  NOTE: .printSchema() shows "nullable = false" columns, but some of these might not cause an exception when collected to the main driver. 
  Thus, the goal of this test is to find those that actually will cause a failure.
  """
  ###we do not cache because df.cache() hides the column which causes the exception
  for c in df.columns:
    if test == 'collect':
      try: 
          df.select(c).collect()
          lp("{} passed the {} test ✓".format( c , test )) 
      except Exception as e:
          lp("***{} did not pass the {} test ***".format( c , test ))
    
DataFrame.test_columns = df_test_columns

# COMMAND ----------

def df_add_null_column(df,list_cols):
  
  ### if passed string, convert to list
  list_cols = str2list(list_cols)
  
  for c in list_cols:
    df = df.withColumn(c, lit(None).cast( StringType() ) )
  return df 

DataFrame.add_null_column = df_add_null_column

# COMMAND ----------

def df_get_exploded(df,column, drop = False, show = False):
  df = df.withColumn(column + "_exploded" , explode( col(column) ) )
  
  if drop == True:
    df = df.drop(column)
  
  if show == True: 
    display(df) 
  return df 
DataFrame.get_exploded = df_get_exploded




# COMMAND ----------

### needs to be generalized to n columns
def df_array_zip(df,b,c,d):
  
  from pyspark.sql.functions import arrays_zip, col
  
  ###https://stackoverflow.com/questions/41027315/pyspark-split-multiple-array-columns-into-rows
  df = df.withColumn("tmp", arrays_zip(b, c ,d ) ) 
  df = df.withColumn("tmp", explode("tmp") ) 
  df = df.select(col("tmp")[b], col("tmp")[c], col("tmp")[d] ) 
  
  df = df.withColumnRenamed('tmp.{}'.format(d) , d )
  df = df.withColumnRenamed('tmp.{}'.format(b) , b )
  df = df.withColumnRenamed('tmp.{}'.format(c) , c )
  
  return df 
  

# COMMAND ----------

def df_copy(df):
  _schema = copy.deepcopy(df.schema)
  return df.rdd.zipWithIndex().toDF(_schema)
DataFrame.copy = df_copy

# COMMAND ----------

def df_snake_headers(df,subset = 'all'):
  ### CamelCase notasjon ---> snake_case notasjon 
  
  if subset == 'all':
    list_cols = df.columns
  else:
    list_cols = subset
    
  for c in list_cols:
    df=df.withColumnRenamed( c, camel2snake(c) ) 
  return df  

DataFrame.snake_headers = df_snake_headers

# COMMAND ----------

def df_change( df, func , c , target = ""):

  """
  changes (updates) a given column based on passed function. Specify a target column if you do not want the original to be overwritten.
  
  Example without target (overwrites column "A"):  df = df.change(lambda x: None if x in ['.','..'] else x , "A" )
  
  Example with target (creates a new column "B"):  df = df.change(lambda x: None if x in ['.','..'] else x , "A", "B" )
  
  """
  
  if len(target) == 0:
    target = c    
    
  return df.withColumn(  target ,  udf(func) ( df[c] )  )

DataFrame.change = df_change           

# COMMAND ----------

def df_recast(df,cols,data_type):
  
  """ 
  
  df = df.recast ( ['createdAt', 'lastSignInAt' , 'changedAt' , 'emailVerifiedAt', 'telephoneNumberVerifiedAt' ]  , 'timestamp' )

  is a more pythonic (clean/concise) alternative to: 

  df = df.withColumn('createdAt', col('createdAt').cast(TimestampType()))
  df = df.withColumn('lastSignInAt', col('lastSignInAt').cast(TimestampType()))
  df = df.withColumn('changedAt', col('changedAt').cast(TimestampType()))
  df = df.withColumn('emailVerifiedAt', col('emailVerifiedAt').cast(TimestampType()))
  df = df.withColumn('telephoneNumberVerifiedAt', col('telephoneNumberVerifiedAt').cast(TimestampType()))
  """ 
  
  ### Check DATA TYPES: https://spark.apache.org/docs/latest/sql-reference.html
  
  ### Accepted input arguments are defined here 
  list_timestamp  =  ["timestamp", "ts" , "TimestampType"] 
  list_date       =  ["date"]
  list_str        =  ["string" , "str", "StringType"]
  list_double     =  ["double"]
  list_int        =  ["integer", "int" , "IntegerType"]
  list_long       =  ["long","LongType","bigint"]
  
  
  list_all=[]  
  list_all.extend(list_timestamp)
  list_all.extend(list_date)
  list_all.extend(list_double)
  list_all.extend(list_int)
  list_all.extend(list_long)
  
  
  if isinstance(cols,str):
    cols = [cols]
  
  if data_type in list_timestamp:
     data_type = TimestampType()
  
  
  elif data_type in list_date:
     data_type = DateType()   
  
  
  elif data_type in list_str:
     data_type = StringType()
  
  
  elif data_type in list_double:
     data_type = DoubleType()
  
  
  elif data_type in list_int:
     data_type = IntegerType()
      
  
  elif data_type in list_long:
     data_type = LongType()    
  
  
  elif data_type.startswith('decimal'):
     str_decimal_type = data_type.replace("d","D").replace("(" , "Type(")
     data_type = eval(str_decimal_type) 
      
  else: 
    ###raise ValueError ("Invalid data_type. Valid options are: {} ".format(list_all) )
    logprint("Passed data type : {} ".format(data_type) )
  
  
  ### If passed data type was invalid, it will fail here
  for c in cols:
      df = df.withColumn(c, df[c].cast( data_type )  )
      ###print('Casted {} ---> {}'.format(c, data_type))
   
  return df  
 
DataFrame.recast = df_recast 

# COMMAND ----------

def df_add_metadata(df, source_system, timestamp_added = current_timestamp(), timestamp_updated = current_timestamp()):
    """Adds standardized metadata to a dataframe, given the source system

    Args:
        source_system (string): The source system the row originates from. If several system applies, pick a 'main' system.
        timestamp_added (timestamp): The timestamp when the row was added to the data warehouse / data lake.
        timestamp_updated (timestamp): The timestamp when the row was last updated.

    Returns:
        DataFrame: The modified data frame with the new columns. If any of the columns exist, they will be overwritten.
    """
    return df.withColumn('zx_timestamp_added', from_utc_timestamp(timestamp_added, 'Europe/Oslo')) \
             .withColumn('zx_timestamp_updated', from_utc_timestamp(timestamp_updated, 'Europe/Oslo')) \
             .withColumn('zx_source_system', lit(source_system))
DataFrame.add_metadata = df_add_metadata

# COMMAND ----------

def df_sort_columns(df, cols, drop = True):
    """ Sort the columns of df based on the order provided in the argument. System columns (zk and zx) are appended to the end of the data frame.

    This should be ran as the last command before writing to Redshift / S3. It will ensure some consistency in the target table.
    It also gives a warning if there are fields in the data frame which should not be written.

    Args:
        cols (list): A list of column names, sorted in the required order

    Returns:
        DataFrame: The modified data frame with columns in the correct order
    """
    system_cols = [x for x in df.columns if x.startswith(("zx_", "zk_", "zs_"))]
    
    ### if optional parameter "drop" set to False, automatically add all columns which were not included in cols
    if drop == False:
      for c in df.columns:
        if c not in cols and c not in system_cols: 
          cols.append(c)
    
    regular_cols = [x for x in cols if x not in system_cols]
    cols = regular_cols + sorted(system_cols)
    missing_cols = [x for x in df.columns if x not in cols]
    
    
    if len(missing_cols) > 0:
        print( "NOTE! These columns are not mapped, and will be removed in the next steps: {}".format(', '.join(missing_cols)) )
    excessive_cols = [x for x in cols if x not in df.columns]
    if len(excessive_cols) > 0:
        print( "NOTE! These columns are mapped, but does not exist in the data frame: {}.".format(', '.join(excessive_cols)))
    return df.select([x for x in cols if x in df.columns])
DataFrame.sort_columns = df_sort_columns

# COMMAND ----------

### Example of Pandas melt: https://i0.wp.com/cmdlinetips.com/wp-content/uploads/2019/06/Pandas_melt_reshape.png?resize=421%2C236
def melt(df, id_vars, value_vars, var_name='variable', value_name='value'):
    """Convert df from wide to long format.

    Args:
        id_vars (list): A list of columns describing the keys of the dataframe (will not be converted)
        value_vars (list): A list of columns to be converted into long format (one row per column)
        var_name: column name of the new attribute (takes values from column names of value_vars)
        value_name: column name of the new value (taks values from the values of value_vars)

    Returns:
        DataFrame: Modified dataframe converted from wide to long format
    """

    # Create array<struct<variable: str, value: ...>>
    _vars_and_vals = array(*(
        struct(lit(c).alias(var_name), col(c).alias(value_name))
        for c in value_vars))

    # Add to the DataFrame and explode
    _tmp = df.withColumn('_vars_and_vals', explode(_vars_and_vals))

    cols = id_vars + [
            col('_vars_and_vals')[x].alias(x) for x in [var_name, value_name]]
    return _tmp.select(*cols)
DataFrame.melt = melt

# COMMAND ----------

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days) + 1):
        yield start_date + timedelta(n)

# COMMAND ----------

def df_write_to_csv(df, folder, name, delimiter=',', header='true', encoding='UTF-8'):
  csv_location = folder + 'temp.folder'
  file_location = folder + name

  df.repartition(1).write.option('delimiter', delimiter).option('encoding', encoding).csv(path=csv_location, mode='append', header=header)
  
  ### Finner .csv filen i temp mappen, og flytter den til riktig mappe. Sletter unødvendinge filer.
  file = dbutils.fs.ls(csv_location)[-1].path
  dbutils.fs.cp(file, file_location)
  dbutils.fs.rm(csv_location, recurse=True)
  
  try:
    write_notebook_source_and_target( [file_location] , "target")
  except Exception as e:
    lp('df_write_to_csv: write_notebook_source_and_target: exception: {}'.format(e) )
    
DataFrame.write_to_csv = df_write_to_csv

# COMMAND ----------

def df_lookup(df, table, lookup_column_left, lookup_column_right, lookup_column):
    """Looks up against a mapping table (using replace)

    Args:
        table (string): The name of the table to use for the lookup.
        lookup_column_left (string): Name of the lookup column in the left table (original table).
        lookup_column_right (string): Name of the lookup column in the right table (lookup table).
        lookup_column (string): Name of the column in the lookup table to use to replace the original value.
        
    Returns:
        A dataframe with the content of the column mapped to a new value
    """
    
    lookup_table = rs_table(table).select(lookup_column_right, lookup_column).where(col(lookup_column_right) != col(lookup_column))
    d = dict([list(row) for row in lookup_table.collect()])
    return df.replace(d, subset=[lookup_column_left])
DataFrame.lookup = df_lookup

# COMMAND ----------

def df_write_json_common_bucket(df, folder):
  """Writes a df to a json file in a bucket that is accessible by both DEV and PROD environments

  Args:
      df: the data frame to be written to json
      folder: folder name to place the data frame in
  """
  ### Both DBC dev and prod instances has access to this bucket
  bucket = env("S3_COMMON_BUCKET")
  df.write.json("s3a://{}/{}/".format(bucket,folder), mode="overwrite")
DataFrame.write_json_common_bucket = df_write_json_common_bucket

# COMMAND ----------

def read_json_common_bucket(folder):
  """Read a df from a json file in a bucket that is accessible by both DEV and PROD environments

  Args:
      df: the data frame to be read
      folder: folder name the data frame is located in
  """
  ### Both DBC dev and prod instances has access to this bucket
  bucket = env('S3_COMMON_BUCKET')
  df = spark.read.json("s3a://{}/{}/*".format(bucket, folder))
  return df

# COMMAND ----------

def df_set_nullable(df, column_list, nullable=True):
  """ Changed the schema of the dataframe.
      Sets the colums specified to nullable=True
      
  Args:
      column_list: A list with column names in the df
      nullable: default is True, can be set to False if columns should be set to "not null"
  """
  for struct_field in df.schema:
    if struct_field.name in column_list:
      struct_field.nullable = nullable
  df = spark.createDataFrame(df.rdd, df.schema)
  return df
DataFrame.set_nullable = df_set_nullable

# COMMAND ----------

### Kan brukes istedet for with_column i tilfeller hvor kolonnene ikke alltid har innhold. Setter innholdet i kolonnen til Null istedet for å fjerne hele raden.
def df_add_column(df, column_name, column, type = 'string'):
      
  try:
    df = df.withColumn(column_name, column)
  except:
    if type == 'int':
      df = df.withColumn(column_name, lit(None).cast(IntegerType()))

    elif type == 'long':
      df = df.withColumn(column_name, lit(None).cast(LongType()))
    
    elif type == 'double':
      df = df.withColumn(column_name, lit(None).cast(DoubleType()))

    elif type == 'string':
      df = df.withColumn(column_name, lit(None).cast(StringType()))
      
    elif type == 'timestamp':
      df = df.withColumn(column_name, lit(None).cast(TimestampType()))

    else:
      logprint('Type not recognized. Defaulting to string')
      df = df.withColumn(column_name, lit(None).cast(StringType()))
      
    logprint('Couldnt find data for column {}. Null value added'.format(column_name))
  return df
DataFrame.add_column = df_add_column

# COMMAND ----------

#def df_format_string_and_cast_to_timestamp(df, list_cols: list):
def df_format_string_and_cast_to_timestamp(df, list_cols):
  for c in list_cols:
      df = df.withColumn(c, F.from_unixtime(df[c]).cast('timestamp'))
      
  return df 

DataFrame.format_string_and_cast_to_timestamp = df_format_string_and_cast_to_timestamp
