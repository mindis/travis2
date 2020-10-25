# Databricks notebook source
# MAGIC %md
# MAGIC https://jico.nsb.no/confluence/display/NSBFTITSI/DBFS

# COMMAND ----------

def delta_exists(table_name):
  sqlquery = f"DESCRIBE delta.`dbfs:/mnt/{KEY_FIRST_LEVEL}/{table_name}/`"
  try:
    spark.sql(sqlquery)
    return True
  except Exception as e:
    if "doesn't exist" in str(e):
       print(e)
       return False 
        

# COMMAND ----------

def _get_mnt_key(BUCKET, KEY):
    return f"mnt/{BUCKET}/{KEY}/"

### RENAMED: mount_key ---> mount_to_dbfs_if_not_exists
def mount_to_dbfs(BUCKET, KEY):
  list_mount_points = [mount.mountPoint for mount in dbutils.fs.mounts()]
  mnt_key_with_slash = f'/{_get_mnt_key(BUCKET,KEY)}'  

  if mnt_key_with_slash in list_mount_points:
     print(f'{mnt_key_with_slash} already mounted.')
  else:
     lp(f'didnt find {mnt_key_with_slash}. Will mount...', newline=False)
     lp(f'mnt_key_with_slash: {mnt_key_with_slash}')
     dbutils.fs.mount(f"s3a://{BUCKET}/{KEY}/", mnt_key_with_slash)
     print('mount completed.')
      
  return mnt_key_with_slash


def read_from_delta_lake(BUCKET, KEY):
    url_s3 = f'dbfs:/{_get_mnt_key(BUCKET,KEY)}'
    return spark.read.format('delta').load(url_s3)  
  
  
def df_write_to_delta_lake(df, BUCKET, KEY, mode, option):
   s3_location = mount_to_dbfs(BUCKET, KEY)
   lp(f'writing to {s3_location} ...', newline=False)
   df.write.option(option, "true").save(s3_location, mode = mode, format='delta')
   print(f'completed.')

DataFrame.write_to_delta_lake = df_write_to_delta_lake  


def df_upsert_to_delta_lake(df,target, upsert_key):
  df.createOrReplaceTempView("updates")

  delta_table = f"delta.`dbfs:/mnt/{target}`"


  spark.sql(f"""MERGE INTO {delta_table} t
      USING updates s
      ON s.{upsert_key} = t.{upsert_key}
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *""") 
    
DataFrame.upsert_to_delta_lake = df_upsert_to_delta_lake
