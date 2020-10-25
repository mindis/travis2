# Databricks notebook source
def get_sum_of_null_values_across_cols(df, list_cols_null_not_acceptable):
  import numpy as np
  list_results=[]
  for cols in list_cols_null_not_acceptable:
    #condition = col(f'{cols}').isNull()
    condition = col('{}'.format(cols)).isNull()
    print('Filtering df on condition: {}'.format(condition))
    df_cols_null_not_acceptable = df.filter(condition)   
    list_results.append(df_cols_null_not_acceptable.count())
    
  #For debugging:
    #lp(f'Test condition: col({cols}).isNull()')
    #lp(df_cols_null_not_acceptable.count())
  
  return np.array(list_results).sum()  
    

# COMMAND ----------

def send_mail_if_false(condition, message='',scope = globals()):
  
   if eval(condition,scope):
      lp("Passed the test ✔")       
               
   else:
      
      ### TODO: implement get_notebook_name(depth=-1) which returns the maximum depth possible
      #message = 'Notification sent: {}: did not pass one of the tests'.format(get_notebook_name(depth=2))
      
      if message == '':
         message = 'Notification sent: {}: did not pass one of the tests'.format(get_notebook_name(depth=2))
      
      print_meesage_and_send_mail(message)             
      
      
def print_meesage_and_send_mail(message):
       lp(message)
       if STR_ENVIRONMENT == 'PROD':
            send_mail(sns_topic='SNS_TOPIC_FORVALTNING', message = message, subject = 'Data quality issues in PROD', environment = env("ENVIRONMENT")) 
            

            


# COMMAND ----------

def get_difference(x1,x2):

  import numpy as np
  difference = x2-x1
  absolute_difference = np.abs(difference)
  
  NUMBER_OF_DECIMALS = 4
  LARGE_CONSTANT_REPRESENTING_INFINITE = 999
  
  if x1 == 0:
    relative_difference = LARGE_CONSTANT_REPRESENTING_INFINITE   
  else:   
    relative_difference = np.round( difference/x1 , NUMBER_OF_DECIMALS)

  lp('difference (x2-x1): ',newline=False)
  print_number(difference)
  
  lp('absolute_difference: ',newline=False)
  print_number(absolute_difference)

  lp('relative_difference: ',newline=False)
  print(relative_difference)
 
  return difference, absolute_difference, relative_difference 


# COMMAND ----------

def get_is_table_updated(df):
  
  ts_last_update = df.select(col('zx_timestamp_updated')).collect()[0][0]
  
  ts_today = pd.Timestamp(datetime.now().date())
  #lp(f'ts_last_update: {ts_last_update}')
  #lp(f'ts_today: {ts_today}')
  
  lp('ts_last_update: {}'.format(ts_last_update))
  lp('ts_today: {}'.format(ts_today))
  
  return  ts_last_update > ts_today
