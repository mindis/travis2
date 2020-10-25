# Databricks notebook source
def fetch_results_from(url): 
  ### REPEATED LOGIC. ENCAPSULATE. 
  lp('read from: {}'.format(url))
  dp_i =pd.read_csv(url)

  n_records_fetched = dp_i['n_records_fetched'].iloc[0]
  lp('n_records_fetched: {}'.format(n_records_fetched))

  return n_records_fetched



# COMMAND ----------

def get_run_id():
  ### If running in interactive mode, there is no "runId". We fill with -1 
  dp_context_tags = get_context_tags()
  try:
    run_id = dp_context_tags[dp_context_tags['key']=='runId']['value'].iloc[0]
  except IndexError as e:
    lp(e)
    run_id = '-1'
    print('Running in interactive mode. run_id set to -1')  
  #lp(f'run_id: {run_id}') 
  lp('run_id: {}'.format(run_id)) 
  return run_id

# COMMAND ----------

def get_parent_folder(depth=2):
    list_path = get_notebook_name(depth=depth).split('/')
    lp('list_path: {}'.format(list_path))
    PARENT_FOLDER = '{}/{}'.format(list_path[0],list_path[1])
    lp('PARENT_FOLDER: {}'.format(PARENT_FOLDER))
  
    return PARENT_FOLDER

# COMMAND ----------

### https://medium.com/better-programming/how-to-flatten-a-dictionary-with-nested-lists-and-dictionaries-in-python-524fd236365

### EXAMPLE
# data = {
#     "id": 1,
#     "first_name": "Jonathan",
#     "last_name": "Hsu",
#     "employment_history": [
#         {
#             "company": "Black Belt Academy",
#             "title": "Instructor",
#             "something": {
#                 "hello": [1,2,3,{
#                     "something":"goes"
#                 }]
#             }
#         },
#         {
#             "company": "Zerion Software",
#             "title": "Solutions Engineer"
#         }
#     ],
#     "education": {
#         "bachelors": "Information Technology",
#         "masters": "Applied Information Technology",
#         "phd": "Higher Education"
#     }
# }

### OUTPUT
# OrderedDict([ ('id', 1),
#               ('first_name', 'Jonathan'),
#               ('last_name', 'Hsu'),
#               ('employment_history_0_company', 'Black Belt Academy'),
#               ('employment_history_0_title', 'Instructor'),
#               ('employment_history_0_something_hello_0', 1),
#               ('employment_history_0_something_hello_1', 2),
#               ('employment_history_0_something_hello_2', 3),
#               ('employment_history_0_something_hello_3_something', 'goes'),
#               ('employment_history_1_company', 'Zerion Software'),
#               ('employment_history_1_title', 'Solutions Engineer'),
#               ('education_bachelors', 'Information Technology'),
#               ('education_masters', 'Applied Information Technology'),
# ('education_phd', 'Higher Education')])

def normalize_dict(d,sep="_"):
    import collections

    obj = collections.OrderedDict()

    def recurse(t,parent_key=""):
        
        if isinstance(t,list):
            for i in range(len(t)):
                recurse(t[i],parent_key + sep + str(i) if parent_key else str(i))
        elif isinstance(t,dict):
            for k,v in t.items():
                recurse(v,parent_key + sep + k if parent_key else k)
        else:
            obj[parent_key] = t

    recurse(d)

    return obj

# COMMAND ----------

def miliseconds_to_datetime(x):
  from datetime import datetime as Datetime
  return Datetime.fromtimestamp(x/1000.0).strftime('%Y-%m-%d %H:%M:%S')
  

# COMMAND ----------

### SHOULD BE GENERALIZED TO ANY WEEKDAY 
def get_date_last_saturday():
  
  ### import with conventional class notation
  from datetime import timedelta as Timedelta
  from datetime import datetime  as Datetime
  from dateutil import relativedelta
  
  today = Datetime.now()
  
  if today.weekday() == 5:
     weeks_back_in_time = 0 ### If today is saturday. we want "last saturday" to adtop today's value
     
  else:
     weeks_back_in_time = -1

  print('weeks_back_in_time: {}'.format(weeks_back_in_time))
  start = today - Timedelta((today.weekday() + 1) % 7)
   
  dtm_last_saturday = start + relativedelta.relativedelta(weekday=relativedelta.SA(weeks_back_in_time))
  dt_last_saturday = dtm_last_saturday.date()
  str_last_saturday = dt_last_saturday.strftime(format="%Y-%m-%d")
  print('Last Saturday was {}'.format(str_last_saturday))
  
  return str_last_saturday, dt_last_saturday

# COMMAND ----------

    
### Write graphviz object to s3 
def write_graphviz_to_s3(dp,table_type,name=''):
      if name=='':
         name=get_notebook_name()
        
      lp("Starting to write csv for graphviz...")    

      import boto3
      s3_resource = boto3.resource("s3")
      str_bucket = 'kundemaster-{}'.format(str_environment_lower_case)
      
      ### table_type = deptable or abs_sources_and_targets
      str_key = 'graphviz/{}_{}.csv'.format(table_type,name)
      s3_object = s3_resource.Object(bucket_name = str_bucket , key = str_key )   
      s3_object.put( Body = dp.to_csv()  , ACL = 'public-read' )
      lp("Dependency table successfully written to:")
      lp( "S3:/{}/{}".format(str_bucket,str_key) ) 

# COMMAND ----------

### Skrive en json fil til S3 og hente den tilbake som en df
### THIS METHOD NEEDS TO BE GENERALIZED TO BE USED IN OTHER PLACES
def build_df_from_json_lines(list_data,subject):
    ###if subject == 'consent':
      ###lp("build_df_from_json_lines: input list_data: {}".format(list_data))
    filename_root = 'json_sqs_to_df'
    ###jslines = '\n'.join([json.dumps(json.loads(msg)) for msg in list_data])
    list_json=[]
        
    len_list_data=len(list_data)
    
    for msg in list_data:
      dict_msg = json.loads(msg)      
      list_json.append( json.dumps( dict_msg ) )
      
    jslines = '\n'.join(list_json)
    
    
    filename = '/tmp/{}_temp_{:%Y%m%d%H%M%S}_r{}_{}'.format(filename_root, datetime.now(), random.randint(1,9999), subject)
    try:      
      ### fs.put takes 2-3 seconds
      dbutils.fs.put(filename, jslines)
      ###logprint('Wrote to: {}'.format(filename))
    except Exception as e:
      lp("build_df_from_json_lines: exception: {}".format(e))

    df = spark.read.json(filename)
    df = df.withColumn( 'subject', lit(subject) ) 
        
    return df,msg,jslines,dict_msg

# COMMAND ----------

def query_databricks(endpoint,request_type,json={}):
    import base64
    url_endpoint = '{}api/{}'.format(env('DATABRICKS_URL') , endpoint)
    token = env('DBC_TOKEN')
    headers={"Authorization": "Basic " + base64.standard_b64encode(("token:" + token).encode("utf-8")).decode("utf-8")}
    
    ### TODO: change for more generic requests.request('get', etc) 
    if  request_type == "get":
      response = requests.get(url_endpoint, 
                              headers=headers,
                              json = json)
    if  request_type == "post":
      response = requests.post(url_endpoint, 
                              headers=headers,
                              json = json)
      
 
    
    return response 

# COMMAND ----------

def databricks_response_to_df(endpoint,response):
    #response = requests.get(url_endpoint, headers={"Authorization": "Basic " + base64.standard_b64encode(("token:" + token).encode("utf-8")).decode("utf-8")})
    
    ### Skriver om resultatet til df
    filename = "/tmp/{}_{}.json".format(endpoint, time.time() )
    dbutils.fs.put(filename, response.text, True)
    df = spark.read.json(filename)
    
    display(df)
    
    return df 

# COMMAND ----------

def print_number(number): 
    return print("{:,}".format(number)) 

# COMMAND ----------

def print_globals_prefixed(prefix):
  dict_space = globals()
  for k,v in dict_space.items():
    if k.startswith(prefix):
      logging.info('{}: {} : {}'.format(prefix,k,v))
  
  

# COMMAND ----------

def snake2camel(name):
    return re.sub(r'(?:^|_)([a-z])', lambda x: x.group(1).upper(), name)

# COMMAND ----------

def snake2camelback(name):
    return re.sub(r'_([a-z])', lambda x: x.group(1).upper(), name)

# COMMAND ----------

def camel2snake(name):
    return name[0].lower() + re.sub(r'(?!^)[A-Z]', lambda x: '_' + x.group(0).lower(), name[1:])

# COMMAND ----------

def camelback2snake(name):
    return re.sub(r'[A-Z]', lambda x: '_' + x.group(0).lower(), name)

# COMMAND ----------

def pv(x):
  list_var_name = [name for name in  globals() if  globals()[name] is x]
  if len(list_var_name) > 0:
    var_name = list_var_name[0]
    
    if type(x) == str:
      ### string
      print(var_name + "  = '{}'".format(x) )
    else:
      ### andre
      print(var_name + " = {}".format(x) )
  else:
    print('cannot print the variable name, but its value is: {}'.format(x) )

# COMMAND ----------

### Lager en liste med første dato i hver måned mellom start- og sluttdato.
from datetime import date
from dateutil.relativedelta import relativedelta

def mthStList(start_date, end_date):
    stdt_list = []
    cur_date = start_date.replace(day=1) # sets date range to start of month
    while cur_date <= end_date:
        stdt_list.append(date(cur_date.year, cur_date.month, cur_date.day))
        cur_date += relativedelta(months=+1)
    return stdt_list

# COMMAND ----------

### reorder columns for a Pandas DataFrame
def reorder_columns(dp , cols ):
    for c in dp.columns:
        if c not in cols:
            cols.append(c)
    dp = dp[cols]               
    return dp

# COMMAND ----------

### Returns the global variable name as a string
def get_var_name(var):
  list_var_name = [name for name in  globals() if  globals()[name] is var]
  
  if len(list_var_name) > 0:
    var_name = list_var_name[0]
    
  return var_name

# COMMAND ----------

### Validity test: arg_init
### get_parameter() returns strings, therefore we do not check for boolean type, but the strings '1' or '0'
def test_arg_init_value():
  arg_list = ['0','1']
  
  if arg_init not in arg_list:
    raise ValueError('arg_int: Wrong input. Accepted inputs are {}'.format(arg_list))
  else:
    lp('Valid input: arg_init') 

# COMMAND ----------

### Validity test: integer arguments
def test_is_castable_to_int(arg):
  arg_str_name = get_var_name(arg)
  
  try: 
    int(arg)
    lp('Valid input: {}'.format(arg_str_name))
  except:
    raise ValueError('{}: Wrong input. Accepted inputs are integers'.format(arg_str_name))

# COMMAND ----------

### Validity test: date arguments
def test_is_castable_to_date(arg):
  arg_str_name = get_var_name(arg)
  
  try: 
    datetime.strptime(arg, '%Y-%M-%d').date()
    lp('Valid input: {}'.format(arg_str_name))
  except:
    raise ValueError('{}: Wrong input. Accepted inputs are dates'.format(arg_str_name))

# COMMAND ----------

### Prefix all columns in a df
def prefix_df_cols(df, str_prefix):
  select_list = [col(col_name).alias('{}_'.format(str_prefix) + col_name)  for col_name in df.columns]
  df = df.select(*select_list)
  
  return df
