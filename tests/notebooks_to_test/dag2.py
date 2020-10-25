# Databricks notebook source
# MAGIC %md
# MAGIC DIFF WITH DAG (V1):
# MAGIC 
# MAGIC 1. Init: no "name" argument --> name = get_notebook_name()
# MAGIC 
# MAGIC 2. send_to_slack AND send_to_sns are now mandatory parameters
# MAGIC   
# MAGIC 3. arg "exception" renamed to "raise_exception"
# MAGIC 
# MAGIC 4. "raise_exception" defaults to True, which is more natural/expected when a task fails
# MAGIC 
# MAGIC 5. topics defaulterer to topics=[env('SNS_TOPIC_FORVALTNING')] (instead of an empy list)
# MAGIC 
# MAGIC 
# MAGIC DISCUSS THE FOLOWING:
# MAGIC 
# MAGIC 1. send_to_slack and sns are not input, but hardcoded to True if environment = PROD (False otherwise, or send to a separate slack channel) 
# MAGIC 
# MAGIC 2. Make "raise_exception" mandatory parameter (no default value)

# COMMAND ----------

# MAGIC %md 
# MAGIC Old note, no longer applies since "name" is assigned automatically:
# MAGIC "En forutsetning for å få treff er at DAG notebooken benytter notebookens navn på DAG-tasken. F.eks. bør ../dag/crm_live.py notebooken definere DAG tasken slik: d = DAG('crm_live', send_to_slack=False).""

# COMMAND ----------

FAILURE='failure'
SUCCESS='success'
PENDING='pending'
SKIPPED='skipped'

### En DAG er en serie av oppgaver som skal utføres i sekvens, med avhengigheter.
### Denne klassen gjør det mulig å spesifisere disse tasks, og eksekvere de.

class DAG2:
  
  ### Initierer en ny DAG. Dette gjør egentlig ingenting annet enn potensielt å sende en Slack-melding, samt nullstille.
  def __init__(self, arguments={}, send_to_slack=True, display_args='df'):
    
    
    name = get_notebook_name()
    logprint("DAG name: {}".format(name))
    
    self.name = name
    self.arguments = arguments
    self.tasks = {}
    self.timer = Timer()
    self.display_args=display_args
    
    ###clean_data_lineage_files(self.name.split("/")[-1])
      
    import pandas as pd 
    self.dp_all_entries = pd.DataFrame({})
    
    
    ### Sender Slack-melding om oppstart (med mindre man ikke ønsker det)
    if send_to_slack: send_slack(
      attachments=[dict(
        title="DAG {} started".format(self.name), 
        text=":crossed_fingers: Fingers crossed!",
        ### Et felt per DAG-argument
        fields=[dict(title="Argument: {}".format(k),value=v) for k,v in iteritems(arguments)],
        title_link="{}#joblist".format(env('DATABRICKS_URL')),
        footer="Databricks DAG [{}]".format(env('ENVIRONMENT')),
        ts=datetime.now().strftime("%s"),
      )])
    
  ### Overloader <<-operatoren til å brukes til å legge inn og kjøre oppgaver
  ### * Først sjekkes eventuelle dependencies til tasken.
  ### * Hvis disse er feilet eller skippet, skippes tasken.
  ### * Hvis ikke kjøres selve tasken (notebooken / testen / etc) og resultatet registreres (SUCCESS eller FAILURE).
  
  def write_docs(self):

    import datetime
    self.dp_all_entries['source'] = ''
    self.dp_all_entries['target'] = ''
    
    
    dict_data={}
    for c in list_cols_docs_job:
      dict_data[c] = []
    
   
    
    if len(self.dp_all_entries)==0:
      raise ValueError("Table 'dp_all_entries' is EMPTY, you need to complete at least one task before calling write_docs()")
    
    
    dp_merged = pd.DataFrame( columns = list_cols_task_level )
    self.dp_all_entries = self.dp_all_entries.set_index('task')    
    for task in self.tasks:
      
      ### ex: task: load_abt/load_abt_akp_aggr_order_customer_foundation
      ###lp("task: {}".format(task) )
      
      ### ex: task_short: load_abt_akp_aggr_order_customer_foundation
      task_short = task.split("/")[-1]
      ###lp("task_short: {}".format(task_short) )
      

      STR_PATH_CSV =  "{}/{}.csv".format(STR_FOLDER_DATA_LINEAGE, task_short )      
      ###lp("reading from STR_PATH_CSV: {}".format(STR_PATH_CSV) )
      dp_key_task = pd.read_csv(STR_PATH_CSV)
      ###lp("read dp_key_task from STR_PATH_CSV: {}".format(STR_PATH_CSV) )
      
      
      
      ### dp_key_task: 
      
      ###    task_timestamp	            TASK	   source	                           target
      ###0	2020-03-30 19:32:18.032263	test	stage.abt_akp_train_source	            NaN
      ###1	2020-03-30 19:32:18.032263	test	stage.abt_akp_train_source_with_flag	NaN
      ###2	2020-03-30 19:33:15.441110	test	stage.abt_akp_stations_and_products	    NaN
      ###3	2020-03-30 19:34:21.980510	test	NaN	                              public.table_test
    
    
      dp_left = dp_key_task[['task',"task_timestamp","source"]].dropna()
      dp_right = dp_key_task[['task',"target"]].dropna()
      
      
      ### dp_merged_i: 
      ###0	test	stage.abt_akp_train_source	public.table_test
      ###1	test	stage.abt_akp_train_source_with_flag	public.table_test
      ###2	test	stage.abt_akp_stations_and_products
      
      dp_merged_i=dp_left.merge(dp_right ,how="inner",on='task')
      
      
      ### builds up dp: 
      
      dp_merged=dp_merged.append(dp_merged_i,sort=True) 
      
      
      
      for str_type in ["source","target"]:
        
        STR_PATH = "{}/{}_{}.txt".format(STR_FOLDER_DATA_LINEAGE,str_type,task_short)
        ###lp('STR_PATH: {}'.format(STR_PATH) )
        
        import os.path
        if os.path.isfile(STR_PATH): 
            with open(STR_PATH, "r") as f_read:
                   list_tables=f_read.readlines()                 
                   ###lp("{}: list_tables: {}".format(str_type,list_tables) )
                  
                   ###STR_PATH: /local_disk0/tmp/source_stage_active_fsales_customers_foundation_last_15_months.txt
                   ###[2020-03-29 03:17:13] source: list_tables: ['public.d_stop_place, public.f_sales, public.d_product, ']
                   ###[2020-03-29 03:17:13] source: list_tables_cleaned: ['public.d_stop_place, public.f_sales, public.d_product, ']
                   ###[2020-03-29 03:17:13] source: list_cleaned_no_spaces: ['public.d_stop_place, public.f_sales, public.d_product, ']
                   ###[2020-03-29 03:17:13] source: list_cleaned_no_spaces2: ['public.d_stop_place, public.f_sales, public.d_product,']
                   ###[2020-03-29 03:17:13] source: str_final: 'public.d_stop_place, public.f_sales, public.d_product,'
                   ###[2020-03-29 03:17:13] source: str_final2: public.d_stop_place, public.f_sales, public.d_product,
                   ###[2020-03-29 03:17:13] source: str_final3: public.d_stop_place, public.f_sales, public.d_product
                   
                   ###target: list_tables: ['stage.active_fsales_customers_foundation_last_15_months, ']
                   ###[2020-03-29 03:17:13] target: list_tables_cleaned: ['stage.active_fsales_customers_foundation_last_15_months, ']
                   ###[2020-03-29 03:17:13] target: list_cleaned_no_spaces: ['stage.active_fsales_customers_foundation_last_15_months, ']
                   ###[2020-03-29 03:17:13] target: list_cleaned_no_spaces2: ['stage.active_fsales_customers_foundation_last_15_months,']
                   ###[2020-03-29 03:17:13] target: str_final: 'stage.active_fsales_customers_foundation_last_15_months,'
                   ###[2020-03-29 03:17:13] target: str_final2: stage.active_fsales_customers_foundation_last_15_months,
                   ###[2020-03-29 03:17:13] target: str_final3: stage.active_fsales_customers_foundation_last_15_months
    
                   import re
                   regex= r'[\w.]+(?=@)'
                   list_tables_cleaned = [ re.sub(regex, '', element) for element in list_tables]
                   ###lp("{}: list_tables_cleaned: {}".format(str_type,list_tables_cleaned) )
                   
                   ### remove dummy entries  like the second one in  ['public.f_sales\n', ', '] 
                   list_cleaned_no_spaces=[x for x in list_tables_cleaned if len(x)>5]  
                   ###lp("{}: list_cleaned_no_spaces: {}".format(str_type,list_cleaned_no_spaces) ) 
                    
                   ###list_cleaned_no_spaces2=[x.replace(", " , "") for x in list_cleaned_no_spaces]
                   list_cleaned_no_spaces2=[x.strip() for x in list_cleaned_no_spaces]
                   ###lp("{}: list_cleaned_no_spaces2: {}".format(str_type,list_cleaned_no_spaces2) )  
                   
                   
                   str_final = str(list_cleaned_no_spaces2).replace("[" , "").replace("]" , "")
                   ###lp("{}: str_final: {}".format(str_type,str_final) )
                  
                   str_final2=str_final.replace("'" , "")
                   ###lp("{}: str_final2: {}".format(str_type,str_final2) )
                  
                   str_final3=str_final2[:-1]
                   ###lp("{}: str_final3: {}".format(str_type,str_final3) )
                   
                  
                   ###self.dp_all_entries.loc[task,str_type]= str(list_tables)
                    
                   ### this table has only one task per row, whith many sources and target in the same row 
                   self.dp_all_entries.loc[task,str_type]= str_final 
                                            
                                     
                      
            ### removes txt files like 
            ### /local_disk0/tmp/source_f_sales_normalisation.txt
            ### /local_disk0/tmp/target_f_sales_normalisation.txt        
            os.remove(STR_PATH)
            ###lp("{} deleted ".format(STR_PATH))
            

    
    lp("Starting to write util.docs_job_ table...")
    self.dp_all_entries = self.dp_all_entries.reset_index(drop=False)
    df_redshift = spark.createDataFrame(self.dp_all_entries)
    
    
    ### Gives: raw_field_value  10959-05-07 20:31:00.440  
    ###df_redshift=df_redshift.recast("task_timestamp","ts")
    ###df_redshift = df_redshift.change(lambda x: miliseconds_to_datetime(x), "task_timestamp")
    
    df_redshift=df_redshift.sort_columns(list_cols_docs_job,drop=False)
    
    df_redshift=df_redshift.orderBy( col("task_timestamp").asc() )   
    df_redshift.write_to_redshift('util.docs_job_{}'.format(self.name), mode = 'overwrite',list_long_strings =list_cols_docs_job )
    df_redshift.show(20, True)
    

    
    
    lp("Starting to write dp_merged to S3: (full dependency tables)...")
    dp_merged=dp_merged.reset_index(drop=True)        
    ###print(dp_merged.head(3))
    write_graphviz_to_s3(dp_merged,'deptable')
    print("")
    
    lp("Starting to write docs_job_dependencies_ to Redshift: (full dependency tables)...")
    df_redshift_dp_merged= spark.createDataFrame(dp_merged)
    df_redshift_dp_merged.write_to_redshift('util.docs_job_dependencies_{}'.format(self.name), mode = 'overwrite',list_long_strings = list_cols_task_level )
    
   
    
  

    
    

    
  def _build_dp_docs_job(self,task,target_table,text):
            
    dict_args =task.arguments
    
    import pandas as pd
    
    series = pd.Series()
    
    #### DAG attributes. Here "self" references the dag object
    series['JOB'] = self.name
    series['JOB_ARGS'] = str(dict_passed_args)
         
    ### TASK attributes
    series['task'] = task.name
    series['TASK_ARGS'] = str(dict_args)
    series['DESCRIPTION'] = text
    
    import datetime
    ###series['task_timestamp'] = pd.datetime.now()
    
    series['task_timestamp'] = datetime.datetime.today().replace(microsecond=0) # datetime.datetime(2020, 4, 12, 14, 46, 41)
    
    dp =  pd.DataFrame(series).T
    self.dp_all_entries = self.dp_all_entries.append(dp,sort=True) 

    
    if len(dp) == 0:
      lp("len (dp) is zero. Should not print on the screen")
      
    ###lp("_build_dp_docs_job: FINISHED")  
    
    
  def _display_task_with_details(self,task):
    import pandas as pd
    import copy
    dict_args =task.arguments
    
    ### grabs only the notebook name
    str_notebook = str(task).split(":")[-1].split('/')[-1]
    
    n=len(dict_args.keys())
    
    ### if no args where passed (n=0), initialize a series of size 1 
    if n == 0:
      n = 1
    series = pd.Series( range(n) )
    dp = pd.DataFrame(series)
    dp.columns = ["dummy"]
    
    dp.loc[:,'TIMESTAMP'] = ''
    
    dp.loc[0,'TIMESTAMP'] = utc_to_oslo( datetime.now() ) .strftime("%Y-%m-%d %H:%M:%S")
    
    dp.loc[:,'NOTEBOOK'] = ''
    dp.loc[0,'NOTEBOOK'] = str_notebook
    

    if len(dict_args) > 0:    
      dp["ARGUMENT"] = dict_args.keys()
      dp["VALUE"] = dict_args.values()
    dp = dp.drop(["dummy"] , axis = 1)

    with pd.option_context('display.max_rows', 100, 'display.max_columns', 100, 'display.max_colwidth', -1 , 'display.expand_frame_repr', False , 'display.colheader_justify' , 'left'):
          print(dp.to_string(index=False , header = False) , end='', sep='   ')

    
  def __lshift__(self, task):
    
    ### Legger inn Tasken i listen over Tasks
    self.tasks[task.name] = task
        
    ### Legger sammen argumentene fra DAGen og fra Tasken
    if self.arguments: task.arguments.update(self.arguments)
    
    
    str_task = str(task)
    str_task_arguments = str(task.arguments)
    if len(task.arguments) > 0:
      str_task_with_arguments =  '{:30s}  {}'.format( str_task , str_task_arguments )
    else: 
      str_task_with_arguments =  '{:30s}'.format( str_task )
    
    
    ### 1. ### displays called task with args
    try:
      
      if self.display_args == 'df':  
         self._display_task_with_details( task )        
      
      elif self.display_args == 'dict':     
         logprint('Running ' + str_task_with_arguments,  newline=False)        
      
      else:
        logprint('Running ' + str_task,  newline=False)
        
    except Exception as e:              
        print("__lshift__: _display_task_with_details: Exception: {}".format(e) )        
        ### If exception, fall back to printing only the runing task
        logprint('Running ' + str_task,  newline=False)
        
    ### 2. ### populates dp_all_entries, which is used in write_docs()
    try:      
      self._build_dp_docs_job(task,task.target_table,task.text)
    except Exception as e:
      print("__lshift__: _build_dp_docs_job: Exception: {}".format(e) )
      
      
    ### Sjekker om noen dependencies ikke finnes i listen over tidligere tasks
    undefined_dependencies = [d for d in task.dependencies if not d in self.tasks]
    ### I så fall skippes tasken
    if undefined_dependencies:
      print('SKIPPED because of undefined dependencies: {}'.format('‚'.join(undefined_dependencies)))
      task.status = SKIPPED
      return
      
    ### Sjekker om noen av avhengighetene til Tasken har feilet eller blitt skippet.
    failed_dependencies = [d for d in task.dependencies if self.tasks[d].status != SUCCESS]
    ### I så fall skippes tasken
    if failed_dependencies:
      print('SKIPPED due to dependencies: {}'.format('‚'.join(failed_dependencies)))
      task.status = SKIPPED
      return

    ### Hvis ikke kjøres selve tasken.
    ### Logikken for hvordan de forskjellige task-typene faktisk executes ligger i deres egne klasser.
    t = Timer()
    
    ### This line calls _execute, which calls dbutils.notebook.run
    ### result is a copy of task.status, do we need this copy? 
    result = task.run()
    
    time_spent = t.stop()
    
    ### print task.status 
    if result == SUCCESS:
      print(" SUCCESS ✓  Time spent: {}".format(time_spent))
      
    else:
      print("***FAILED*** Time spent: {}".format(time_spent))
      print("ERROR: {}".format(task.error))
      
      
      
    ### After printing task.status to screen, print job_id
    try:
      with open(STR_DESTINATION_JOB_ID, "r") as f_read:
               
              list_lines=f_read.readlines() 
              str_0 = list_lines[0]
              task.job_id = str_0
              if task.status == FAILURE:                                              
                ### prints something like "https://dbc-f0d109a3-dea7.cloud.databricks.com/#job/262049/run/1"
                ### This attribute should be renamed to task.link_job
                print(task.job_id)
                              
              
              
              
    except Exception as e:
              ###lp("class Task: run: {}".format(e) )
              ### in INTERACTIVE MODE: "class Task: run: name 'STR_DESTINATION_JOB_ID' is not defined". Needs to be fixed
              print("")
              ###logprint("Running in interactive mode")  

  
  def task_error(self):      return [t.error for t in itervalues(self.tasks) if t.status == FAILURE]
  def failed_tasks(self):    return [t for t in itervalues(self.tasks) if t.status == FAILURE]
  def skipped_tasks(self):   return [t for t in itervalues(self.tasks) if t.status == SKIPPED]
  def completed_tasks(self): return [t for t in itervalues(self.tasks) if t.status not in [SKIPPED,FAILURE]]
  
  def display_list_task_with_status(self,status): 
    if status =='failed':
      list_failed_task = self.failed_tasks()
      if len(list_failed_task ) >0: 
        logprint("**********************FAILED TASKS ARE SHOWN BELOW***********************************''")
        logprint("NOTE: if f.e. the link points to JOB ID: 4 and this has status 'Succeeded', check JOB 5 (chances are it failed to initialize or it's a Qlik or DBfit task")
        for task in list_failed_task:
          self._display_task_with_details(task)
        
          try:
            print(" task.job_id: {}".format(task.job_id) )
          except Exception as e:
            print(" no job_id")
            print(e)
        logprint("***********************************************************************************''")
    else:
          print("NO FAILED TASKS ✓")  
    
  
  ### summarize_performance(): improvements for a future relase: 
  ### 1. spit into several methods, where each method does only one thing 
  def summarize_performance(self,time_spent):
    import pandas as pd
    import copy
    import numpy as np
    filename = "/tmp/job_performance.json"

    
    list_completed_task = self.completed_tasks()
    
    
    if len(list_completed_task) == 0:
      raise Exception('list_completed_task is empty, no performance to summarize')
    
    list_arguments_completed_tasks = [completed_task.arguments for completed_task in list_completed_task ] 
    
    arguments = list_arguments_completed_tasks[0]

      
    ###get list of all configs: spark.sparkContext.getConf().getAll()
    current_notebook_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split("/")[-1]
    dp = pd.DataFrame(data = [time_spent] , index = ["running_time"], columns = [current_notebook_name] ).T
    col_key = "job_timestamp"
    dp[col_key] = datetime.now()
    
    
    list_arguments = list(arguments)
    str_arguments = str(arguments)
    
    dp["arguments"] = str_arguments
    

    ###list_attributes = ["driverNodeTypeId" , "workerNodeTypeId","clusterUsageTags.clusterMinWorkers","clusterUsageTags.clusterMaxWorkers"] clusterScalingType
    list_attributes = ["driverNodeTypeId" , "workerNodeTypeId",'clusterUsageTags.clusterScalingType','clusterUsageTags.clusterWorkers']
    list_autoscale_attributes = ["clusterUsageTags.clusterMinWorkers","clusterUsageTags.clusterMaxWorkers"]
    
    
    for attribute in list_autoscale_attributes:
        if spark.conf.get("spark.databricks.clusterUsageTags.clusterScalingType") == "fixed_size" :
          dp[attribute] = 'empty'
        else:
          dp[attribute] = spark.conf.get("spark.databricks.{}".format(attribute))
        
      
    for attribute in list_attributes:
        dp[attribute] = spark.conf.get("spark.databricks.{}".format(attribute))
        
    ### delete the prefix "clusterUsageTags" 
    for c in dp.columns:
      if c.startswith("clusterUsageTags"):
        c_attribute = c.split(".")[1]        
        dp = dp.rename( columns = {c: c_attribute} )
        
    ### CamelCase ---> snake_case    
    for c in dp.columns:
      dp = dp.rename( columns = {c: camel2snake(c) } )
    
    dp = dp.reset_index(drop = False).rename(columns = {"index": "job_name"})      
    
    
    ### https://databricks.com/product/aws-pricing/instance-types
    ### THIS DATA SHOULD BE READ "DYNAMICALLY" FROM THE WEBSITE
    text = """
    General Purpose Instances - M,vCPUs,Memory(GB),Data Engineering (DBU/hour) ?
    m4.large,2,8,0.4
    m4.xlarge,4,16,0.75
    m4.2xlarge,8,32,1.5
    m4.4xlarge,16,64,3
    m4.10xlarge,40,160,8
    m4.16xlarge,64,256,12
    m5.large,2,8,0.35
    m5.xlarge,4,16,0.69
    m5.2xlarge,8,32,1.37
    m5.4xlarge,16,64,2.74
    m5.12xlarge,48,192,8.23
    m5.24xlarge (Beta),96,384,16.46
    m5d.large (Beta),2,8,0.34
    m5d.xlarge (Beta),4,16,0.69
    m5d.2xlarge (Beta),8,32,1.37
    m5d.4xlarge (Beta),16,64,2.74
    m5d.12xlarge (Beta),48,192,8.23
    m5d.24xlarge (Beta),96,384,16.46
    m5a.large (Beta),2,8,0.31
    m5a.xlarge (Beta),4,16,0.61
    m5a.2xlarge (Beta),8,32,1.23
    m5a.4xlarge (Beta),16,64,2.46
    m5a.8xlarge (Beta),32,128,4.91
    m5a.12xlarge (Beta),48,192,7.37
    m5a.16xlarge (Beta),64,256,9.83
    m5a.24xlarge (Beta),96,384,14.74
    Compute Optimized Instances - C,vCPUs,Memory(GB),Data Engineering (DBU/hour) ?
    c3.2xlarge,8,15,1
    c3.4xlarge,16,30,2
    c3.8xlarge,36,60,4
    c4.2xlarge,8,15,1
    c4.4xlarge,16,30,2
    c4.8xlarge,36,60,4
    c5.xlarge,4,8,0.61
    c5.2xlarge,8,16,1.21
    c5.4xlarge,16,32,2.43
    c5.9xlarge,36,72,5.46
    c5.18xlarge (Beta),72,144,10.92
    c5d.xlarge (Beta),4,8,0.69
    c5d.2xlarge (Beta),8,16,1.37
    c5d.4xlarge (Beta),16,32,2.74
    c5d.9xlarge (Beta),36,72,5.48
    c5d.18xlarge (Beta),72,144,10.96
    Memory Optimized Instances - R,vCPUs,Memory(GB),Data Engineering (DBU/hour) ?
    r3.xlarge,4,31,1
    r3.2xlarge,8,61,2
    r3.4xlarge,16,122,4
    r3.8xlarge,32,244,8
    r4.xlarge,4,31,1
    r4.2xlarge,8,61,2
    r4.4xlarge,16,122,4
    r4.8xlarge,32,244,8
    r4.16xlarge,64,488,16
    r5.large,2,16,0.45
    r5.xlarge,4,32,0.9
    r5.2xlarge,8,64,1.8
    r5.4xlarge,16,128,3.6
    r5.12xlarge,48,384,10.8
    r5.24xlarge (Beta),96,768,21.6
    r5d.large (Beta),2,16,0.45
    r5d.xlarge (Beta),4,32,0.9
    r5d.2xlarge (Beta),8,64,1.8
    r5d.4xlarge (Beta),16,128,3.6
    r5d.12xlarge (Beta),48,384,10.8
    r5d.24xlarge (Beta),96,768,21.6
    r5a.xlarge (Beta),4,32,0.81
    r5a.2xlarge (Beta),8,64,1.61
    r5a.4xlarge (Beta),16,128,3.23
    r5a.8xlarge (Beta),32,256,6.46
    r5a.12xlarge (Beta),48,384,9.69
    r5a.16xlarge (Beta),64,512,12.91
    r5a.24xlarge (Beta),96,768,19.37
    Storage Optimized Instances - I,vCPUs,Memory(GB),Data Engineering (DBU/hour) ?
    i3.large,2,15,0.75
    i3.xlarge,4,31,1
    i3.2xlarge,8,61,2
    i3.4xlarge,16,122,4
    i3.8xlarge,32,244,8
    i3.16xlarge (Beta),64,488,16
    i2.xlarge,4,31,1.5
    i2.2xlarge,8,61,3
    i2.4xlarge,16,122,6
    i2.8xlarge,32,244,12
    GPU Instances - P,vCPUs,Memory(GB),Data Engineering (DBU/hour) ?
    p2.xlarge,4,61,1.22
    GPU,,,
    p2.8xlarge,32,488,9.76
    GPU,,,
    p2.16xlarge,64,732,19.52
    GPU,,,
    p3.2xlarge,8,61,4.15
    GPU,,,
    p3.8xlarge,32,244,16.6
    GPU,,,
    p3.16xlarge (Beta),64,488,33.2
    GPU,,,
    Memory Optimized Instances - Z1D,vCPUs,Memory(GB),Data Engineering (DBU/hour) ?
    z1d.large (Beta),2,16,0.66
    z1d.xlarge (Beta),4,32,1.33
    z1d.2xlarge (Beta),8,64,2.66
    z1d.3xlarge (Beta),12,96,3.99
    z1d.6xlarge (Beta),24,192,7.97
    z1d.12xlarge (Beta),48,384,15.94

    """
    
    
    index_time = pd.DatetimeIndex(dp["running_time"])
    array_time = index_time.hour * 60 + index_time.minute
    
    col_running_time_in_minutes = "running_time_in_minutes"
    dp[col_running_time_in_minutes] = array_time
    

    ### node atrtibutes: en node can be either a worker or a driver
    col_node = 'node_type_id'
    col_dbu = 'dbu_per_minute' ### [dbu / hour]
    col_cost = 'cost_per_minute_per_node' ### [usd / hour]
    list_header = [ col_node, 'vcpu', 'ram', col_dbu ]
    list_instance_type = text.split('\n')[2:]
    
    
    ### [usd / dbu]
    BASE_RATE_DATA_ENGINEERING_CLUSTER = 0.2
    FACTOR = BASE_RATE_DATA_ENGINEERING_CLUSTER / 60
    
    
    
    ### [dbu / hour]
    col_dbu_driver = 'dbu_per_minute_per_driver'
    col_dbu_worker = 'dbu_per_minute_per_worker'
    
    ### [usd / hour] = [usd / dbu] * [dbu / hour] = BASE_RATE_DATA_ENGINEERING_CLUSTER * col_dbu
    col_cost_per_driver_per_minute= 'cost_per_driver_per_minute'
    col_cost_per_worker_per_minute = 'cost_per_worker_per_minute'
    
    ### 
    col_driver = 'driver_node_type_id'
    col_worker = 'worker_node_type_id'
    
    
    dp_cost = pd.DataFrame(list_instance_type)
    dp_cost = dp_cost[0].str.split("," , expand =True)
    dp_cost.columns = list_header
    
    
    dp_cost[col_node]=dp_cost[col_node].str.strip()
    dp_cost[col_dbu] = pd.to_numeric(dp_cost[col_dbu] , errors = "coerce")
    dp_cost[col_cost] = dp_cost[col_dbu] * FACTOR
    
    
    col_cost_per_driver = 'cost_job_per_driver'
  
    
    str_driver_instance = dp[col_driver][0]
    str_worker_instance = dp[col_worker][0]

    
    print("***********************************")
    
    
    
    dbu_per_hour_per_driver = dp_cost[dp_cost[col_node] == str_driver_instance].reset_index(drop=True)[col_dbu]
    dbu_per_hour_per_worker = dp_cost[dp_cost[col_node] == str_worker_instance].reset_index(drop=True)[col_dbu]
    
    vcpu_driver = dp_cost[dp_cost[col_node] == str_driver_instance].reset_index(drop=True)['vcpu']
    vcpu_worker = dp_cost[dp_cost[col_node] == str_worker_instance].reset_index(drop=True)['vcpu']
    
    for node in ['driver','worker']:
      for resource in ['vcpu', 'ram']:
         
         str_node = '{}_node_type_id'.format(node)
         str_node_instance = dp[str_node][0]    
         dp [ '{}_{}'.format(node,resource) ]  = dp_cost[dp_cost[col_node] == str_node_instance].reset_index(drop=True)[resource]
    
    
    logprint("dbu_per_hour_per_driver: {}".format(dbu_per_hour_per_driver[0]) ) 
    logprint("dbu_per_hour_per_worker: {}".format(dbu_per_hour_per_worker[0]) )
    logprint("BASE_RATE_DATA_ENGINEERING_CLUSTER: {}".format(BASE_RATE_DATA_ENGINEERING_CLUSTER) )
    logprint("FACTOR: {}".format(FACTOR) )
    
    
      
    if spark.conf.get("spark.databricks.clusterUsageTags.clusterScalingType") == "fixed_size":
      n_workers = int( dp['cluster_workers'] )
    else:
      
      ### if autoscaling, approximate by taking the average amount of workers
      cluster_min_workers = int(dp['cluster_min_workers'])
      cluster_max_workers = int(dp['cluster_max_workers'])
      logprint("cluster_min_workers: {}".format(cluster_min_workers))
      logprint("cluster_max_workers: {}".format(cluster_max_workers))
      
      n_workers = ( cluster_min_workers + cluster_max_workers ) / 2
      logprint("avg n_workers: {}".format(n_workers))
      
      ### overwrite with avg number of workers
      dp['cluster_workers'] = n_workers
    
    
    total_cost_per_minute = FACTOR * ( float(dbu_per_hour_per_driver) + float(n_workers) * float(dbu_per_hour_per_worker) )
    
    total_cost_per_minute = np.round(total_cost_per_minute,4)
    logprint("total_cost_per_minute: {}".format(total_cost_per_minute) )
    
     
    dp["total_cost_per_minute"] = total_cost_per_minute
    dp["total_cost"] = total_cost_per_minute * dp[col_running_time_in_minutes]
    dp["total_cost"] = dp["total_cost"].round(2)
    
    print(dp.T)
    df = spark.createDataFrame(dp)
    
    df = df.recast('cluster_workers','str')
  
    df.upsert_to_redshift('util.job_performance', upsert_keys=col_key, sort=col_key , list_long_strings = "arguments" )
    
  


    
        
    

    
  def query_jobs_api():
    ### Henter info om run fra dbc
    ### En forutsetning for å få treff er at DAG notebooken benytter notebookens navn på DAG-tasken. F.eks. bør ../dag/crm_live.py notebooken definere DAG tasken slik: d = DAG('crm_live', send_to_slack=False). 
    ### THIS IS DONE AUTOMATICALLY BY INITIALIZING DAG WITHOUT ANY NAME (SEE THE __init__ METHOD OF CLASS DAG)
    import base64
    job_runs_url = '{}api/2.0/jobs/runs/list'.format(env('DATABRICKS_URL'))
    token = env('DBC_TOKEN')
    r = requests.get(job_runs_url, headers={"Authorization": "Basic " + base64.standard_b64encode(("token:" + token).encode("utf-8")).decode("utf-8")})
    ### Skriver om resultatet til df
    filename = "/tmp/jobRuns_{}.json".format(time.time())
    dbutils.fs.put(filename, r.text, True)
    df = spark.read.json(filename)
    
    return df
    

  ### Sender ut varsel med oppsummering av kjøringen.
  def summarize(self, send_to_slack=True, send_to_sns=False, topics=[env('SNS_TOPIC_FORVALTNING')], raise_exception=True):
    time_spent = self.timer.stop()
    self.display_list_task_with_status(status='failed')
    
    ### Henter info om run fra dbc
    ### En forutsetning for å få treff er at DAG notebooken benytter notebookens navn på DAG-tasken. F.eks. bør ../dag/crm_live.py notebooken definere DAG tasken slik: d = DAG('crm_live', send_to_slack=False).
    import base64
    job_runs_url = '{}api/2.0/jobs/runs/list'.format(env('DATABRICKS_URL'))
    token = env('DBC_TOKEN')
    r = requests.get(job_runs_url, headers={"Authorization": "Basic " + base64.standard_b64encode(("token:" + token).encode("utf-8")).decode("utf-8")})
    ### Skriver om resultatet til df
    filename = "/tmp/jobRuns_{}.json".format(time.time())
    dbutils.fs.put(filename, r.text, True)
    df = spark.read.json(filename)
    
    ### Henter ut og formaterer resultatsettet fra JSONen
    df = df.select(explode('runs').alias('runs_ex')).select('runs_ex.*')
    df = (df.withColumn('notebook_path', df.task.notebook_task.notebook_path)
            .withColumn('start_time', from_unixtime(df.start_time / 1000))
            .withColumn('dag', split('notebook_path', '/')))
    df = df.withColumn('dag', df.dag[size(df.dag)-1])
    df = df.select( 'run_name', 'dag', 'notebook_path', 'run_page_url', 'start_time')
    ### Beholder kun de nyeste iterasjonene av job runs
    window = Window.partitionBy('run_name').orderBy(df['start_time'].desc())
    df = df.withColumn('latest_run', rank().over(window))
    df = df.filter('latest_run = 1 AND dag = "{}"'.format(self.name))
    ###df.show()
    
      
    ### Henter ut verdiene vi ønsker
    if df.count() > 0:
      run_url = df.select('run_page_url').collect()[0][0]
      job_name = df.select('run_name').collect()[0][0]
      
      
    else:
      run_url = "{}#joblist".format(env('DATABRICKS_URL'))
      job_name = '?'
      
    
    if STR_ENVIRONMENT == 'PROD':
      send_to_sns=True
    ### Publiserer til SNS ved feil
    if self.failed_tasks():
      if send_to_sns==True:
       
        
        lp("PREPARING THE MESSAGE TO THE DWH-VAKT")
        ### Formaterer meldingstekst
        dag           = self.name
        errors_str    = '{}'.format('\n'.join([str(c) for c in self.task_error()]))
        failed_str    = 'Failed task: {}'.format('\n'.join([c.link() for c in self.failed_tasks()]))
        skipped_str   = 'Skipped task: {}'.format('\n'.join([c.link() for c in self.skipped_tasks()]))
        completed_str = 'Completed task: {}'.format('\n'.join([c.link() for c in self.completed_tasks()]))
        time_str      = 'Time spent: {}'.format(time_spent)
        formatert_for_mail = failed_str + '\n\n' + skipped_str + '\n\n' + completed_str + '\n\n' + time_str
       
         
        ### If no text was initialized in the dictionary, fill with default mesage
        try: 
          dict_databricks_job[dag]
        except:  
          dict_databricks_job[dag] = 'Not defined yet'
          lp("message to vakt not defined yet")
          
        ### Publiserer til SNS
        session = boto3.session.Session(region_name='eu-central-1')
        snsClient = session.client('sns', config=boto3.session.Config(signature_version='s3v4'))
        for t in topics:
          
          message = """Noe gikk galt under kjøringen!\n\n  -ENV:\t{}\n  -JOB:\t{}\n  -DAG:\t{}\n\n\n{}\n\n\nERROR: {}\nLOG: {}\n\n\n  - WHAT TO DO: {}""".format(env('ENVIRONMENT').lower(), job_name, dag, formatert_for_mail, errors_str, run_url, dict_databricks_job[dag])
          
          
          response = snsClient.publish(
            TopicArn=t,
            Message=message,
            Subject="AWS - DAG **{}** har feilet under kjøring - [{}]".format(dag, env('ENVIRONMENT')),
            MessageStructure='string',
            MessageAttributes={
                'Environment': {
                    'DataType': 'String',
                    'StringValue': '{}'.format(env('ENVIRONMENT')),
                }
            }
          )
    
 

    ### Trist melding til Slack hvis man har hatt feil under noen av kjøringene.
    
    if send_to_slack==True:
      
      if self.failed_tasks():
        
        list_failed_task = self.failed_tasks()      
        failed_task_with_arguments = ''.join( [failed_task.link() + '  {}'.format(failed_task.arguments) for failed_task in list_failed_task ] )
        slack_dict = dict(
          title="DAG {} completed with failures".format(self.name),
          fields=[

            dict(title='Failed tasks'   , value= failed_task_with_arguments,    short=False),
            ###dict(title='LINK FAILED TASK'   , value= link_job,    short=False),
            dict(title='Error'  , value='\n'.join([ str(c) for c in self.task_error()]),   short=False),
            dict(title='Skipped tasks'  , value='\n'.join([c.link() for c in self.skipped_tasks()]),   short=False),
            dict(title='Completed tasks', value=', '.join([c.link() for c in self.completed_tasks()]), short=False),
            dict(title='Time spent'     , value=time_spent,                                            short=True),
          ],
          title_link="{}".format(run_url),
          color="danger",
          footer="NSB Databricks DAG [{}]".format(env('ENVIRONMENT')),
          ts=datetime.now().strftime("%s"),
        )
        if env('ENVIRONMENT')=='PROD':
          slack_dict["text"]=':rotating_light: <!channel> DAG failure.'
        else:
          slack_dict["text"]=':rotating_light: DAG failure.'
        attachments_slack = [slack_dict]
        send_slack(attachments = attachments_slack)

      
      ### Gladmelding til Slack hvis alt har gått bra.
      else:
        send_slack(
          attachments=[dict(
            title="DAG {} finished successfully".format(self.name), 
            text=":surfer: All tasks finished successfully.",
            fields=[
              dict(title='Completed tasks',value=', '.join([c.link() for c in self.completed_tasks()]), short=False),
              dict(title='Time spent', value=time_spent, short=True)
            ],
            title_link="{}".format(run_url),
            color="good",
            footer="NSB Databricks DAG [{}]".format(env('ENVIRONMENT')),
            ts=datetime.now().strftime("%s"),
          )])
        
    ### Sender exception om spesifisert - typisk om .failedTasks() stemmer. Status på en jobb settes til failed
    ### når .summarize() kjøres med exception = TRUE
    
    if self.failed_tasks() and raise_exception == True:
      raise Exception('Error in notebook {}'.format(self.name))
    if self.failed_tasks():
      lp("Failed tasks, skipping self.write_docs() , self.summarize_performance() ")
    else:  
      try:
        lp("Writing documentation tables...")
        self.write_docs()
      except Exception as e:
        logprint("write_docs: exception: {}".format(e))

      try:
        logprint("Attempting to summarize performance...")
        logprint("check total cost formula here: https://jico.nsb.no/confluence/display/NSBFTITSI/Job+performance+-+cluster+config+-+kostnad")      
        self.summarize_performance(time_spent)
      except Exception as e:
        logprint("summarize_performance: exception: {}".format(e))

      
  def __str__(self):
    return "DAG: {}".format(self.name)
