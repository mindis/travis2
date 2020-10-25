# Databricks notebook source
### Used when calling upsert_to_redshift to detect inconsistencies
### Can also be used in conditional workflows (ex: if comparison.n_inconsistent_names > 0: comparison.drop_table() ) 
class SchemaComparison():
  def __init__(self,df,table_name):
    
    self.df = df
    self.table_name = table_name
    self.table_redshift = rs_query("select top 1 * from {}".format(table_name), print_query = False, trace_lineage = False )
    
  
  def drop_table():
     logprint('Deleting the existing table in Redsfhit...')
     rs_command("DROP TABLE " +  self.table_name, print_status=True)
     logprint(  self.table_name + " deleted") 
     ### After dropping, it seems necessary to wait a bit 
     time.sleep(15)
    
    
  def get_schema(self):
    
    ### Populates attriubte "n_inconsistencies"
    self.count_inconsistencies()
    
    ### Return the "fixed" df (if there are no inconsistencies, returns the same df as input)
    return self.fix_schema()
    
    
  def count_inconsistencies(self):
    
    ### list of cols 
    self.list_cols_table_in_redshift = self.table_redshift.columns
    list_cols_table_in_redshift = self.list_cols_table_in_redshift
    
    list_df_cols = self.df.columns
    
    ### list of tuples (col , datatype)
    list_tuple_cols_and_datatypes_table_in_redshift = self.table_redshift.dtypes
    list_tuple_cols_and_datatypes_df = self.df.dtypes
    
    ### len of lists
    len_list_cols_table_in_redshift = len(list_cols_table_in_redshift)
    len_list_df_cols = len(list_df_cols)
    
    
    self.name_consistency = set(list_df_cols) == set(list_cols_table_in_redshift)
    self.name_and_order_consistency = list_cols_table_in_redshift == list_df_cols 
    self.name_and_order_and_datatype_consistency = list_tuple_cols_and_datatypes_table_in_redshift == list_tuple_cols_and_datatypes_df 
     
    logprint('len_list_cols_table_in_redshift: {}'.format(len_list_cols_table_in_redshift) )
    logprint('len_list_df_cols: {}'.format(len_list_df_cols) )
    
    self.display_consistency()
     
  
  def fix_schema(self):

    """
    Fetches the schema (columns and data types) from Redshift and enforces it on df.     
    """
     
    
    logprint("self.n_inconsistencies: {}".format(self.n_inconsistencies) ) 
    if self.n_inconsistencies > 0:
      
      logprint("Fixing order and data types to match Redshift...")
      
      ### Fix 1: For each column where the names coincides, impose the data type from Redshift
      for c,d in self.table_redshift.dtypes:                        
          self.df = self.df.recast(c,d)
          
      ### Fix 2: Reorder columns according to order in Redshift  
      self.df = self.df.select(self.list_cols_table_in_redshift) 
      
      ### After fixing order and datatypes, show the consistency table again
      self.display_consistency()
        
    return self.df   
  
  def display_consistency(self):
    
        """ Shows a consistency table, where inconsistency = 1 if, for each entry, there is a difference in column name or datatype  """
        ###dp_structure = pd.DataFrame(df.schema.jsonValue()["fields"])
        def break_struct_type(schema_input):
          s_schema=pd.DataFrame(schema_input).iloc[:,0].astype(str)           
          ### removes frist 12 chars: " StructField( "
          s_trimmed=s_schema.apply( lambda x:  x[12:] )        
          dp_expanded = s_trimmed.str.split( ',' , expand=True ) 
          ###print(dp_expanded)
          dp= dp_expanded.iloc[:,0:2]
          return dp
        
        import pandas as pd   
        dp = break_struct_type(self.df.schema)
        dp.columns = ['DF_COL' , 'DF_DATA_TYPE']
 
        dp2= break_struct_type(self.table_redshift.schema)
        dp2.columns = ['REDSHIFT_COL' , 'REDSHIFT_DATA_TYPE']

        dp_all = dp.join(dp2, how = "outer")
        dp_all = dp_all[ ['DF_COL' , 'REDSHIFT_COL','DF_DATA_TYPE','REDSHIFT_DATA_TYPE'] ]
        
        ### initalise all rows with 0
        dp_all.loc[:,"INCONSISTENCY"] = 0

        for i in range( 0 , len(dp_all) ):
          ### cast entries to lowercase just for determining consistency
          a = list( dp_all[ ['DF_COL' , 'DF_DATA_TYPE'] ] .iloc[i].str.lower() )
          b = list( dp_all[ ['REDSHIFT_COL' , 'REDSHIFT_DATA_TYPE'] ] .iloc[i].str.lower() )
          if a != b:
            dp_all.loc[i,"INCONSISTENCY"] = 1

        self.n_inconsistencies = dp_all["INCONSISTENCY"].sum() 
        with pd.option_context('display.max_rows', 100, 'display.max_columns', 100, 'display.max_colwidth', -1 , 'display.expand_frame_repr', False , 'display.colheader_justify' , 'left'):
          print(dp_all)
        logprint("Inconsistent entries (name/data type): {}".format(self.n_inconsistencies) )  
        
    
