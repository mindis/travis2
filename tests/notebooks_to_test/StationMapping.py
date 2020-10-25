# Databricks notebook source
class StationMapping():
    
  """ Usage example:
      
      mapping=StationMapping()
      mapping.get_id("Oslo S")  # 100000
      mapping.get_name(1382000) # 'Rognan'
      mapping.get_stations_between_a_b("Oslo S" , "Järpen") #['Oslo S', 'Åre', 'Östersund Central', 'Järpen']
     
      Used in notebook: 
      _stage_planned_railway_maintenance

  """
      
  import pandas as pd 
  
  def __init__(self):
    query = """
    SELECT lisa_id , name 
    FROM stage.sharepoint_entur_mapping_nsr_lisa
    WHERE lisa_id is not null
    """
    df_mapping_nsr_lisa = rs_query(query)
    self.dp_mapping = df_mapping_nsr_lisa.toPandas()

    self.dp_mapping["name"] = self.dp_mapping["name"].str.replace("stasjon","").str.replace("station","").str.strip()
    self.list_distinct_stations=list(self.dp_mapping["name"].unique()) 
    
  
  def get_id(self,name):
    
    if name not in self.list_distinct_stations:
      raise NameError(f"The station you passed does not exist in the database. The present stations are: {self.list_distinct_stations} ")
    
    return self.dp_mapping[self.dp_mapping["name"] == name]["lisa_id"].iloc[0]

  def get_name(self,lisa_id):
    return self.dp_mapping[self.dp_mapping["lisa_id"] == lisa_id]["name"].iloc[0]


    
  def get_stations_between_a_b(self,a,b,display=True):
  
    if isinstance(a,str):
      print(f"station: {a}",end=" ")
      a=self.get_id(a)

    if isinstance(b,str):
      print(f"station: {b}",end=" ")
      b=self.get_id(b)

    ### hente alle stasjoner som ligger mellom A og B 
    dp_mapping_a_b = self.dp_mapping[ ( self.dp_mapping["lisa_id"] >= a ) & ( self.dp_mapping["lisa_id"] <= b ) ].sort_values("lisa_id")
    self.list_stations_id = dp_mapping_a_b["lisa_id"].tolist()
    self.list_stations    = dp_mapping_a_b["name"].tolist()
    
    print("list_stations between")
    print(self.list_stations)
    
    if display==True:
      print(dp_mapping_a_b)

    
