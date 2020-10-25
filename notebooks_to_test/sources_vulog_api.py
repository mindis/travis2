# Databricks notebook source
#%run ../setup_general

# COMMAND ----------

#%run ../_modules/sources_redshift

# COMMAND ----------

from base64 import b64encode
import requests

# COMMAND ----------

#################################################################
### flatten_df flater ut alle nestede structer i en dataframe ###
### argument er dataframe som skal behandles                  ###
### hentet fra nettet, 2020-06-02, Jan Eilertsen              ###
#################################################################

def df_flatten_dataframe(nested_df):
    flat_cols = [c[0] for c in nested_df.dtypes if c[1][:6] != 'struct']
    nested_cols = [c[0] for c in nested_df.dtypes if c[1][:6] == 'struct']

    flat_df = nested_df.select(flat_cols +
                               [F.col(nc+'.'+c).alias(nc+'_'+c)
                                for nc in nested_cols
                                for c in nested_df.select(nc+'.*').columns])
    return flat_df

# COMMAND ----------

############################################################
### returnerer token som benyttes i requester etter data ###
############################################################
def vulog_api_token(str_url, str_client_secret, str_client_id, str_x_api_key, str_content_type, str_username, str_password):
  url = str_url  
  
  payload = 'client_secret={str_client_secret}&client_id={str_client_id}&grant_type=password&username={str_username}&password={str_password}'.format(str_client_secret = str_client_secret, str_client_id = str_client_id, str_username = str_username, str_password = str_password)
    
  headers = {
  'x-api-key': "%s" % str_x_api_key,
  'Content-Type': "%s" % str_content_type
}
  api_tries_before_fail = 5
  for n in range(api_tries_before_fail):
    try:
      response = requests.request("POST", url, headers=headers, data = payload)
      response.raise_for_status()
    except requests.exceptions.HTTPError as errh:
      logprint("Http Error:", errh)
      time.sleep(5)
      continue
    except requests.exceptions.ConnectionError as errc:
      logprint("Error Connecting:", errc)
      time.sleep(5)
    except requests.exceptions.ChunkedEncodingError as errce:
      logprint("Connection broken, incompleate read:", errce)
      time.sleep(5)
      continue
    except requests.exceptions.Timeout as errt:
      logprint("Timeout Error:", errt)
      time.sleep(5)
      continue
    except requests.exceptions.RequestException as err:
      logprint("Error", err)
      time.sleep(5)
      continue
    except:
      logprint("Failed to get contact with API.")
      time.sleep(5)
      continue
    else:    
      break
    
  
  logprint('response fra api: {}'.format(response))
  
  ### response er en streng, så den må konverteres til json for å tolkes som dict, for å kunne trekke ut access_token
  dict_response = json.loads(response.text)
  token = dict_response['access_token']
  
  return token

# COMMAND ----------

###########################################################################################
### returnerer en dataframe med innholdet av csv-filene som prosessere i hver iterasjon ###
###########################################################################################
def vulog_api_data_csv(str_url, str_token, str_x_api_key, str_fromdate, str_filename='vulog_'):
  url = str_url
  filename_prefix = '/tmp/' + str_filename + str_fromdate + '/'  
  logprint(filename_prefix)
  filenumber = 1
  
  payload = {}
  headers = {
  'Accept': 'text/csv, text/plain',
  'Content-Type': 'application/json',
  'Authorization': "Bearer %s" % str_token,
  'x-api-key': "%s" % str_x_api_key
}
  
  api_tries_before_fail = 5
  for n in range(api_tries_before_fail):
    try:
      response = requests.request("POST", url, headers=headers, data = payload)        
      response.raise_for_status()
    except requests.exceptions.HTTPError as errh:
      logprint("Http Error:", errh)
      time.sleep(5)
      continue
    except requests.exceptions.ConnectionError as errc:
      logprint("Error Connecting:", errc)
      time.sleep(5)
    except requests.exceptions.ChunkedEncodingError as errce:
      logprint("Connection broken, incompleate read:", errce)
      time.sleep(5)
      continue
    except requests.exceptions.Timeout as errt:
      logprint("Timeout Error:", errt)
      time.sleep(5)
      continue
    except requests.exceptions.RequestException as err:
      logprint("Error", err)
      time.sleep(5)
      continue
    except:
      logprint("Failed to get contact with API.")
      time.sleep(5)
      continue
    else:    
      break
      
  logprint('Skriver til S3')
  dbutils.fs.put(filename_prefix + str(filenumber), response.text, True)
    
  logprint('Leser alle filer som er skrevet til S3')
  df = spark.read.csv(filename_prefix + '*', inferSchema=True, header=True)
        
  return df

# COMMAND ----------

##############################################################################################
### returnerer en dataframe med innholdet av json-filene som prosessere i hver iterasjon   ###
### str_operation: GET eller POST                                                          ###
### str_url: base og endppoint                                                             ###
### str_token: token for autentisering, som hentes fra def vulog_api_token                 ###
### str_x_api_key: fast verdi opprettet av Vulog for Vy                                    ###
### str_fromdate: dato man henter data for, kan bare ta en om gangen                       ###
### page_number: den siden man henter data for. Inkrementeres utenfor denne prosedyren.    ###
### page_size: antall elementer på hver page. Denne reduseres hvis apiet begynner å hoste. ###
##############################################################################################
def vulog_api_data_json(str_operation, str_url, str_token, str_x_api_key, str_fromdate=None, str_filename='vulog_', page_number=0,
                         page_size=5000, environment='PROD'):
  url = str_url+str_fromdate+'&page={}&size={}'.format(page_number, page_size)
  filename = '/tmp/{}_{:%Y%m%d%H%M%S}_{}_p'.format(str_filename, datetime.now(), environment)
 
  outputlist = []
  
  filenumber = 1
  payload = {}
  ### POST eller GET
  operation = str_operation 
    
  headers = {  
  'Content-Type': 'application/json',
  'Authorization': "Bearer %s" % str_token,
  'x-api-key': "%s" % str_x_api_key
}
  api_tries_before_fail = 5
  for n in range(api_tries_before_fail):
    try:
      response = requests.request(operation, url, headers=headers, data = payload)      
      response.raise_for_status()
    except requests.exceptions.HTTPError as errh:
      logprint("Http Error:", errh)
      time.sleep(5)
      continue
    except requests.exceptions.ConnectionError as errc:
      logprint("Error Connecting:", errc)
      time.sleep(5)
    except requests.exceptions.ChunkedEncodingError as errce:
      logprint("Connection broken, incompleate read:", errce)
      time.sleep(5)
      continue
    except requests.exceptions.Timeout as errt:
      logprint("Timeout Error:", errt)
      time.sleep(5)
      continue
    except requests.exceptions.RequestException as err:
      logprint("Error", err)
      time.sleep(5)
      continue
    except:
      logprint("Failed to get contact with API.")
      time.sleep(5)
      continue
    else:    
      break
      
  #print(response.headers)    
  page_last = response.headers['last']  
  page_number = response.headers['number']   
  total_pages = response.headers['totalPages']    
  j_data = json.loads(response.text)       
      
  outputlist.append(j_data)      

  jslines = '\n'.join([json.dumps(outputObject) for outputObject in outputlist])
  dbutils.fs.put("{}.json".format(filename), jslines, True)
  df = spark.read.json("{}.json".format(filename)) 
  
  return df, int(total_pages), page_last, int(page_number)

# COMMAND ----------

#str_url = 'https://java-sta.vulog.com/auth/realms/VY-NOOSL/protocol/openid-connect/token'
#str_client_secret = 'e5f9a5bc-a17c-4339-98ea-9abc6a79c9fd'
#str_client_id = 'VY-NOOSL_secure'
#str_username = 'arve.heieren@vy.no'
#str_password = 'Arve1234'
#str_x_api_key = '5581e22f-6aad-4a0e-9dec-541d565386d4'
#str_content_type = 'application/x-www-form-urlencoded'

#str_filename = 'bybil_vulog_'

### hente token
#str_token = vulog_api_token(str_url, str_client_secret, str_client_id, str_x_api_key, str_content_type, str_username, str_password)


# COMMAND ----------

### json hente data med token
#str_url_data = 'https://java-sta.vulog.com/boapi/proxy/user/billing/fleets/VY-NOOSL/invoices?startDate='
#str_fromdate = '2020-05-19T00:00:00Z'
#str_fromdate = '2020-05-27T00:00:00Z'
#str_fromdate = '2020-06-05T00:00:00Z'
#url = str_url_data + str_fromdate
#page_number = 0
#total_pages = 100
#page_last = False
#page_size = 100
#print(url)

#while page_number <= total_pages:
#  str_token = vulog_api_token(str_url, str_client_secret, str_client_id, str_x_api_key, str_content_type, str_username, str_password)
#  print('iterasjon')
#  logprint('current_page: {}'.format(page_number))  
#  logprint('total_pages: {}'.format(total_pages))
#  logprint('page_last: {}'.format(page_last))  
#  df, total_pages, page_last, page_number = vulog_api_data_json('GET', str_url_data, str_token, str_x_api_key, str_fromdate, str_filename, page_number, page_size, 'TEST') 
#  if page_number < total_pages:
#    if df.count():
#      logprint('skriver dataframe for invoice til redshift')
#      process_df_invoice(df, str_fromdate, page_number )
#      logprint('skriver dataframe for trip til redshift')
#      process_df_trip(df, str_fromdate, page_number)
#      logprint('skriver dataframe for product til redshift')
#      process_df_product(df, str_fromdate, page_number)
      
    
#  page_number = page_number + 1

# COMMAND ----------

### json hente data med token
#str_url_data = 'https://java-sta.vulog.com/boapi/proxy/user/billing/fleets/VY-NOOSL/invoices?startDate='
#str_fromdate = '2020-05-19T00:00:00Z'
#str_fromdate = '2020-05-27T00:00:00Z'
#url = str_url_data + str_fromdate
#page_number = 0
#total_pages = 100
#page_last = False
#page_size = 10
#print(url)
#https://java-sta.vulog.com/boapi/proxy/user/billing/fleets/VY-NOOSL/[REDACTED]s?startDate=2020-05-14T00:00:00Z
#str_operation, str_url, str_token, str_x_api_key, str_fromdate=None, str_filename='vulog_', environment='PROD'
#str_url_data = 'https://java-sta.vulog.com/boapi/proxy/user/billing/fleets/VY-NOOSL/invoices?startDate='
#str_fromdate = '2020-05-14T00:00:00Z'
#url = str_url_data + str_fromdate
#print(url)
# https://java-sta.vulog.com/boapi/proxy/user/billing/fleets/VY-NOOSL/invoicxx?startDate=2020-05-14T00:00:00Z
#df, total_pages, page_last, page_number = vulog_api_data_json('GET', str_url_data, str_token, str_x_api_key, str_fromdate, str_filename, page_number, page_size, 'TEST') 

# COMMAND ----------

#display(df)
