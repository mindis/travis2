# Databricks notebook source
import random
import time
import requests
import json
from six.moves.urllib.parse import urlencode

# COMMAND ----------

##################################################################################################################################
### Lage POST-request for å hente X-User-ID og X-API-Key. Disse ligger i header. Selve body mangler data, derav returkode 204. ###
##################################################################################################################################
def banenor_get_authorization(str_url_base, str_endpoint, str_selskap, str_GUID):
    
  dict_header = {'Authorization': "{selskap};{GUID}".format(selskap=str_selskap, GUID=str_GUID)}
  
  
  str_url = str_url_base + str_endpoint
  logprint('Eksekverer: {}'.format(str_url))
  response = requests.post(str_url, headers=dict_header)
  logprint('returkode banenor_get_authorization: {}'.format(response))
  
  x_user_id = response.headers['X-User-ID']
  x_api_key = response.headers['X-API-Key']
  
  dict_authorization_header = {'X-User-ID': "%s" % x_user_id,
          'X-API-Key': "%s" % x_api_key,
         'Accept': 'application/json'}
  
  logprint('returnerer dict med aid-er for autentisering: {}'.format(dict_authorization_header))
  
  
  return dict_authorization_header


  

# COMMAND ----------

###################################################
### /TogFremforing/GetTogFremforingFraTidspunkt ###
################################################### 
def banenor_closed_api_to_df(url, header, S3_filename, argument_list, max_pages=500, page_size=5000, page_number=1, max_iterations=30):
  filename = '/tmp/{}_api_to_df_temp_{:%Y%m%d%H%M%S}_r{}_p'.format(S3_filename, datetime.now(), random.randint(1,9999))
  outputList =[]
  notFound = []
  count = 0  
  url_temp = url
  if not argument_list:
    argument_list = '!'
  
  #page = 1
  totalPages = max_pages
 

  ### Paginerer med en loop som terminerer når det ikke finnes mer data.   

  for ref in argument_list:      
    logprint('parameter : {}'.format(ref))
    #url_arg = url_temp.format(ref)
    url_arg = url_temp + ref
    logprint('url_arg: {}'.format(url_arg))
    page = page_number    
    logprint('page: {}'.format(page))
    #totalPages = 1
    while page and (count <= max_iterations and page <= totalPages and page <= max_pages):
      url = "{}?pageNumber={}&pageSize={}".format(url_arg, page, page_size)
      logprint('url: {}'.format(url))      
      t0 = time.time()
      api_tries_before_fail = 5
      logprint('while-loop page: {}'.format(page))
      for n in range(api_tries_before_fail):
        try:
          response = requests.get(url, headers=header)
          response.raise_for_status()
        except requests.exceptions.HTTPError as errh:
          logprint("Http Error: " + str(errh))      
          #time.sleep(3)
          break
        except requests.exceptions.ConnectTimeout as errct:
            logprint("Connection Timeout Error: " + str(errct))
            time.sleep(10)
            continue
        except requests.exceptions.ReadTimeout as errrt:
            logprint("Read Timeout Error: " + str(errrt))
            time.sleep(10)
            continue
        except requests.exceptions.ConnectionError as errc:
            logprint("Error Connecting: " + str(errc))
            time.sleep(10)
        except requests.exceptions.ChunkedEncodingError as errce:
            logprint("Connection broken, incompleate read: " + str(errce))
            time.sleep(10)
            continue
        except requests.exceptions.RequestException as err:
            logprint("Error: " + str(err))
            time.sleep(10)
            continue
        except:
            logprint("Failed to get contact with API.")
            time.sleep(10)
            continue
        else:
          break       
  
      
      ### Henter ut ordre samt meta-informasjonen (antall sider etc)
      ### elementet til X-Pagination er en tekststreng, så vi omformer den til en dict via json-konvertering.
      ### strip() fjerner fnuttene rundt objektet først.
      try:  
        j_header =  response.headers['X-Pagination'].strip("\'")
        j_header = json.loads(j_header)        
        logprint('Current page: {}'.format(j_header['CurrentPage']))
        logprint('Pagesize: {}'.format(j_header['PageSize']))   
        logprint('Total number of rows: {}'.format(j_header['TotalNumberOfRecords']))
        totalPages = j_header['TotalPages']
        logprint('totalPages: {}'.format(totalPages))
        currentPage = j_header['CurrentPage']
        j_data = json.loads(response.text)
        page = page + 1
        outputList.append(j_data)
        count = count + 1
        logprint('Request for dato: {} Iterasjon nummer: {} av totalt: {}'.format(ref, count, totalPages))
      except Exception as e:
        logprint("Error reading API response!")
        logprint("Response: {}".format(response.text))
        logprint("Error: {}".format(e))
        errorcode = 'url: {}  feilmelding: {}'.format(url,response.text)
        raise Exception(errorcode)             
        continue 

  ### mellomlagrer data på S3
  jslines = '\n'.join([json.dumps(outputObject) for outputObject in outputList])
  dbutils.fs.put("{}.json".format(filename), jslines, True)


  ### leser fra S3, og tilordner dataframe som returneres til kallende instans.
  df = spark.read.json("{}.json".format(filename))
  return df, page, totalPages
