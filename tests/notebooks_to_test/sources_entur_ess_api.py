# Databricks notebook source
import random
import time
import requests
import json
from six.moves.urllib.parse import urlencode

# COMMAND ----------

def get_auth0_header():
  token = get_auth0_token() 
  header = {'Accept': 'application/json', 
          'Content-Type': 'application/json',
          'Authorization': '{}'.format(token)}
  return header

# COMMAND ----------

def get_auth0_token():
  import requests
  arg_environment = env('ENVIRONMENT')
  
  if arg_environment == 'PROD':
    client_secret = dbutils.secrets.get('ENTUR_AUTH0_PROD', 'password')
    client_id = dbutils.secrets.get('ENTUR_AUTH0_PROD', 'client_id')
    audience = 'https://api.entur.io'
    token_url = 'https://partner.entur.org/oauth/token'
    auth_url = 'https://partner.entur.org'
  
  elif arg_environment == 'TEST':
    client_secret = dbutils.secrets.get('ENTUR_AUTH0_STAGING', 'password')
    client_id = dbutils.secrets.get('ENTUR_AUTH0_STAGING', 'client_id')
    audience = 'https://api.staging.entur.io'
    token_url = 'https://partner.staging.entur.org/oauth/token'
    auth_url = 'https://partner.staging.entur.org'

  auth0_payload = {
    'grant_type': 'client_credentials',
    'client_id': client_id,
    'client_secret': client_secret,
    'audience': audience
  }


  auth0_response = requests.post(auth_url + '/oauth/token', json=auth0_payload).json()

  token = auth0_response['token_type'] + ' ' + auth0_response['access_token']
  return token


# COMMAND ----------

def entur_open_api_to_df(reference_list, url):
  filename = '/tmp/json_open_api_to_df_temp_{:%Y%m%d%H%M%S}_r{}_p'.format(datetime.now(), random.randint(1,9999))
  outputList =[]
  notFound = []
  count = 0
  url = url + '{}'
  for ref in reference_list:
    t0 = time.time()
    api_tries_before_fail = 5
    for n in range(api_tries_before_fail):
      try:
        response = requests.get(url.format(ref), headers={'ET-Client-Name': 'NSB-KK'}, timeout = (16, 16))
        response.raise_for_status()
      except requests.exceptions.HTTPError as errh:
        logprint("Http Error: " + str(errh))
        notFound.append(ref)
        #time.sleep(3)
        break
      except requests.exceptions.ConnectTimeout as errct:
        logprint("Connection Timeout Error: " + str(errct))
        time.sleep(5)
        continue
      except requests.exceptions.ReadTimeout as errrt:
        logprint("Read Timeout Error: " + str(errrt))
        time.sleep(5)
        continue
      except requests.exceptions.ConnectionError as errc:
        logprint("Error Connecting: " + str(errc))
        time.sleep(5)
      except requests.exceptions.ChunkedEncodingError as errce:
        logprint("Connection broken, incompleate read: " + str(errce))
        time.sleep(5)
        continue
      except requests.exceptions.RequestException as err:
        logprint("Error: " + str(err))
        time.sleep(5)
        continue
      except:
        logprint("Failed to get contact with API.")
        time.sleep(5)
        continue
      else:
        break

    ### Henter ut ordre samt meta-informasjonen (antall sider etc)
    try:
      j = json.loads(response.text)
      outputList.append(j)
      count = count + 1
      print(count)
      time.sleep(0.3)
      if count % 50 == 0:
        time.sleep(5)
        print('Pause for API')
    except Exception as e:
      logprint("Error reading API response!")
      logprint("Response: {}".format(response.text))
      logprint("Error: {}".format(e))
      continue 
    ##############################################################################################################################################

  jslines = '\n'.join([json.dumps(outputObject) for outputObject in outputList])
  dbutils.fs.put("{}.json".format(filename), jslines, True)

  df = spark.read.json("{}.json".format(filename))
  return df

# COMMAND ----------

################################################################################
### Skrive til ENTUR-API                                                     ###
### url = baseline + endpoint(med eventuelle parametre)                      ###
### data = dictionary med attributter og data for oppdatering                ###
### operation = POST for nye forekomster, PUT for oppdateringer              ###
### funksjonen tar én rad om gangen.                                         ###
### hvis PUT/POST-operasjonen gikk OK, returnerer funksjonen kode 200.       ###
### alle andre returkoder indikerer feil.                                    ###
################################################################################
def entur_closed_df_to_api(url, data, header, operation):
  print('data')
  print(data)
  print('')
  
  for n in range(2):
      try:
        if operation == 'PUT':
          logprint('PUT - updating API')
          response = requests.put(url, data=json.dumps(data), headers=header) 
        elif operation == 'POST':
          logprint('Post - writing new row to API')
          response = requests.post(url, data=json.dumps(data), headers=header) 
        response.raise_for_status()
      except requests.exceptions.HTTPError as errh:
          logprint("Http Error: " + str(errh)) 
          #time.sleep(3)
          break
      except requests.exceptions.ConnectTimeout as errct:
            logprint("Connection Timeout Error: " + str(errct))
            time.sleep(5)
            continue
      except requests.exceptions.ReadTimeout as errrt:
            logprint("Read Timeout Error: " + str(errrt))
            time.sleep(5)
            continue
      except requests.exceptions.ConnectionError as errc:
            logprint("Error Connecting: " + str(errc))
            time.sleep(5)
      except requests.exceptions.ChunkedEncodingError as errce:
            logprint("Connection broken, incompleate read: " + str(errce))
            time.sleep(5)
            continue
      except requests.exceptions.RequestException as err:
            logprint("Error: " + str(err))
            time.sleep(5)
            continue
      except:
            logprint("Failed to get contact with API.")
            time.sleep(5)
            continue
      else:
          break                   
  
  return response.status_code

# COMMAND ----------

#############################################################################################################################################################
### Funksjonen er generisk og myntet på API for auth0-autentisering, og styres av url = baseline+endpoint og eventuelle parametre mot api, samt header.   ###                                            
### S3_filename = navn på fil som lagres på S3.                                                                                                           ###
### header = get_auth0_header() som kalles i samme skript.                                                                                                ###
### url = 'https://api.staging.entur.io/personnelticket/v2/companies/'                                                                                    ###
### argument_list kan være tom, eller innholde argumenter. Når man sender argumenter, bør url være utstyrt med {} der parametre skal inn.                 ###
### max_pages er maksimalt antall pager vi kan hente ut. Kan settes til 1 hvis man bare ønsker å hente ut testdata.                                       ###
### per_page er antall 'items' per page. Kan endres etter hva API-et tillater for å få maksimal ytelse ved requester.                                     ###
### Jan Eilertsen, 20190806                                                                                                                               ###  
### https://api.staging.entur.io/personnelticket/v2/employees?companyId=4&page=1&perPage=5                                                                ###
#############################################################################################################################################################

def entur_closed_api_to_df(url, header, S3_filename, argument_list=None, max_pages=3000, per_page=1000):
  filename = '/tmp/{}_api_to_df_temp_{:%Y%m%d%H%M%S}_r{}_p'.format(S3_filename, datetime.now(), random.randint(1,9999))
  outputList =[]
  notFound = []
  count = 0  
  url_temp = url
  if not argument_list:
    argument_list = '!'
  
  #page = 1
  totalPages = 2

  ### Paginerer med en loop som terminerer når det ikke finnes mer data.   

  for ref in argument_list:      
    logprint('parameter : {}'.format(ref))
    url_arg = url_temp.format(ref)
    page = 1    
    logprint('page: {}'.format(page))
    totalPages = 1
    while page and (page <= totalPages and page <= max_pages):
      if argument_list != '!':
        url = "{}&page={}&perPage={}".format(url_arg, page, per_page)
        logprint('url: {}'.format(url))
      else:
        url = "{}&page={}&perPage={}".format(url_temp, page, per_page)
        logprint('url: {}'.format(url))
      t0 = time.time()
      api_tries_before_fail = 5
      logprint('while-loop page: {}'.format(page))
      for n in range(api_tries_before_fail):
        try:
          response = requests.get(url, headers = header)
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
      try:  
        j = json.loads(response.text)
        totalPages = j['totalPages']
        logprint('totalPages: {}'.format(totalPages))
        totalItems = j['totalItems']
        page = page + 1
        outputList.append(j)
        count = count + 1
        logprint('Iterasjon nummer: {}'.format(count))
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
  return df
