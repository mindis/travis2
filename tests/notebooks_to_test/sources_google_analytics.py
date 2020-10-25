# Databricks notebook source
# MAGIC %run ../setup

# COMMAND ----------

### denne fungerer. Fjern '#' hvis den må kjøres på nytt
%sh
pip install google-api-python-client==1.5.3

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install apiclient.discovery

# COMMAND ----------

filename = "s3://nsb-da-landing-test/client_secrets.json" 
df_current = spark.read.json(filename) 
display(df_current)

# COMMAND ----------

client_id = '363888307589-d6pr6llv51t966qussbqqn36i07nkl5a.apps.googleusercontent.com'
secret_id = 'O9OzJRGvxrPZbTQdCQCOBAnR'

# COMMAND ----------

###"""Hello Analytics Reporting API V4."""

import argparse

from apiclient.discovery import build
import httplib2
from oauth2client import client
from oauth2client import file
from oauth2client import tools

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
DISCOVERY_URI = ('https://analyticsreporting.googleapis.com/$discovery/rest')
CLIENT_SECRETS_PATH = 's3://nsb-da-landing-test/client_secrets.json' # Path to client_secrets.json file.
VIEW_ID = '<REPLACE_WITH_VIEW_ID>'

# COMMAND ----------

def initialize_analyticsreporting():
  """Initializes the analyticsreporting service object.

  Returns:
    analytics an authorized analyticsreporting service object.
  """
  ### Parse command-line arguments.
  parser = argparse.ArgumentParser(
      formatter_class=argparse.RawDescriptionHelpFormatter,
      parents=[tools.argparser])
  flags = parser.parse_args([])

  ### Set up a Flow object to be used if we need to authenticate.
  flow = client.flow_from_clientsecrets(
      CLIENT_SECRETS_PATH, scope=SCOPES,
      message=tools.message_if_missing(CLIENT_SECRETS_PATH))
  
  logprint('janeil')
  logprint(type(flow))

  ### Prepare credentials, and authorize HTTP object with them.
  ### If the credentials don't exist or are invalid run through the native client
  ### flow. The Storage object will ensure that if successful the good
  ### credentials will get written back to a file.
  storage = file.Storage('analyticsreporting.dat')
  credentials = storage.get()
  if credentials is None or credentials.invalid:
    credentials = tools.run_flow(flow, storage, flags)
  http = credentials.authorize(http=httplib2.Http())

  ### Build the service object.
  analytics = build('analytics', 'v4', http=http, discoveryServiceUrl=DISCOVERY_URI)

  return analytics

# COMMAND ----------

  flow = client.flow_from_clientsecrets(
      CLIENT_SECRETS_PATH, scope=SCOPES,
      message=tools.message_if_missing(CLIENT_SECRETS_PATH))

print(type(flow))

# COMMAND ----------

def get_report(analytics):
  ### Use the Analytics Service Object to query the Analytics Reporting API V4.
  return analytics.reports().batchGet(
      body={
        'reportRequests': [
        {
          'viewId': VIEW_ID,
          'dateRanges': [{'startDate': '7daysAgo', 'endDate': 'today'}],
          'metrics': [{'expression': 'ga:sessions'}]
        }]
      }
  ).execute()

# COMMAND ----------

def print_response(response):
  """Parses and prints the Analytics Reporting API V4 response"""

  for report in response.get('reports', []):
    columnHeader = report.get('columnHeader', {})
    dimensionHeaders = columnHeader.get('dimensions', [])
    metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
    rows = report.get('data', {}).get('rows', [])

    for row in rows:
      dimensions = row.get('dimensions', [])
      dateRangeValues = row.get('metrics', [])

      for header, dimension in zip(dimensionHeaders, dimensions):
        logprint('{}:{}'.format(header,dimension))

      for i, values in enumerate(dateRangeValues):
        logprint('Date range {}'.format(str(i)))
        for metricHeader, value in zip(metricHeaders, values.get('values')):
          logprint('{} {}'.format(metricHeader.get('name'), value))

# COMMAND ----------

def main():
  analytics = initialize_analyticsreporting()
  response = get_report(analytics)
  print_response(response)

# COMMAND ----------

main()

# COMMAND ----------

import base64

# COMMAND ----------

def greenmobility_api_to_df(api_metode='/adyen/all', from_datetime=None, to_datetime=None, max_pages=None, environment='PROD'):  
  ### Lokale variabler
  metode = api_metode
  
  ### Fjerner '/' fra api_metode før tilordning til filname
  filename_list = metode.split('/')  
  filename = ''.join(filename_list)  
  filename = '/tmp/GM-{}'.format(filename)  
  
  #parameters = {}
  #if to_datetime:   parameters['end_time']   = to_datetime
  #if from_datetime: parameters['start_time'] = from_datetime
  
  ### Keyene i env hentes fra Secrets
  if environment == 'PROD':
    url = env('GREENMOBILITY_API_URL_PROD')
    username = env('GREENMOBILITY_API_USERNAME_PROD')
    password = env('GREENMOBILITY_API_PASSWORD_PROD')
  if environment == 'TEST':
    url = env('GREENMOBILITY_API_URL_TEST')
    username = env('GREENMOBILITY_API_USERNAME_TEST')
    password = env('GREENMOBILITY_API_PASSWORD_TEST')      
    
  ### Setter et unikt temp-filnavn som skrives til med Python og leses fra med Spark.
  ### Dette skal aldri brukes på tvers av sesjoner (og ligger derfor i /tmp/).
  ### Eneste viktige er at det ikke ved en tilfeldighet blir gjenbrukt, 
  ### derfor kombinerer vi datetime for kjøringen og et tilfeldig tall i filnavnet.
  print(environment)
  print(url)
  print(username)
  print(password)
  print(api_metode)
  filename = '{}_{}_{:%Y%m%d%H%M%S}_r{}.json'.format(filename, environment, datetime.now(), random.randint(1,9999))
  
  headers = {
        'Authorization': "Basic " + base64.standard_b64encode((username + ":" + password).encode("utf-8")),
        'Cache-Control': "no-cache"
        }
  
  
  ###### Request mot Green Mobilitys API for å hente token
  ###response = requests.request("GET", url+"auth/login", headers=headers)
  ### Request mot Green Mobilitys API for å hente token
  print('kall på API for å hente token')
  api_tries_before_fail = 5
  for n in range(api_tries_before_fail):
    try:
      response = requests.request("GET", url+"auth/login", headers=headers)      
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
  
  token = json.loads(response.text)["token"]
  headers.update({"Authorization" : "Bearer " + token})
  
  ### Request mot Green Mobilitys API for å eksekvere ønsket metode. Der datoparametre er relevant, legges de til headers. Hvis headers har tomme verdier, vil
  ### de andre requestene som ikke er parmeterisert likevel fungere.
  headers['start_time'] = from_datetime
  headers['end_time']   = to_datetime
  
  print('kall på API-metode med token')
  ### Prøver opptil 5 ganger med en venteperiode på 5 sekunder hvis noe skjer feil under lesingen av APIet (nettverkstrøbbel f.eks.).
  api_tries_before_fail = 5
  for n in range(api_tries_before_fail):
    try:
      response_raw = requests.request("GET", url+metode, headers=headers)
      response_raw.raise_for_status()
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
  

  ### Manglende resultatsett kan være en normalsituasjon, men det får jobben til å feile. Dette ønskes unngått ved testen under.      
  if response_raw.text == '':
    print('tom fil')
    dbutils.notebook.exit("Tomt datasett fra GM. Programmet terminerer.")
  else:
    print('ikke tom fil')    
  ### Workaround. Konkatenerer inn klammeparentes for at python skal kunne parse json  
    response = '['+ response_raw.text + ']' 
      

  ### Skriver til S3 for midlertidig mellomlagring
    dbutils.fs.put(filename, response, True)

  ### Oppretter dataframe og tilordner innhold fra fil, og returnerer til kallende instans
    df = spark.read.json(filename)    
    return(df)
