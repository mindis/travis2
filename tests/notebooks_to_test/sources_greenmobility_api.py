# Databricks notebook source
from base64 import b64encode

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
  logprint(environment)
  logprint(url)
  logprint(username)
  logprint(password)
  logprint(api_metode)
  userPass = username + ":" + password
  userPass = bytes(userPass, encoding='utf-8')
  logprint(userPass)
  b64UserPass = b64encode(userPass).decode("ascii")
  logprint(b64UserPass)
  filename = '{}_{}_{:%Y%m%d%H%M%S}_r{}.json'.format(filename, environment, datetime.now(), random.randint(1,9999))
  
  headers = {
        'Authorization': "Basic %s" % b64UserPass,
        'Cache-Control': "no-cache"
        }
  
  ###### Request mot Green Mobilitys API for å hente token
  ###response = requests.request("GET", url+"auth/login", headers=headers)
  ### Request mot Green Mobilitys API for å hente token
  logprint('kall på API for å hente token')
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
  
  logprint('kall på API-metode med token')
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
    logprint('tom fil')
    dbutils.notebook.exit("Tomt datasett fra GM. Programmet terminerer.")
  else:
    logprint('ikke tom fil')    
  ### Workaround. Konkatenerer inn klammeparentes for at python skal kunne parse json  
    response = '['+ response_raw.text + ']' 
      

  ### Skriver til S3 for midlertidig mellomlagring
    dbutils.fs.put(filename, response, True)

  ### Oppretter dataframe og tilordner innhold fra fil, og returnerer til kallende instans
    df = spark.read.json(filename)    
    return(df)
