# Databricks notebook source
### Funksjonen henter data fra b2c-API'et: https://b2c-api-testing-nettbuss.tsolutions.co, skriver til en json fil og returnerer en dataframe.

def turnit_b2c_api_to_df(url_par):
    logprint("Reading from b2c api to a json file.")
    
    filename = '/tmp/json_api_to_df_temp_{:%Y%m%d%H%M%S}_r{}_p'.format(datetime.now(), random.randint(1,9999))

    access_token = 'O5B8H6cga8wQbboTi11eAw8BEUBM70qJxffDyb-fzJ7mvSc2tF5OjdjpJ5N-D27xggsTuhdTCQW37yVwg7ZHk8YsoBeE3ZEVgvqehA_xrmd1AH5jiSbYvpXMRKOx_algMgY1zR4T6azmttPGunDzojusscG3ZfqL4mZpE1K1jINsHuEOAUj3RxZkAppqV243SLr5V5Mli83yBq4Jgml2HwacR4fTtD4EBqhwo9m1yUYbO80I1ZWvkXjxyqeugA6YHCjTwm-EtmbqePnVIKUjpD1zZhinD7J8t2WZDTNYLjLW4mQvw3v_nqOh2IW2oyTZi00QL-9h9Qw8Sip2_4mLq9P4P0cEfQVJMrgjyJb-pDwAZjk2mW26odZypF-bPfTR1ylq9iqZl7Ry52NvedG3wA'
  
    turnit_api_base_url = 'https://b2c-api-testing-nettbuss.tsolutions.co/'

    headers = {'Content-Type': 'application/json',
           'Authorization': 'bearer {0}'.format(access_token)}

    url = '{0}{1}'.format(turnit_api_base_url, url_par)
    print(url)
    temp = requests.get(url, headers=headers)
    
    response =  temp.content
    
    ### Leser fra den genererte json-filen til en dataframe
    logprint("Reading to DF from json file.")

    dbutils.fs.put('{}nettbuss_test.json'.format(filename), response)
    
    df = spark.read.json('{}nettbuss_test.json'.format(filename))
    
    return df

# COMMAND ----------

### Funksjonen henter Nettbuss historiske data fra S3, lagrer dataen i en dataframe, (leser til Redshift stage), og returnerer df for videre sammenslåing til en stor tabell. 
### NB: utf-8 tolker norske bokstaver fra .csv-filer.

def nettbuss_historical_S3_to_df(name, year, linenr):
    logprint("Reading from S3 to a df.")
    
    filelocation = '/manual/Nettbuss/CSV_NETTBUSS_BUSSLINJER_MND_RAPPORT/'
    
    filename = '{0}{1}'.format(filelocation, name)
    
    df = spark.read.csv(path='s3a://' + env('S3_DATA_BUCKET') + filename, header=True, sep=';', encoding='utf-8')
    
    return df

# COMMAND ----------

### NB: utf-8 tolker norske bokstaver fra .csv-filer.

def read_from_S3_to_df(filelocation, name):
    logprint("Reading from S3 to a df.")
    
    filename = '{0}{1}'.format(filelocation, name)
    
    df = spark.read.csv(path='s3a://' + env('S3_DATA_BUCKET') + filename, header=True, sep=',', encoding = 'utf-8') #  encoding = 'iso-8859-1') # encoding='Windows-1252')
    
    return df
