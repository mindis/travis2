# Databricks notebook source
import random
import time
import requests
import json
import urllib
from six.moves.urllib.parse import urlencode

# COMMAND ----------

### Her definerer vi json-schema vi skal benytte til å tolke responsen fra API-et.
### Vi hardkoder for å sikre at vi ikke ved en tilfeldighet får et meldingssett med et annet (mindre) schema. JSON 4 the win...
### Dette ble satt sammen ved å kjøre df.schema på en eksempelordre, men det er gjort ganske mange type-endringer for å få et fornuftig format ut.
### Ved mindre endringer i schema er nok det enkleste bare å manuelt gjøre korreksjoner i schema under.

entur_api_schema = StructType([
    StructField('changed_at',TimestampType(),True),
    StructField('collection_code',StringType(),True),
    StructField('collection_point',StructType([
      StructField('id',StringType(),True),
      StructField('name',StringType(),True)
      ]),True),
    StructField('contact_person',StructType([
      StructField('customer_number',StringType(),True),
      StructField('date_of_birth',TimestampType(),True),
      StructField('ecard',StructType([
        StructField('ecard_number',StringType(),True),
        StructField('ecard_number_16digits',StringType(),True),
        StructField('expiration_date',StringType(),True),
        StructField('operator_id',StringType(),True)
        ]),True),
      StructField('email',StringType(),True),
      StructField('first_name',StringType(),True),
      StructField('id',StringType(),True),
      StructField('postal_address',StructType([
        StructField('address_line_1',StringType(),True),
        StructField('address_line_2',StringType(),True),
        StructField('country_code',StringType(),True),
        StructField('post_code',StringType(),True),
        StructField('town',StringType(),True)
        ]),True),
      StructField('surname',StringType(),True),
      StructField('telephone_number',StringType(),True)
      ]),True),
    StructField('external_reference',StringType(),True),
    StructField('id',StringType(),True),
    StructField('language_preference',StringType(),True),
    StructField('order_events',ArrayType(StructType([
      StructField('channel_id',StringType(),True),
      StructField('channel_name',StringType(),True),
      StructField('clerk_id',StringType(),True),
      StructField('event_time',TimestampType(),True),
      StructField('event_type',StringType(),True),
      StructField('id',StringType(),True),
      StructField('point_of_sale_id',StringType(),True),
      StructField('point_of_sale_name',StringType(),True),
      StructField('retail_device_id',StringType(),True),
      StructField('shift_id',StringType(),True)
      ]),True),True),
    StructField('ordered_by',StructType([
      StructField('customer_number',StringType(),True),
      StructField('date_of_birth',TimestampType(),True),
      StructField('ecard',StructType([
        StructField('ecard_number',StringType(),True),
        StructField('ecard_number_16digits',StringType(),True),
        StructField('expiration_date',StringType(),True),
        StructField('operator_id',StringType(),True)
        ]),True),
      StructField('email',StringType(),True),
      StructField('first_name',StringType(),True),
      StructField('id',StringType(),True),
      StructField('postal_address',StructType([
        StructField('address_line_1',StringType(),True),
        StructField('address_line_2',StringType(),True),
        StructField('country_code',StringType(),True),
        StructField('post_code',StringType(),True),
        StructField('town',StringType(),True)
        ]),True),
      StructField('surname',StringType(),True),
      StructField('telephone_number',StringType(),True)
      ]),True),
    StructField('payments',ArrayType(StructType([
      StructField('betalingtransaksjon_id',StringType(),True),
      StructField('card_number_suffix',StringType(),True),
      StructField('delbetaling_id',StringType(),True),
      StructField('end_of_validity',StringType(),True),
      StructField('id',StringType(),True),
      StructField('payment_agreement',StructType([
        StructField('alias',StringType(),True),
        StructField('changed_at',TimestampType(),True),
        StructField('changed_by',StringType(),True),
        StructField('created_by',StringType(),True),
        StructField('credit_card_valid_through',StringType(),True),
        StructField('customer_number',StringType(),True),
        StructField('expiration_date',StringType(),True),
        StructField('id',StringType(),True),
        StructField('is_default',BooleanType(),True),
        StructField('masked_credit_card_number',StringType(),True),
        StructField('original_created_at',TimestampType(),True),
        StructField('original_id',StringType(),True),
        StructField('payment_method_id',StringType(),True),
        StructField('reference',StringType(),True),
        StructField('status',StringType(),True)
        ]),True),
      StructField('payment_agreement_original_id',StringType(),True),
      StructField('payment_confirmed_at',TimestampType(),True),
      StructField('payment_error_message',StringType(),True),
      StructField('payment_method_id',StringType(),True),
      StructField('payment_method_name',StringType(),True),
      StructField('payment_status',StringType(),True),
      StructField('price',StructType([
        StructField('amount',StringType(),True),
        StructField('currency',StringType(),True),
        StructField('foreign_currency_amount',StringType(),True),
        StructField('refunded_amount',StringType(),True),
        StructField('vat_amount',StringType(),True),
        StructField('vat_rate',StringType(),True),
        StructField('withdrawal_amount',StringType(),True)
        ]),True),
      StructField('rrn',StringType(),True),
      StructField('voucher_id',StringType(),True)
      ]),True),True),
    StructField('sales_transactions',ArrayType(StructType([
      StructField('alternative_transport_code',StringType(),True),
      StructField('arrival',TimestampType(),True),
      StructField('cancelled_at',TimestampType(),True),
      StructField('cancelled_by',StringType(),True),
      StructField('departure',TimestampType(),True),
      StructField('description',StringType(),True),
      StructField('destination',StructType([
        StructField('country_id',StringType(),True),
        StructField('id',StringType(),True),
        StructField('lisa_id',StringType(),True),
        StructField('name',StringType(),True)
        ]),True),
      StructField('discount_id',StringType(),True),
      StructField('discount_name',StringType(),True),
      StructField('fare_price',StructType([
        StructField('amount',StringType(),True),
        StructField('currency',StringType(),True),
        StructField('foreign_currency_amount',StringType(),True),
        StructField('vat_amount',StringType(),True),
        StructField('vat_rate',StringType(),True)
        ]),True),
      StructField('id',StringType(),True),
      StructField('interconnection_code',StringType(),True),
      StructField('is_foreign_vat',BooleanType(),True),
      StructField('nominal_date',TimestampType(),True),
      StructField('operator_id',StringType(),True),
      StructField('origin',StructType([
        StructField('country_id',StringType(),True),
        StructField('id',StringType(),True),
        StructField('lisa_id',StringType(),True),
        StructField('name',StringType(),True)
        ]),True),
      StructField('original_id',StringType(),True),
      StructField('passenger_category_id',StringType(),True),
      StructField('passenger_category_name',StringType(),True),
      StructField('quantity',LongType(),True),
      StructField('relation_sequence_number',LongType(),True),
      StructField('season_ticket',StructType([
        StructField('ecard_configuration_id',StringType(),True),
        StructField('id',StringType(),True),
        StructField('name',StringType(),True),
        StructField('short_name',StringType(),True)
        ]),True),
      StructField('seats',ArrayType(StructType([
        StructField('accommodation_facility_id',StringType(),True),
        StructField('accommodation_facility_name',StringType(),True),
        StructField('car_number',StringType(),True),
        StructField('fare_class',StringType(),True),
        StructField('fare_class_name',StringType(),True),
        StructField('id',StringType(),True),
        StructField('price',StringType(),True),
        StructField('seat_number',StringType(),True),
        StructField('seat_status',StringType(),True),
        StructField('seat_status_name',StringType(),True),
        StructField('type_of_accommodation_facility',StringType(),True),
        StructField('type_of_accommodation_facility_name',StringType(),True)
        ]),True),True),
      StructField('sequence_number',LongType(),True),
      StructField('service_date',TimestampType(),True),
      StructField('service_from_date',StringType(),True),
      StructField('service_to_date',StringType(),True),
      StructField('status',StringType(),True),
      StructField('status_name',StringType(),True),
      StructField('ten',LongType(),True),
      StructField('ticket_group_id',StringType(),True),
      StructField('ticket_group_name',StringType(),True),
      StructField('ticket_service',StringType(),True),
      StructField('train_number',StringType(),True),
      StructField('transfer_from_id',StringType(),True),
      StructField('transfer_from_price',StringType(),True),
      StructField('transfer_from_season_ticket',StructType([
        StructField('ecard_configuration_id',StringType(),True),
        StructField('id',StringType(),True),
        StructField('name',StringType(),True),
        StructField('short_name',StringType(),True)
        ]),True),
      StructField('transfer_to_id',StringType(),True),
      StructField('transfer_to_price',StringType(),True),
      StructField('transfer_to_season_ticket',StructType([
        StructField('ecard_configuration_id',StringType(),True),
        StructField('id',StringType(),True),
        StructField('name',StringType(),True),
        StructField('short_name',StringType(),True)
        ]),True),
      StructField('traveller_customer_number',StringType(),True),
      StructField('traveller_sequence_number',LongType(),True),
      StructField('type_of_sales_transaction',StringType(),True)
      ]),True),True),
    StructField('status',StringType(),True),
    StructField('status_name',StringType(),True),
    StructField('total_price',StructType([
      StructField('amount',StringType(),True),
      StructField('currency',StringType(),True),
      StructField('foreign_currency_amount',StringType(),True),
      StructField('vat_amount',StringType(),True),
      StructField('vat_rate',StringType(),True)
      ]),True)
  ])

# COMMAND ----------

### Metode for å lese et JSON REST-api og returnere en dataframe.
### Paginerer automatisk hvis man får returnert mer enn en side.
### Prøver på nytt noen ganger hvis man får feil under HTTP-tilkobling.
def entur_api_to_df(from_datetime, to_datetime=None, per_page=100, max_pages=None, environment='PROD'):

  ### Setter et unikt temp-filnavn som skrives til med Python og leses fra med Spark.
  ### Dette skal aldri brukes på tvers av sesjoner (og ligger derfor i /tmp/).
  ### Eneste viktige er at det ikke ved en tilfeldighet blir gjenbrukt, 
  ### derfor kombinerer vi datetime for kjøringen og et tilfeldig tall i filnavnet.
  filename = "/tmp_lisa/json_api_to_df_temp_{:%Y%m%d%H%M%S}_r{}_p".format(datetime.now(), random.randint(1,9999))

  parameters = {}
  if to_datetime:   parameters['to_datetime']   = to_datetime
  if from_datetime: parameters['from_datetime'] = from_datetime
  if per_page:      parameters['per_page']      = per_page

  if environment == 'PROD':
    target_url = env('ENTUR_API_URL')
    target_host = env('ENTUR_API_HOST')
  if environment == 'TEST':
    target_url = env('ENTUR_API_URL_TEST')
    target_host = env('ENTUR_API_HOST_TEST')
    
  page = 1
  ### Paginerer med en loop som terminerer når det ikke finnes mer data eller max_pages er nådd.
  while page and (page <= max_pages or max_pages == None):
    
    ### Setter tidspunkt for timing.
    t0 = time.time()
    
    parameters['page'] = page
    url = "{}?{}".format(target_url, urlencode(parameters))

    ### Prøver opptil 5 ganger med en venteperiode på 5 sekunder hvis noe skjer feil under lesingen av APIet (nettverkstrøbbel f.eks.).
    api_tries_before_fail = 5
    for n in range(api_tries_before_fail):
      try:
        ### Spesifiserer timeout for connection (wait for your client to establish a connection) og read (number of seconds that the client will wait between bytes sent from the server)
        ### Fra requests doc:
        ### It’s a good practice to set connect timeouts to slightly larger than a multiple of 3, which is the default TCP packet retransmission window.
        response = requests.get(url, headers={'Host': target_host }, timeout=(16, 16))
        response.raise_for_status()
      except requests.exceptions.HTTPError as errh:
        logprint("Http Error: " + str(errh))
        time.sleep(5)
        continue
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
      orders = j['orders']
      meta = j['meta']
    except Exception as e:
      logprint("Error reading API response!")
      logprint("Response: {}".format(response.text))
      logprint("Error: {}".format(e))
      return
    
    ### Skriver hver ordre som en JSON-linje til fil (såkalt JSON Lines-format, som gjør det lettere å slå sammen 
    ### flere API-resultater, og som Spark leser finfint)
    ### Skriver en fil per API-kall, men med samme prefix som gjør at Spark kan lese de samlet.
    jslines = '\n'.join([json.dumps(order) for order in orders])
    dbutils.fs.put("{}{}.json".format(filename, parameters['page']), jslines, True)
    
    time_spent = int(time.time() - t0)
    max_changed_at = orders[-1]['changed_at'] if orders else ""

    logprint("From: {}. Max changed_at: {}. Page: {}/{}. Orders on current page: {}. Seconds spent: {}.".format(
      from_datetime, max_changed_at, page, j['meta']['total_pages'], len(orders), time_spent))
    
    page = j['meta']['next_page']

  logprint("Reading to DF from file.")

  ### Etter alle resultatene er skrevet til filer leses de inn med *-notasjon i Spark
  ### Bruker fixed schema definert i forrige celle så vi ikke får noen overraskelser, og så vi kan overstyre noen formater.
  ### Bruker PERMISSIVE mode siden eventuelle faktiske endringer i schema uansett vil krasje hvis det påvirker senere transformasjon.
  logprint("Filename is: |{}|".format(filename))
  df = spark.read.json("{}*".format(filename), schema=entur_api_schema, mode="PERMISSIVE")
  
  ### Returnerer dataframen med ordre samt en dict med meta-informasjon (kan ignoreres hvis man ikke ønsker den).
  return df, meta
