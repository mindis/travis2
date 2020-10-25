# Databricks notebook source
### Pga oppdatering av avro-python3 til 1.9.2 den 12.02.2020, fungerte ikke confluent-kafka som den skulle. Setter derfor versjonen til 1.9.1
dbutils.library.installPyPI('avro-python3', '1.9.1')
dbutils.library.installPyPI('confluent-kafka', extras='avro')

import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
from confluent_kafka import KafkaError
from confluent_kafka import Consumer
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
import json

# COMMAND ----------

def create_kafka_consumer(group_id = 'nsb-salgsdvh', autoCommit = False, environment = 'None'):
  username = 'nsb'
  if environment == 'PROD':
    password = dbutils.secrets.get('KAFKA_ENTUR_PROD', 'password')
    config = {
      'bootstrap.servers': 'bootstrap.prod-ext.kafka.entur.io:9095',
      'group.id': group_id,
      'enable.auto.commit': autoCommit,
      'auto.offset.reset': 'earliest',
      'schema.registry.url': 'http://schema-registry.prod-ext.kafka.entur.io:8001',
      'security.protocol': 'SASL_SSL',
      'sasl.mechanism': 'SCRAM-SHA-512',
      'sasl.username': username,
      'sasl.password': password
    }
    
  elif environment == 'TEST':
    password = dbutils.secrets.get('KAFKA_ENTUR', 'password')
    config = {
      'bootstrap.servers': 'bootstrap.test-ext.kafka.entur.io:9095',
      'group.id': group_id,
      'enable.auto.commit': autoCommit,
      'auto.offset.reset': 'earliest',
      'schema.registry.url': 'http://schema-registry.test-ext.kafka.entur.io:8001',  
      'security.protocol': 'SASL_SSL',
      'sasl.mechanism': 'SCRAM-SHA-512',
      'sasl.username': username,
      'sasl.password': password
    }
  c = AvroConsumer(config)

  return c

# COMMAND ----------

## Nullstiller offset til 1 slik at vi kan lese data fra starten. Kun for debug, fjernes i prod. Da må man sette on_assign = reset_offset. Eksempel: c.subscribe(['topic', on_assign = reset_offset])
def reset_offset (consumer, partitions):
  for p in partitions:
    p.offset = -2
  consumer.assign(partitions)

# COMMAND ----------

def entur_kafka_to_df(c):
  count = 0
  messages = []

  while True:
      try:
          msg = c.poll(10)

      except SerializerError as e:
          print("Message deserialization failed for {}: {}".format(msg, e))
          break

      if msg is None:
          print('Message is None; continuing!')
          continue

      if msg.error():
          if msg.error().code() == KafkaError._PARTITION_EOF:
              print('No new messages; continuing!')
              continue
          else:
              print(msg.error())
              break
            
      if (count == 0):
        messages = [msg]
        count += 1
      else:
        messages.append(msg)
        count += 1

      logprint("Topic of message %d is %s and the ID is %s" % (count, msg.topic(), msg.key()))

  c.close()
  for i in messages:
    print(i.key())

  return(messages)
  

# COMMAND ----------

### Metode for å lese JSON verdier fra kafka kø og returnere en dataframe
###Metoden tar inn en liste med JSON objekter
### Spark klarer ikke lese json fra en list, må konvertere til JSON lines med ett JSON objekt per linje
def entur_kafka_to_df(messages):
  ### Gjøre om listen til en dataframe ved å skrive til S3, og deretter lese igjen som en dataframe
  ### Skriver til fil i S3, tar hvert JSON object i listen og skriver det om til individuelle JSON linjer da Spark ikke takler JSON objekter i et array/list 
  jslines = '\n'.join([json.dumps(msg) for msg in messages])
  
  logprint("Reading to DF from file.")

  ### Generere en unik filnavn som lagres i S3, skrives til med Python - leses fra med Spark
  filename = '/tmp/json_kafka_to_df_temp_{:%Y%m%d%H%M%S}_r{}_p'.format(datetime.now(),random.randint(1,9999))
  dbutils.fs.put("{}.json".format(filename), jslines, True)
  df = spark.read.json("{}.json".format(filename))
  return df
