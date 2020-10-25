# Databricks notebook source
### Sender epost med emnefelt og tekst til mottakere oppført i ønsket sns_topic. Håndterer ikke vedlegg. 
### PARAMETRE:
### sns_topic: definert i Secrets med referanse til AWS der epostmottakere er knyttet til aktuell topic.
### message: eposttekst. Kan være None.
### subject: emnefelt. Kan være None.
### environment: angir hvilket miljø funksjonen kalles i. Default=Prod. 
### RETURVERDI:  produserer en epost per kall. 
### EKSEMPEL:
### send_mail(‘SNS_TOPIC_GREENMOBILITY’,’Dette er et automatisk varsel….’,’Det er dubletter i Sharepointliste..’, ‘TEST’)
### UTVIKLER: Jan Eilertsen, 2018-09-17
def send_mail(sns_topic,message=None,subject=None,environment='PROD'):
  session = boto3.session.Session(region_name='eu-central-1')
  snsClient = session.client('sns', config= boto3.session.Config(signature_version='s3v4'))
  response = snsClient.publish(
  TopicArn=env(sns_topic),
  Message=message,
  Subject=subject,
  MessageAttributes={
  'Environment': {
  'DataType': 'String',
  'StringValue': '{}'.format(env('ENVIRONMENT')),
                }
            }
        )
