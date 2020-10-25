# Databricks notebook source
### Taken + adapted from https://stackoverflow.com/questions/33842944/check-if-a-key-exists-in-a-bucket-in-s3-using-boto3/34562141

def s3_key_exists(BUCKET,KEY):
  import botocore
  s3 = boto3.resource('s3')

  try:
      s3.Object(BUCKET, KEY).load()
  except botocore.exceptions.ClientError as e:
      if e.response['Error']['Code'] == "404":
          return False
      else:
          # Something else has gone wrong.
          raise
  else:
      return True
