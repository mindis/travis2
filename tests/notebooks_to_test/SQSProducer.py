# Databricks notebook source
import boto3

# COMMAND ----------

AWS_SQS_SERVICE_NAME = 'sqs'


class SqsWriteQueue:
    def __init__(self, url):
        self.sqs_client = boto3.client(service_name=AWS_SQS_SERVICE_NAME, region_name='eu-central-1')
        #self.output_queue_url = env('sms_service_output_queue_name')
        self.output_queue_url=url
    def write_response(self, json_string):
        self.sqs_client.send_message(QueueUrl=self.output_queue_url, MessageBody=json_string)

    def is_connected(self):
        return self.sqs_client.get_queue_attributes(QueueUrl=self.output_queue_url) is not None
