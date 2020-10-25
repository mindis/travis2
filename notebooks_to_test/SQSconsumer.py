# Databricks notebook source
### NEEDS TO BE SIMPLIFIED
class SQSconsumer:
  
  """ 
  1. Usage example: 
  
  consumer = SQSconsumer(username,password,url,region,identity_pool_id,sqs_client_id,user_pool_id)
 
  consumer.create_sqs_consumer()                  # populates the attribute "self.sqs_client" (=instance of boto3.SQS.Client)
  consumer.count_messages_waiting_sqs_consumer()  # populates the attribute "self.number_messages_in_queue"
  consumer.sqs_client.receive_message()           # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Client.receive_message
  
  
  2. Used in notebook: stage/kundemaster_abt_akp_foundation_batch  
    
  
  """
  def __init__(self,username,password,url,region,identity_pool_id,sqs_client_id,user_pool_id):
      self.username = username
      self.password = password
      self.url = url
      self.region = region
      self.identity_pool_id = identity_pool_id
      self.sqs_client_id = sqs_client_id
      self.user_pool_id = user_pool_id      
      
      ### input has to be a list
      write_notebook_source_and_target( [url] ,"source")
      
      self.set_conf()
      
  def set_conf(self):
    self.conf = {}
    dict_conf = self.conf
    dict_conf['username'] = self.username
    dict_conf['password'] = self.password
    dict_conf['sqs-queue-url'] = self.url 
    dict_conf['sqs-region'] = self.region
    dict_conf['identity-pool-id'] = self.identity_pool_id
    dict_conf['sqs-client-id'] = self.sqs_client_id
    dict_conf['user-pool-id'] = self.user_pool_id
    
    import pprint
    pp = pprint.PrettyPrinter(indent=4)
    
    lp('dict_conf')
    lp(dict_conf)
    
  def create_sqs_consumer(self):
    conf = self.conf  
    
    ########################################################################################################
    ### Setter opp Cognito credentials for User Pool
    ### Vi bruker boto3 - cognito-idp som er laget for Cognito - User Pool
    ### Autentifisering skjer her
    ########################################################################################################
    ### Setter i gang tilkoblingen
    client = boto3.client('cognito-idp', region_name=conf["sqs-region"],aws_access_key_id = '', aws_secret_access_key = '')

    ### Autentifisering
    response = client.initiate_auth(
       ClientId = conf['sqs-client-id'],
       AuthFlow='USER_PASSWORD_AUTH',
       AuthParameters={
           'USERNAME': conf["username"],
           'PASSWORD' : conf["password"]
       },
       ClientMetadata={
         'UserPoolId': conf["user-pool-id"]
       }
    )

    ### Lagrer Tokens for å bruke på neste steget
    idToken = response['AuthenticationResult']['IdToken']

    ########################################################################################################  
    ### Cognito - Identity pool
    ### Vi bruker boto3 - cognito-identity som er laget for Cognito - Identity pool
    ### Godskjenning av Cognito credentials skjer her
    ########################################################################################################
    ### Setter i gang tilkoblingen
    client_ci = boto3.client('cognito-identity', region_name = conf["sqs-region"])

    ### Godskjenning med Tokens fra forrige steg
    response_id = client_ci.get_id(
        IdentityPoolId = conf["identity-pool-id"],
        Logins={'cognito-idp.eu-central-1.amazonaws.com/' + str(conf["user-pool-id"]):idToken}
    )

    ### Lagrer credentials til det neste steget
    response_creds = client_ci.get_credentials_for_identity(
        IdentityId = response_id['IdentityId'],
        Logins={'cognito-idp.eu-central-1.amazonaws.com/' + str(conf["user-pool-id"]): idToken}
    )

    ########################################################################################################
    ### Setter opp SQS tilkoblingen
    ### Her lytter man på en melding også laster man ned dataen
    ### Man er nødt til å slette meldingene til slutt av prosessen
    ########################################################################################################
    ### Setter opp tilkoblingen med crdentials fra forrige steg
    self.sqs_client = boto3.client(
    'sqs', 
    region_name = conf["sqs-region"], 
    aws_session_token = response_creds['Credentials']['SessionToken'], 
    aws_access_key_id = response_creds['Credentials']['AccessKeyId'], 
    aws_secret_access_key = response_creds['Credentials']['SecretKey']
    )
    
    
  def count_messages_waiting_sqs_consumer(self):
        
        response = self.sqs_client.get_queue_attributes(
            QueueUrl= self.conf["sqs-queue-url"],
            AttributeNames=['All']
        )
        self.number_messages_in_queue = int(response['Attributes']['ApproximateNumberOfMessages'])
        print("")
        logprint("self.sqs_client: {}: is checking Queue: number_messages_in_queue: {}. ".format(self.sqs_client,self.number_messages_in_queue) ,  newline = False)
              
  
      
  def process_message(self,list_data,count_messages_processed,batch_size,max_number_messages_per_request,wait_time_seconds,delete_in_batches = False):
      try:
            ### get message
            list_message = self.sqs_client.receive_message(
            QueueUrl = self.url,
            AttributeNames = ['SentTimestamp'],
            MaxNumberOfMessages = max_number_messages_per_request,
            MessageAttributeNames = ['All'],
            VisibilityTimeout = 0,
              
            ### this little pause is to avoid: 
            ### TooManyRequestsException: An error occurred (TooManyRequestsException) when calling the InitiateAuth operation: Too many requests  
            WaitTimeSeconds = wait_time_seconds
            )
            
            len_list_messages = len( list_message['Messages'] ) 
            
            if delete_in_batches == True:           
                      response = self.sqs_client.delete_message_batch(
                      QueueUrl=self.url,
                      Entries=[
                          {
                              'Id': 'string',
                              'ReceiptHandle': str_json_receipt_handle
                          },
                      ]
                  )
              
            else:  
                  ###lp("--------------------------------------------")
                  for i in range(0,len_list_messages):                    
                      dict_json_message = list_message['Messages'][i]     
                      
                      if len_list_messages > 1:
                        logprint("Message: {} readed ✓".format(i) , newline=False)
                      else:  
                        logprint("Message readed ✓" , newline=False)
                        
                      str_json_receipt_handle = dict_json_message['ReceiptHandle']
                      
                      print(". Receipt id (last 5 chars): {}".format(  str_json_receipt_handle[-5:] ) )
                      json_body = json.loads( dict_json_message['Body'] )
                      list_data.append(json_body['Message'])       

                      ### delete message            
                      self.sqs_client.delete_message(QueueUrl = self.url, ReceiptHandle = str_json_receipt_handle)  

                         
                      if len_list_messages > 1:
                        logprint("Message {} deleted ✓".format(i) )
                      else:  
                        logprint("Message deleted ✓".format(i) ) 
                  ###lp("--------------------------------------------")
            valid_message = True
      except KeyError as e:
        
            lp( "process_message: KeyError: {}".format(e) )
            lp( "list_message: {}".format(list_message) )
            lp("Discarding the message")
            valid_message = False
            
      
      if len(list_data) == 0:
         valid_message = False
          
      return list_data,count_messages_processed,valid_message


    
