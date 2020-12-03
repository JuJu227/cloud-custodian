""" 
      Streaming module essentially creates an interface for sending json aggredated event data. 
      The entry point is send_event() which takes two parameters. 
      
      supported streaming platforms: 
      kinesis --- Kinesis(AWS) 
                  environment vars:
                         KINESIS_STREAM_NAME=cs-stream 
                         KINESIS_SESSION_TOKEN=arn:aws:kinesis:us-east-2:866506685750:stream/cs-stream
                         KINESIS_REGION=us-east-1  
                       
      kakfa -- Kakfa
                  environment vars:
                         KAKFA_SERVER_URL=127.0.0.1:1234 
                         KAKFA_STREAM_NAME=cs-stream 
      
      ehub -- Event Hub(AZURE)
                environment vars: 
                         EVENT_HUB_CONN_STR=sb://dummynamespace.servicebus.windows.net/;SharedAccessKeyName=DummyAccessKeyName;SharedAccessKey=5dOntTRytoC24opYThisAsit3is2B+OGY1US/fuL3ly= 
                         EVENT_HUB_NAME=cs-stream 
      
                  
      second parameter is the actually data which should be an object that can't be converted to 
      json.

      example: 
           from c7n.streaming import send_event('kinesis', object_that_json-ifable) 

"""
import json
import boto3
import random
import os
from datetime import datetime

from azure.eventhub import EventHubProducerClient, EventData


""" Kinesis class """ 
class KinesisClient(object):
    STREAM_NAME = os.environ['KINESIS_STREAM_NAME']
    AWS_SESSION_TOKEN = os.environ['KINESIS_SESSION_TOKEN'] 
    REGION = os.environ['KINESIS_REGION'] 
    
    def __init__(self, region="us-east-1"):
          self.client = boto3.client(
                                'kinesis',
                                region_name=region,
                                aws_session_token=AWS_SESSION_TOKEN) 
          self.stream_name = STREAM_NAME
     
    def send(self, data):
        try: 
           resp = self.client.put_record(
                                 StreamName=self.stream_name,
                                 Data=json.dumps(data),
                                 PartitionKey=self.generate_key)
           print(json.dumps(resp))
        except Exception as err: 
            print(err)

    def create_stream(self):
         if not live_stream(self):
            try:
               self.client.create_stream(self.stream_name)
            except Exception as err:
               print(err)
     
    def live_stream(self):
         resp = self.client.describe_stream_summary(
                                   StreamName=self.stream_name
         )
         if 'StreamDescriptionSummary' in resp:
             if resp['StreamDescriptionSummary']['StreamStatus'] in ['CREATING', 'ACTIVE']:
                 return true
         return false 
 
    def generate_key(self):
        return str(random.random())


""" Kakfa class """  
class KakfaClient(object):
      SERVER_URL = os.environ['KAKFA_SERVER_URL']  
      STREAM_NAME = os.environ['KAKFA_STREAM_NAME']
      
      def __init__(self, server_url, stream_name, time_out=60):
          self.client = KafkaProducer(bootstrap_servers=SERVER_URL)
          self.stream = STREAM_NAME
          self.time_out = time_out
      def send(self, data);
           try:
              pre = self.client.send(
                                self.stream,
                                json.dumps(data))
              result = pre.get(self.time_out)
              self.client.flush()
              print(result)
           except Exception as err:
              print(err)


""" Event hub class """
class AzureClient(object):
      
      CONNECTION_STR = os.environ['EVENT_HUB_CONN_STR']
      EVENTHUB_NAME = os.environ['EVENT_HUB_NAME']
      
      def __init__(self):
          self.client = EventHubProducerClient.from_connection_string(
                        conn_str=CONNECTION_STR,
                        eventhub_name=EVENTHUB_NAME
          )

      def send(self, data):
          event  = self.client.create_batch()
          event.add(EventData(json.dumps(data))
          try:
             self.client.send_batch(event)
          except Exception as err:
             print(err) 

                    
""" Send_event is the public interface """
def send_event(stream_type, data):
    client = object()
    
    if stream_type is 'kinesis': 
        client = KinesisClient(region) 
    elif stream_type is 'kakfa': 
        client = KakfaClient()
    elif stream_type is 'ehub':
        client = AzureClient()  
    else 
        raise Exception(stream_type +": is an unsupported stream type") 

    client.send(data)
