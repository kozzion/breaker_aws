
import time
import os

from breaker_core.datasource.jsonqueue import Jsonqueue
from breaker_aws.tools_sqs import ToolsSqs

class JsonqueueSqs(Jsonqueue):

    def __init__(self, config:dict, aws_name_region:str, id_queue:str) -> None:
        super().__init__(config)
        self.aws_name_region = aws_name_region
        self.id_queue = id_queue

        self.aws_access_key_id = self.config['aws_access_key_id']
        self.aws_secret_access_key = self.config['aws_secret_access_key']

        client_sqs, resource_sqs = ToolsSqs.create_client_and_resource_sqs(self.aws_name_region, self.aws_access_key_id, self.aws_secret_access_key)
        self.client_sqs = client_sqs 
        self.resource_sqs = resource_sqs

    def exists(self) -> bool:
        return ToolsSqs.queue_exists(self.client_sqs, self.resource_sqs, self.id_queue)

    def create(self) -> None:
        return ToolsSqs.queue_create(self.client_sqs, self.resource_sqs, self.id_queue)
    
    def delete(self) -> None:
        return ToolsSqs.queue_delete(self.client_sqs, self.resource_sqs, self.id_queue)

    def clear(self) -> None:
        return ToolsSqs.queue_clear(self.client_sqs, self.resource_sqs, self.id_queue)

    def dequeue(self) -> dict:
        return ToolsSqs.message_recieve_json(self.client_sqs, self.resource_sqs, self.id_queue)
         
    #TODO move to parent? aws specific awaiters exist
    def dequeue_blocking(self, timeout_ms:int=-1, *, sleep_increment_ms:int=10) -> dict:
        while True:
            dict_json = self.dequeue(self)
            if not dict_json is None:
                return dict_json
            elif timeout_ms == -1:
                continue
            elif timeout_ms == 0:
                return None
            elif (timeout_ms <= sleep_increment_ms):
                timeout_ms = 0
                time.sleep(timeout_ms)
            else:
                timeout_ms -= sleep_increment_ms
                time.sleep(sleep_increment_ms)

    def enqueue(self, dict_json:dict) -> None:
        return ToolsSqs.message_send_json(self.client_sqs, self.resource_sqs, self.id_queue, dict_json)


    def to_dict(self) -> 'dict':
        dict_bytessource = {}
        dict_bytessource['type_jsonqueue'] = 'JsonqueueSqs'
        dict_bytessource['aws_name_region'] = self.aws_name_region
        dict_bytessource['id_queue'] = self.id_queue
        return dict_bytessource

    @staticmethod
    def from_dict(config:dict, dict_bytessource) -> 'JsonqueueSqs' :
        if not dict_bytessource['type_jsonqueue'] == 'JsonqueueSqs':
            raise Exception('incorrect_dict_type')
        return JsonqueueSqs( 
            config,
            dict_bytessource['aws_name_region'],
            dict_bytessource['id_queue'])
