
import time
import os

from breaker_core.datasource.jsonqueue import Jsonqueue
from breaker_aws.tools_sqs import ToolsSqs

class JsonqueueSqs(Jsonqueue):

    def __init__(self, config:dict, id_queue:str) -> None:
        super().__init__(config)
        self.id_queue = id_queue

        self.aws_name_region = self.config['aws_name_region']
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
        

    def enqueue(self, dict_json:dict) -> None:
        return ToolsSqs.message_send_json(self.client_sqs, self.resource_sqs, self.id_queue, dict_json)


    def to_dict(self) -> 'dict':
        dict_bytessource = {}
        dict_bytessource['type_jsonqueue'] = 'JsonqueueSqs'
        dict_bytessource['id_queue'] = self.id_queue
        return dict_bytessource

    @staticmethod
    def from_dict(config:dict, dict_bytessource) -> 'JsonqueueSqs' :
        if not dict_bytessource['type_jsonqueue'] == 'JsonqueueSqs':
            raise Exception('incorrect_dict_type')
        return JsonqueueSqs( 
            config,
            dict_bytessource['id_queue'])
