
import time
import os
from breaker_aws.datasource.bytessource_s3 import BytessourceS3

from breaker_core.datasource.jsonqueue import Jsonqueue
from breaker_aws.tools_s3 import ToolsS3

class JsonqueueS3(Jsonqueue):

    def __init__(self, config:dict, aws_name_region:str, name_bucket:str, prefix_root:str) -> None:
        super().__init__(config)
        self.aws_name_region = aws_name_region
        self.name_bucket = name_bucket
        self.prefix_root = prefix_root
        self.bytessource = BytessourceS3(config, aws_name_region, name_bucket, prefix_root)

        self.aws_name_region = self.config['aws_name_region']
        self.aws_access_key_id = self.config['aws_access_key_id']
        self.aws_secret_access_key = self.config['aws_secret_access_key']

        client_sqs, resource_sqs = ToolsS3.create_client_and_resource_s3(self.aws_name_region, self.aws_access_key_id, self.aws_secret_access_key)
        self.client_sqs = client_sqs 
        self.resource_sqs = resource_sqs

    def create(self) -> None:
        pass

    def exists(self) -> bool:
        return True

    def delete(self) -> None:
        pass   

    def clear(self) -> None:
        return ToolsS3.object_delete.queue_clear(self.client_sqs, self.resource_sqs, self.id_queue)

    def dequeue(self) -> dict:
        list_list_key = self.bytessource.list_shallow() 
        if 0 < len(list_list_key):
            bytessource_item = self.bytessource.join(list_list_key[0])
            dict_json = bytessource_item.read_json()
            bytessource_item.delete()
            return dict_json

    #TODO move to parent? aws specific awaiters exist
    def dequeue_blocking(self, timeout_ms:int=-1, *, sleep_increment_ms:int=5000) -> dict:
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
        return self.bytessource.join([str(int(time.time() * 1000))]).write_json(dict_json)


    def to_dict(self) -> 'dict':
        dict_jsonqueue = {}
        dict_jsonqueue['type_jsonqueue'] = 'JsonqueueS3'
        dict_jsonqueue['aws_name_region'] = self.aws_name_region
        dict_jsonqueue['name_bucket'] = self.name_bucket
        dict_jsonqueue['prefix_root'] = self.prefix_root
        return dict_jsonqueue

    @staticmethod
    def from_dict(config:dict, dict_jsonqueue) -> 'JsonqueueS3' :
        if not dict_jsonqueue['type_jsonqueue'] == 'JsonqueueS3':
            raise Exception('incorrect_dict_type')
        return JsonqueueS3( 
            config,
            dict_jsonqueue['aws_name_region'],
            dict_jsonqueue['name_bucket'],
            dict_jsonqueue['prefix_root'])
