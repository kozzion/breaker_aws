import os
import json
import time

from pathlib import Path
from breaker_aws.tools_sqs import ToolsSqs
from breaker_aws.jsonqueue_sqs import JsonqueueSqs


path_file_config = Path('config.cfg')
with path_file_config.open('r') as file:
    dict_config = json.load(file)

name_region = dict_config['aws_name_region']
aws_access_key_id = dict_config['aws_access_key_id']
aws_secret_access_key = dict_config['aws_secret_access_key']
name_bucket = dict_config['aws_name_bucket']

client_sqs, resource_sqs = ToolsSqs.create_client_and_resource_sqs(name_region, aws_access_key_id, aws_secret_access_key)
id_queue = 'qu-breakerawstest'

queue = JsonqueueSqs(client_sqs, resource_sqs, id_queue)

if not queue.exists():
    queue.create()
print(queue.exists())


result = queue.dequeue()
print(result)
queue.enqueue({'test':'test0'})
queue.enqueue({'test':'test1'})
queue.enqueue({'test':'test2'})
queue.enqueue({'test':'test3'})
result = queue.dequeue()

while not result == None:
    print(result)
    result = queue.dequeue()
print(result)