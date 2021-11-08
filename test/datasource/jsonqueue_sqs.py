import os
import json
import time

from pathlib import Path
from breaker_aws.tools_sqs import ToolsSqs
from breaker_aws.datasource.jsonqueue_sqs import JsonqueueSqs


path_file_config_breaker = Path(os.getenv('PATH_FILE_CONFIG_BREAKER_AWS_DEV', '/config/config.cfg'))
with open(path_file_config_breaker, 'r') as file:
    config_breaker = json.load(file)
aws_name_region = 'eu-west-1'
name_bucket = 'breaker-data-0000'
id_queue = 'qu-breakerawstest'

queue = JsonqueueSqs(config_breaker, aws_name_region, id_queue)

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