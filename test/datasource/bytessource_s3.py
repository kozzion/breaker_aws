import json
import os
import boto3
import pprint

from pathlib import Path
from breaker_aws.tools_s3 import ToolsS3
from breaker_aws.datasource.bytessource_s3 import BytessourceS3
from breaker_core.datasource.bytessource import Bytessource

path_file_config_breaker = Path(os.getenv('PATH_FILE_CONFIG_BREAKER_AWS_DEV', '/config/config.cfg'))
with open(path_file_config_breaker, 'r') as file:
    config_breaker = json.load(file)
aws_name_region = 'eu-west-1'
name_bucket = 'breaker-data-0000'

bytessource_root = BytessourceS3(config_breaker, aws_name_region, name_bucket, '', [])

bytessource = bytessource_root.join(['result.wav'])
print(bytessource.exists())
bytessource.write(bytearray('test'.encode('utf-8')))
print(bytessource.exists())
bytes = bytessource.read()
print(bytes)
bytessource.delete()
print(bytessource.exists())

print('is_public')
print(bytessource._is_public)
bytessource.set_public(True)
print(bytessource._is_public)
bytessource.set_public(False)