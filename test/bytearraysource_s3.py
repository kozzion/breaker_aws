import json
import boto3

from pathlib import Path
from breaker_aws.tools_s3 import ToolsS3
from breaker_aws.datasource.bytessource_generator_s3 import BytessourceGeneratorS3

path_file_config = Path('config.cfg')
with path_file_config.open('r') as file:
    dict_config = json.load(file)


aws_name_region = dict_config['aws_name_region']
aws_access_key_id = dict_config['aws_access_key_id']
aws_secret_access_key = dict_config['aws_secret_access_key']
name_bucket = dict_config['aws_name_bucket']

generator = BytessourceGeneratorS3(aws_name_region, aws_access_key_id, aws_secret_access_key, name_bucket)
bytearray_source = generator.generate(['result.wav'])
print(bytearray_source.exists())
bytearray_source.save(bytearray('test'.encode('utf-8')))
print(bytearray_source.exists())
bytearray = bytearray_source.load()
print(bytearray)
bytearray_source.delete()
print(bytearray_source.exists())