import os
import json
import boto3
import pprint

from pathlib import Path
from breaker_aws.tools_s3 import ToolsS3
from breaker_aws.datasource.bytessource_s3 import BytessourceS3
from breaker_core.datasource.bytessource_file import BytessourceFile
from breaker_core.datasource.bytessource import Bytessource

aws_name_region = 'eu-west-1'
name_bucket = 'breaker-data-0000'
list_key_prefix = ['breaker_discord', 'bot_dev']
bytessource_s3 = BytessourceS3(aws_name_region, name_bucket, '', list_key_prefix)
bytessource_file = BytessourceFile(Path(os.environ['PATH_DIR_DATA_BREAKER']), list_key_prefix)



bytessource_file.sync_to(bytessource_s3)


list_list_key = bytessource_file.list_deep()
dict = Bytessource.list_list_key_to_dict_hierarchy(list_list_key)
pprint.pprint(dict)
list_list_key = bytessource_s3.list_deep()
dict = Bytessource.list_list_key_to_dict_hierarchy(list_list_key)
pprint.pprint(dict)
print(dict.keys())