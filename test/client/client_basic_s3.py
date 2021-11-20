import sys
import os
import json

from pathlib import Path

from breaker_core.client.client_basic import ClientBasic
from breaker_core.datasource.jsonqueue import Jsonqueue
from breaker_core.datasource.bytessource_callback import BytessourceCallback

from breaker_aws.datasource.bytessource_s3 import BytessourceS3


if __name__ == '__main__':
    path_file_config_breaker = Path(os.getenv('PATH_FILE_CONFIG_BREAKER', '/config/config.cfg'))

    with open(path_file_config_breaker, 'r') as file:
        config_breaker = json.load(file)

    jsonqueue_request = Jsonqueue.from_dict(config_breaker, config_breaker['queue_request_basic_v1'])
    if not jsonqueue_request.exists():
        jsonqueue_request.create()

    name_region = 'eu-west-1'
    name_bucket = 'breaker-data-0000'
    name_object_source = 'breaker_discord/bot_tes/sound/028.mp3'
    name_object_target = 'breaker_discord/bot_tes/sound/test.mp3'
    url_callback = 'https:://kexxu.com'
  
    client = ClientBasic(jsonqueue_request)
    source = BytessourceS3(config_breaker,  name_region, name_bucket,  name_object_source)
    target = BytessourceS3(config_breaker,  name_region, name_bucket,  name_object_target)
    
    response =  BytessourceCallback(config_breaker, url_callback)
    print(source.exists())
    print(target.exists())
    client.request_copy(source, target, response)
    print(source.exists())
    print(target.exists())