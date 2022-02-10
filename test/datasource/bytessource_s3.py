import json
import os
import boto3
import requests

from pathlib import Path

from breaker_core.datasource.bytessource import Bytessource
from breaker_aws.config_aws import ConfigAws
from breaker_aws.tools_s3 import ToolsS3
from breaker_aws.datasource.bytessource_s3 import BytessourceS3

if __name__ == '__main__':
    path_file_config_breaker = Path(os.getenv('PATH_FILE_CONFIG_BREAKER_AWS_DEV', '/config/config.cfg'))
    
    with open(path_file_config_breaker, 'r') as file:
        config_breaker = json.load(file)
    print(config_breaker.keys())

    

    aws_name_region = 'eu-west-1'
    name_bucket = 'breaker-data-0000'
    config_breaker['aws_name_bucket_data'] = name_bucket
    config = ConfigAws(config_breaker)

    
    bytessource_root = BytessourceS3(config_breaker, aws_name_region, name_bucket, '', [])

    # write and delete
    bytessource = bytessource_root.join(['result.wav'])
    print(bytessource.exists())
    bytessource.write(bytearray('test'.encode('utf-8')))
    print(bytessource.exists())
    bytes = bytessource.read()
    print(bytes)
    bytessource.delete()
    print(bytessource.exists())



    #
    # verify public read acl
    #
    # dict_bpab = ToolsS3.bucket_public_access_block_load(config.client_s3(), config.resource_s3(), name_bucket)
    # print(dict_bpab)
    ToolsS3.bucket_public_access_block_save(config.client_s3(), config.resource_s3(), name_bucket, 
        BlockPublicAcls=False,
        IgnorePublicAcls=False)
    # policy = ToolsS3.bucket_read_policy(config.client_s3(), config.resource_s3(), name_bucket)
    # print(policy)
    # exit()

    print()
    print('write and delete public')
    bytessource.write(bytearray('test'.encode('utf-8')))
    print(bytessource.exists())
    print(bytessource.is_public_load())
    url_public = bytessource.url_public()
    print(requests.get(url_public).status_code)
    bytessource.is_public_save(True)
    print('should now be public')
    print(bytessource.is_public_load())
    print(requests.get(url_public).status_code)
    bytessource.is_public_save(False)
    print('should now be prive')
    print(requests.get(url_public).status_code)


    bytessource.write(bytearray('test'.encode('utf-8')), )

    # s3_resource = boto3.resource('s3')
    # bucket_policy = s3_resource.BucketPolicy('bucket_name')

    # # Or use client to get Bucket policy
    # s3_client = boto3.client('s3')
    # policy = s3_client.get_bucket_policy(Bucket='bucket_name')

    # # assign policy using s3 resource 
    # user_policy = { "Effect": "Allow",... } 
    # new_policy =  policy['Statement'].append(user_policy)
    # bucket_policy.put(Policy=new_policy) 





#     {
#     "Version": "2012-10-17",
#     "Id": "Policy1601373240685",
#     "Statement": [
#         {
#             "Sid": "Stmt1601373234616",
#             "Effect": "Allow",
#             "Principal": "*",
#             "Action": "s3:GetObject",
#             "Resource": "arn:aws:s3:::semerse-dev-demo/*"
#         }
#     ]
# }