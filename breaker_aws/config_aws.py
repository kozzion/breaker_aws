import boto3
from pathlib import Path

from breaker_aws.system_s3 import SystemS3

class ConfigAws(object):

    def __init__(self, dict_config_aws)  -> None:
        super().__init__()
        self._aws_name_region = dict_config_aws['aws_name_region']
        self._aws_access_key_id = dict_config_aws['aws_access_key_id']
        self._aws_secret_access_key = dict_config_aws['aws_secret_access_key']
        self._aws_name_bucket_data = dict_config_aws['aws_name_bucket_data']
    

    def system_s3(self) -> 'SystemS3':
        return SystemS3(self.client_s3(), self.resource_s3()) #TODO add bucket_name to system s3

    def client_s3(self):
        return boto3.client(
            's3', 
            region_name=self._aws_name_region, 
            aws_access_key_id=self._aws_access_key_id, 
            aws_secret_access_key=self._aws_secret_access_key)

    def resource_s3(self):
        return boto3.resource(
            's3', 
            region_name=self._aws_name_region, 
            aws_access_key_id=self._aws_access_key_id, 
            aws_secret_access_key=self._aws_secret_access_key)