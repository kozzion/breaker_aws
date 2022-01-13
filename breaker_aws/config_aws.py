import boto3
from pathlib import Path

class ConfigAws(object):

    def __init__(self, dict_config_aws)  -> None:
        super().__init__()
        self._aws_name_region = dict_config_aws['aws_name_region']
        self._aws_access_key_id = dict_config_aws['aws_access_key_id']
        self._aws_secret_access_key = dict_config_aws['aws_secret_access_key']
        self._aws_name_bucket_data = dict_config_aws['aws_name_bucket_data']
        self._path_dir_data = Path(dict_config_aws['path_dir_data'])
    

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