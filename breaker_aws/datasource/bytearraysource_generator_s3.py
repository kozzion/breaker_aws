import boto3
from breaker_aws.tools_s3 import ToolsS3

from breaker_core.datasource.bytearraysource_generator import BytearraysourceGenerator
from breaker_aws.datasource.bytearraysource_s3 import BytearraysourceS3

class BytearraysourceGeneratorS3(BytearraysourceGenerator):

    def __init__(self, aws_name_region, aws_access_key_id, aws_secret_access_key, name_bucket) -> None:
        self.aws_name_region = aws_name_region
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.name_bucket = name_bucket
        client_s3, resource_s3 = ToolsS3.create_client_and_resource_s3(aws_name_region, aws_access_key_id, aws_secret_access_key)

        self.client_s3 = client_s3 
        self.resource_s3 = resource_s3


    def generate(self, list_key:list[str]) -> BytearraysourceS3:
        self.validate_list_key(list_key)
        name_object = '/'.join(list_key)
        return BytearraysourceS3(self.aws_name_region, self.aws_access_key_id, self.aws_secret_access_key, self.name_bucket, name_object)


    def list_for_prefix(self, list_key_prefix:list[str]) -> list[list[str]]:
        self.validate_list_key(list_key_prefix)
        prefix = '/'.join(list_key_prefix)
        list_name_object = ToolsS3.list_name_object_for_prefix(self.client_s3, self.resource_s3, self.name_bucket, prefix)
        list_list_key = []
        for name_object in list_name_object:
            list_list_key.append(name_object.split('/'))
        return list_list_key
       
    def to_dict(self) -> dict:
        dict_bytearraysource = {}
        dict_bytearraysource['type_bytearraysource'] = 'BytearraysourceGeneratorS3'
        dict_bytearraysource['aws_name_region'] = self.aws_name_region
        dict_bytearraysource['aws_access_key_id'] = self.aws_access_key_id
        dict_bytearraysource['aws_secret_access_key'] = self.aws_secret_access_key
        dict_bytearraysource['name_bucket'] = self.name_bucket
        return dict_bytearraysource

    @staticmethod
    def from_dict(dict_bytearraysource) -> BytearraysourceGenerator:
        if not dict_bytearraysource['type_bytearraysource'] == 'BytearraysourceGeneratorS3':
            raise Exception('incorrect_dict_type')
        return BytearraysourceGeneratorS3( 
            dict_bytearraysource['aws_name_region'],
            dict_bytearraysource['aws_access_key_id'],
            dict_bytearraysource['aws_secret_access_key'], 
            dict_bytearraysource['name_bucket'])
