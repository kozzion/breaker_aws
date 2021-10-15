import boto3

from breaker_core.datasource.bytearraysource import Bytearraysource

from breaker_aws.tools_s3 import ToolsS3

class BytearraysourceS3(Bytearraysource):

    def __init__(self, aws_name_region, aws_access_key_id, aws_secret_access_key, name_bucket, name_object) -> None:
        self.aws_name_region = aws_name_region
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.name_bucket = name_bucket
        self.name_object = name_object

        client_s3, resource_s3 = ToolsS3.create_client_and_resource_s3(aws_name_region, aws_access_key_id, aws_secret_access_key)
        self.client_s3 = client_s3 
        self.resource_s3 = resource_s3

    def exists(self) -> bool:
        return ToolsS3.object_exists(self.client_s3, self.resource_s3, self.name_bucket, self.name_object)

    def save(self, bytearray_object:bytearray) -> None:
        ToolsS3.object_save(self.client_s3, self.resource_s3, self.name_bucket, self.name_object, bytearray_object)

    def load(self) -> bytearray:
        return ToolsS3.object_load(self.client_s3, self.resource_s3, self.name_bucket, self.name_object)

    def delete(self) -> None:
        ToolsS3.object_delete(self.client_s3, self.resource_s3, self.name_bucket, self.name_object)
        
    def to_dict(self):
        dict_bytearraysource = {}
        dict_bytearraysource['type_bytearraysource'] = 'BytearraysourceS3'
        dict_bytearraysource['aws_name_region'] = self.aws_name_region
        dict_bytearraysource['aws_access_key_id'] = self.aws_access_key_id
        dict_bytearraysource['aws_secret_access_key'] = self.aws_secret_access_key
        dict_bytearraysource['name_bucket'] = self.name_bucket
        dict_bytearraysource['name_object'] = self.name_object
        return dict_bytearraysource

    @staticmethod
    def from_dict(dict_bytearraysource):
        if not dict_bytearraysource['type_bytearraysource'] == 'BytearraysourceS3':
            raise Exception('incorrect_dict_type')
        return BytearraysourceS3( 
            dict_bytearraysource['aws_name_region'],
            dict_bytearraysource['aws_access_key_id'],
            dict_bytearraysource['aws_secret_access_key'], 
            dict_bytearraysource['name_bucket'],
            dict_bytearraysource['name_object'])

