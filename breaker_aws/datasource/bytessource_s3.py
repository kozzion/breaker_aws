import os
from breaker_core.datasource.bytessource import Bytessource
from breaker_aws.tools_s3 import ToolsS3

class BytessourceS3(Bytessource):

    def __init__(self, aws_name_region, name_bucket, prefix_root, list_key) -> None:
        self.validate_list_key(list_key)
 
        self.aws_name_region = aws_name_region
        if not 'AWS_ACCESS_KEY_ID' in os.environ:
            raise Exception('missing environment_variable: AWS_ACCESS_KEY_ID')
        else:
            self.aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']

        if not 'AWS_SECRET_ACCESSS_KEY' in os.environ:
            raise Exception('missing environment_variable: AWS_SECRET_ACCESSS_KEY')
        else:
            self.aws_secret_access_key = os.environ['AWS_SECRET_ACCESSS_KEY']
  
 
        self.name_bucket = name_bucket
        self.prefix_root = prefix_root
        self.list_key = list_key
        self.name_object = prefix_root + '/' + '/'.join(list_key)

        client_s3, resource_s3 = ToolsS3.create_client_and_resource_s3(self.aws_name_region, self.aws_access_key_id, self.aws_secret_access_key)
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

    def join(self, list_key:list[str]):
        self.validate_list_key(list_key)
        list_key_extended = self.list_key.copy()
        list_key_extended.extend(list_key)
        return BytessourceS3(
            self.aws_name_region, 
            self.aws_access_key_id, 
            self.aws_secret_access_key, 
            self.name_bucket, 
            self.prefix_root,
            list_key_extended)

    def list_for_prefix(self, list_key_prefix:list[str]) -> list[list[str]]:
        self.validate_list_key(list_key_prefix)
        prefix = '/'.join(list_key_prefix)
        list_name_object = ToolsS3.list_name_object_for_prefix(self.client_s3, self.resource_s3, self.name_bucket, prefix)
        list_list_key = []
        for name_object in list_name_object:
            list_list_key.append(name_object.split('/'))
        return list_list_key
       
    def to_dict(self):
        dict_bytessource = {}
        dict_bytessource['type_bytessource'] = 'BytessourceS3'
        dict_bytessource['aws_name_region'] = self.aws_name_region
        dict_bytessource['name_bucket'] = self.name_bucket
        dict_bytessource['prefix_root'] = self.prefix_root
        dict_bytessource['list_key'] = self.list_key.copy()
        return dict_bytessource

    @staticmethod
    def from_dict(dict_bytessource):
        if not dict_bytessource['type_bytessource'] == 'BytessourceS3':
            raise Exception('incorrect_dict_type')
        return BytessourceS3( 
            dict_bytessource['aws_name_region'],
            dict_bytessource['name_bucket'],
            dict_bytessource['prefix_root'],
            dict_bytessource['list_key'])

