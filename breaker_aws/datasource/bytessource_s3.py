import os
from typing import List
from breaker_core.datasource.bytessource import Bytessource
from breaker_aws.tools_s3 import ToolsS3

class BytessourceS3(Bytessource):

    def __init__(self, config:dict, aws_name_region, name_bucket, prefix_root, list_key=[]) -> None:
        super().__init__(config)
        self.validate_list_key(list_key)
        self.aws_name_region = aws_name_region

        self.name_bucket = name_bucket
        self.prefix_root = prefix_root
        self.list_key = list_key
        self.name_object = prefix_root + '/'.join(list_key)

        self.aws_access_key_id = self.config['aws_access_key_id']
        self.aws_secret_access_key = self.config['aws_secret_access_key']
        client_s3, resource_s3 = ToolsS3.create_client_and_resource_s3(self.aws_name_region, self.aws_access_key_id, self.aws_secret_access_key)
        self.client_s3 = client_s3 
        self.resource_s3 = resource_s3

    def _prefix(self):
        prefix = self.prefix_root  
        if 0 < len(self.list_key):
            prefix += '/'.join(self.list_key) + '/'
        return prefix

    def url_public(self) -> str:
        location = self.client_s3.get_bucket_location(Bucket=self.name_bucket)['LocationConstraint']
        return "https://s3-%s.amazonaws.com/%s/%s" % (location, self.name_bucket, self.name_object)

    def is_public_save(self, is_public) -> None:
        ToolsS3.object_is_public_save(self.client_s3, self.resource_s3, self.name_bucket, self.name_object, is_public)

    def is_public_load(self) -> bool:
        return ToolsS3.object_is_public_load(self.client_s3, self.resource_s3, self.name_bucket, self.name_object)
        # self._is_public = is_public
        # ToolsS3.object_set_public(self.client_s3, self.resource_s3, self.name_bucket, self.name_object, is_public)


    def exists(self) -> bool:
        return ToolsS3.object_exists(self.client_s3, self.resource_s3, self.name_bucket, self.name_object)

    def list_shallow(self) -> None:
        prefix = self._prefix()
        list_name_object = ToolsS3.list_name_object_for_prefix(self.client_s3, self.resource_s3, self.name_bucket, prefix)
        list_list_key = []
        for name_object in list_name_object:
            postfix = name_object[len(prefix):]
            if not '/' in name_object[len(prefix):]:
                list_list_key.append(postfix.split('/'))
        return list_list_key

    def list_deep(self) -> None:
        prefix = self._prefix()
        list_name_object = ToolsS3.list_name_object_for_prefix(self.client_s3, self.resource_s3, self.name_bucket, prefix)
        list_list_key = []
        for name_object in list_name_object:
            postfix = name_object[len(prefix):]
            list_list_key.append(postfix.split('/'))
        return list_list_key

    def write(self, bytearray_object:bytearray) -> None:
        ToolsS3.object_save(self.client_s3, self.resource_s3, self.name_bucket, self.name_object, bytearray_object)

    def read(self) -> bytearray:
        return ToolsS3.object_load(self.client_s3, self.resource_s3, self.name_bucket, self.name_object)

    def delete(self) -> None:
        ToolsS3.object_delete(self.client_s3, self.resource_s3, self.name_bucket, self.name_object)

    def size(self):
        return ToolsS3.object_size(self.client_s3, self.resource_s3, self.name_bucket, self.name_object)

    def join(self, list_key:List[str]):
        self.validate_list_key(list_key)
        list_key_extended = self.list_key.copy()
        list_key_extended.extend(list_key)
        return BytessourceS3(
            self.config,
            self.aws_name_region, 
            self.name_bucket, 
            self.prefix_root,
            list_key_extended)

    def list_for_prefix(self, list_key_prefix:List[str]) -> 'List[List[str]]':
        self.validate_list_key(list_key_prefix)
        prefix = '/'.join(list_key_prefix)
        list_name_object = ToolsS3.list_name_object_for_prefix(self.client_s3, self.resource_s3, self.name_bucket, prefix)
        list_list_key = []
        for name_object in list_name_object:
            list_list_key.append(name_object.split('/'))
        return list_list_key
       
    def to_dict(self) -> 'dict':
        dict_bytessource = {}
        dict_bytessource['type_bytessource'] = 'BytessourceS3'
        dict_bytessource['aws_name_region'] = self.aws_name_region
        dict_bytessource['name_bucket'] = self.name_bucket
        dict_bytessource['prefix_root'] = self.prefix_root
        dict_bytessource['list_key'] = self.list_key.copy()
        dict_bytessource['is_public'] = self._is_public
        return dict_bytessource

    @staticmethod
    def from_dict(config:dict, dict_bytessource) -> 'BytessourceS3':
        if not dict_bytessource['type_bytessource'] == 'BytessourceS3':
            raise Exception('incorrect_dict_type')
        
        return BytessourceS3( 
            config,
            dict_bytessource['aws_name_region'],
            dict_bytessource['name_bucket'],
            dict_bytessource['prefix_root'],
            dict_bytessource['list_key'])
        