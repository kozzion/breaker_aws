import boto3
from botocore.exceptions import ClientError

class ToolsS3:

    @staticmethod
    def create_client_and_resource_s3(name_region, aws_access_key_id, aws_secret_access_key):
        client_s3 = boto3.client(
            's3', 
            region_name=name_region, 
            aws_access_key_id=aws_access_key_id, 
            aws_secret_access_key=aws_secret_access_key)

        resource_s3  = boto3.resource(
            's3', 
            region_name=name_region, 
            aws_access_key_id=aws_access_key_id, 
            aws_secret_access_key=aws_secret_access_key)

        return client_s3, resource_s3

    @staticmethod
    def object_exists(client_s3, resource_s3, name_bucket:str, name_object:str):
        try:
            resource_s3.Object(name_bucket, name_object).load()
        except ClientError as client_error:
            if client_error.response['Error']['Code'] == "404":
                return False
            elif client_error.response['Error']['Code'] == "403":
                raise Exception('Access denied')
            else:
                raise Exception('fail')
        else:
            return True

    @staticmethod
    def object_save(client_s3, resource_s3, name_bucket:str, name_object:str, bytearray_object:bytearray, is_public:bool=False):
        object_s3 = resource_s3.Object(name_bucket, name_object)
        object_s3.put(Body=bytearray_object)
        ToolsS3.object_set_public(client_s3, resource_s3, name_bucket, name_object, is_public)

    @staticmethod      
    def object_set_public(client_s3, resource_s3, name_bucket:str, name_object:str, is_public:bool=False) -> None:
        object_s3_acl = resource_s3.ObjectAcl(name_bucket, name_object)
        if is_public:
            object_s3_acl.put(ACL='public-read')
        else:
            object_s3_acl.put(ACL='private')

    @staticmethod
    def object_load(client_s3, resource_s3, name_bucket:str, name_object:str):
        return resource_s3.Object(name_bucket, name_object).get()['Body'].read()

    @staticmethod
    def object_delete(client_s3, resource_s3, name_bucket:str, name_object:str):
        resource_s3.Object(name_bucket, name_object).delete()

    @staticmethod
    def object_size(client_s3, resource_s3,name_bucket:str, name_object:str):
        return resource_s3.Bucket(name_bucket).Object(name_object).content_length

    @staticmethod
    def list_name_object_for_prefix(client_s3, resource_s3, name_bucket, prefix):
        bucket = resource_s3.Bucket(name_bucket)
        object_summary_iterator = bucket.objects.filter(
            Delimiter=',',
            EncodingType='url',
            MaxKeys=1000,
            Prefix=prefix,
        )
        list_name_object = []
        for object_summary in object_summary_iterator:
            list_name_object.append(object_summary.key)
        
        return list_name_object