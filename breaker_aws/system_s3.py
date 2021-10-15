from pickle import TRUE
import sys
import os
import threading

from botocore.exceptions import ClientError

from openeye_model.tools_general import ToolsGeneral as tg

#TODO add feedback option from system instancegroup

class RunnableUpload(object):
    def __init__(self, system_s3, path_file_source, name_bucket_target, name_object_target, overwrite='always'):
        super(RunnableUpload, self).__init__()
        if not overwrite in ['always', 'never', 'ifdifferent']: #TODO add ifnewer, iflarger
            raise Exception('overwrite cannot be: ' + overwrite)
        self.system_s3 = system_s3
        self.path_file_source = path_file_source
        self.name_bucket_target = name_bucket_target
        self.name_object_target = name_object_target
        self.overwrite = overwrite

    def run(self):
        if self.overwrite == 'never':
            if self.system_s3.object_has(self.name_bucket_target, self.name_object_target):
               return
        elif self.overwrite == 'ifdifferent':
            if self.system_s3.object_has_hash_for_file(self.name_bucket_target, self.name_object_target, self.path_file_source):
               return

        self.system_s3.client_s3.upload_file(self.path_file_source, self.name_bucket_target, self.name_object_target)


class RunnableDownload(object):
    def __init__(self, system_s3, path_file_target, name_bucket_source, name_object_source, overwrite='always'):
        super(RunnableDownload, self).__init__()
        if not overwrite in ['always', 'never', 'ifdifferent']: #TODO add ifnewer, iflarger
            raise Exception('overwrite cannot be: ' + overwrite)
        self.system_s3 = system_s3
        self.path_file_target = path_file_target
        self.name_bucket_source = name_bucket_source
        self.name_object_source = name_object_source
        self.overwrite = overwrite

    def run(self):
        if self.overwrite == 'never':
            if os.path.isfile(self.path_file_target):
               return
        elif self.overwrite == 'ifdifferent':
            if self.system_s3.object_has_hash_for_file(self.name_bucket_target, self.name_object_target, self.path_file_source):
               return

        bucket = self.system_s3.resource_s3.Bucket(self.name_bucket_source)
        bucket.download_file(self.name_object_source, self.path_file_target)

class RunnableCopy(object):
    def __init__(self, resource_s3, name_bucket_source, name_object_source, name_bucket_target, name_object_target):
        super(RunnableCopy, self).__init__()
        self.resource_s3 = resource_s3
        self.name_bucket_source = name_bucket_source
        self.name_object_source = name_object_source
        self.name_bucket_target = name_bucket_target
        self.name_object_target = name_object_target
    
    def run(self):
        copy_source = {'Bucket': self.name_bucket_source,'Key': self.name_object_source}
        self.resource_s3.meta.client.copy(copy_source, self.name_bucket_target, self.name_object_target)

class SystemS3(object):    
    def __init__(self, client_s3, resource_s3):
        super(SystemS3, self).__init__()
        self.client_s3 = client_s3
        self.resource_s3 = resource_s3



    def bucket_create(self, name_bucket):
        self.client_s3.create_bucket(Bucket=name_bucket, CreateBucketConfiguration={'LocationConstraint': 'eu-west-1'})
        self.client_s3.put_public_access_block(
            Bucket=name_bucket,
            PublicAccessBlockConfiguration={
                'BlockPublicAcls': True,
                'IgnorePublicAcls': True,
                'BlockPublicPolicy': True,
                'RestrictPublicBuckets': True
            },
        )

    def object_has(self, name_bucket, name_object):
        try:
            self.resource_s3.Object(name_bucket, name_object).load()
        except ClientError as client_error:
            if client_error.response['Error']['Code'] == "404":
                return False
            else:
                # Something else has gone wrong.
                raise Exception('fail')
        else:
            return True
        
    def object_has_hash_for_file(self, name_bucket, name_object, path_file):
        string_hash_md5 = tg.md5_for_file(path_file)
        self.object_has_hash( name_bucket, name_object, string_hash_md5)

    def object_has_hash(self, name_bucket, name_object, string_hash_md5):
        raise Exception('fail')
        # return False

    def object_hash(self, name_bucket, name_object):
        bucket = self.resource_s3.Bucket(name_bucket)
        return bucket.get_key(name_object).etag[1 :-1]

    def bucket_has(self, name_bucket):
        try:
            self.client_s3.head_bucket(Bucket=name_bucket)
            return True
        except ClientError:
            return False

    def bucket_delete(self, name_bucket):
        self.client_s3.delete_bucket(Bucket=name_bucket)

    def copy_for_prefix(self, name_bucket_source, prefix_source, name_bucket_target, prefix_target):
        bucket = self.resource_s3.Bucket(name_bucket_source)
        object_summary_iterator = bucket.objects.filter(
            Delimiter=',',
            EncodingType='url',
            MaxKeys=1000,
            Prefix=prefix_source,
        )
        for object_summary in object_summary_iterator:
            name_object_source = object_summary.key
            name_object_target = prefix_target + object_summary.key[len(prefix_source):]
            print(name_object_source)
            print(name_object_target)
            
            sys.stdout.flush()
            # exit()
            runnable = RunnableCopy(self.resource_s3, name_bucket_source, name_object_source, name_bucket_target, name_object_target)
            threading.Thread(target=runnable.run).start()

    def exists_for_prefix(self, name_bucket, prefix):
        bucket = self.resource_s3.Bucket(name_bucket)
        object_summary_iterator = bucket.objects.filter(
            Delimiter=',',
            EncodingType='url',
            MaxKeys=1,
            Prefix=prefix,
        )
        return 0 < len(list(object_summary_iterator))

    def delete_for_prefix(self, name_bucket, prefix):
        bucket = self.resource_s3.Bucket(name_bucket)
        for obj in bucket.objects.filter(Prefix=prefix):
            self.resource_s3.Object(bucket.name,obj.key).delete()
 
    def list_name_object_for_prefix(self, name_bucket, prefix):
        print(name_bucket)
        print(prefix)
        bucket = self.resource_s3.Bucket(name_bucket)
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


     
    def load_object(self, name_bucket, name_object):
        object = self.resource_s3.Object(name_bucket, name_object)
        object.load()
        return object


    def list_id_for_prefix(self, name_bucket, prefix):
        list_name_object = self.list_name_object_for_prefix(name_bucket, prefix)
        return [name_object.split('/')[-1] for name_object in list_name_object]

    def upload_dir_for_list_path(self, name_bucket, list_path):
        path_dir_source = ''
        prefix = ''
        self.upload_dir_for_prefix(name_bucket, path_dir_source, prefix)




    # def upload_dir_for_prefix(self, name_bucket, path_dir_source, prefix):
    #     print(path_dir_source)
    #     bucket = self.resource_s3.Bucket(name_bucket)
    #     for path_dir_absolute, _, list_name_file in os.walk(path_dir_source):
    #         path_dir_relative = os.path.join(os.path.relpath(path_dir_absolute, path_dir_source))
    #         for name_file in list_name_file:
    #             print('next')
    #             print(path_dir_relative)
    #             if path_dir_relative == '.':
    #                 path_file_source = os.path.join(path_dir_source, name_file)
    #                 name_object = '/'.join([prefix, name_file])
    #             else:
    #                 path_file_source = os.path.join(path_dir_source, path_dir_relative, name_file)
    #                 name_object = '/'.join([prefix, path_dir_relative, name_file])
    #             print(path_file_source)
    #             print(name_object)
    #             bucket.upload_file(path_file_source, name_object)

    def upload_dir_for_prefix(self, path_dir_source, name_bucket_target, prefix_target='', overwrite='always', report_progress=True):

        list_runnable = []
        for path_dir_absolute, _, list_name_file in os.walk(path_dir_source):
            path_dir_relative = os.path.join(os.path.relpath(path_dir_absolute, path_dir_source))
            for name_file in list_name_file:
                print('next')
                if path_dir_relative == '.':
                    path_file_source = os.path.join(path_dir_source, name_file)
                    if prefix_target == '':
                        name_object_target = name_file
                    else:
                        name_object_target = '/'.join([prefix_target, name_file])

                else:
                    path_file_source = os.path.join(path_dir_source, path_dir_relative, name_file)
                    if prefix_target == '':
                        list_path = []
                    else:
                        list_path = [prefix_target]

                    list_path.extend(path_dir_relative.split(os.path.sep))
                    list_path.append(name_file)
                    name_object_target = '/'.join(list_path)
                
                runnable = RunnableUpload(self, path_file_source, name_bucket_target, name_object_target, overwrite)
                list_runnable.append(runnable)
        tg.complete_list_runnable(list_runnable, report_progress=report_progress)
                

            
    def download_dir_for_prefix(self, path_dir_target, name_bucket_source, prefix_source, overwrite='always', report_progress=True):
        
        list_name_object = self.list_name_object_for_prefix(name_bucket_source, prefix_source)
        list_runnable = []
        for name_object in list_name_object:
            name_object_short = name_object[len(prefix_source):]
            list_path_dir_target = [path_dir_target]
            list_path_dir_target.extend(name_object_short.split('/'))
            path_dir_target_file = os.path.sep.join(list_path_dir_target[:-1])
            path_file_target = os.path.sep.join(list_path_dir_target)
                
            #TODO move inside
            if not os.path.isdir(path_dir_target_file):
                os.makedirs(path_dir_target_file)       
            runnable = RunnableDownload(self, path_file_target, name_bucket_source, name_object, overwrite)
            list_runnable.append(runnable)

        tg.complete_list_runnable(list_runnable, report_progress=report_progress)


            