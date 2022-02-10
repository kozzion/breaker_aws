import json
import boto3

class ToolsSqs(object):

    @staticmethod
    def create_client_and_resource_sqs(name_region, aws_access_key_id, aws_secret_access_key):
        client_sqs = boto3.client(
            'sqs', 
            region_name=name_region, 
            aws_access_key_id=aws_access_key_id, 
            aws_secret_access_key=aws_secret_access_key)

        resource_sqs  = boto3.resource(
            'sqs', 
            region_name=name_region, 
            aws_access_key_id=aws_access_key_id, 
            aws_secret_access_key=aws_secret_access_key)

        return client_sqs, resource_sqs

    @staticmethod
    def queue_list_name(client_sqs, resourse_sqs):
        response =  client_sqs.list_queues()
        if not 'QueueUrls' in response:
            return []
        list_name_queue = []
        for url in response['QueueUrls']:
            list_name_queue.append(url.split('/')[-1])
        return list_name_queue

    @staticmethod
    def queue_create(client_sqs, resourse_sqs, id_queue):
        client_sqs.create_queue(QueueName=id_queue)


    @staticmethod
    def queue_exists(client_sqs, resourse_sqs, id_queue):
        try:
            client_sqs.get_queue_url(QueueName=id_queue)
        except client_sqs.exceptions.QueueDoesNotExist:
            return False
        return True

    @staticmethod
    def queue_clear(client_sqs, resourse_sqs, id_queue):
        url_queue = client_sqs.get_queue_url(QueueName=id_queue)['QueueUrl']
        client_sqs.purge_queue(QueueUrl=url_queue)

    @staticmethod
    def queue_delete(client_sqs, resourse_sqs, id_queue):
        url_queue = client_sqs.get_queue_url(QueueName=id_queue)['QueueUrl']
        client_sqs.delete_queue(QueueUrl=url_queue)

    @staticmethod
    def message_recieve(client_sqs, resourse_sqs, id_queue, delete_after_reception=True):
        url_queue = client_sqs.get_queue_url(QueueName=id_queue)['QueueUrl']
        response = client_sqs.receive_message(QueueUrl=url_queue, MaxNumberOfMessages=1) # adjust MaxNumberOfMessages if needed
        if 'Messages' in response:
            ToolsSqs.message_delete(client_sqs, resourse_sqs, id_queue,  response['Messages'][0]['ReceiptHandle'])
            return response['Messages'][0]['Body'], response['Messages'][0]['MessageId'], response['Messages'][0]['ReceiptHandle']
        else:
            return None, None, None

    @staticmethod
    def message_recieve_json(client_sqs, resourse_sqs, id_queue, delete_after_reception=True):
        bytearray_message, id_message, handle_message = ToolsSqs.message_recieve(client_sqs, resourse_sqs, id_queue, delete_after_reception)
        if bytearray_message is None:
            return None
        else: 
            return json.loads(bytearray_message)
            
    @staticmethod  
    def message_delete(client_sqs, resourse_sqs, id_queue, receipt_handle):
        url_queue = client_sqs.get_queue_url(QueueName=id_queue)['QueueUrl']
        client_sqs.delete_message(QueueUrl=url_queue, ReceiptHandle=receipt_handle)
    
    @staticmethod
    def message_send(client_sqs, resourse_sqs, id_queue, bytearray_message):
        url_queue = client_sqs.get_queue_url(QueueName=id_queue)['QueueUrl']
        client_sqs.send_message(QueueUrl=url_queue, MessageBody=bytearray_message)

    @staticmethod
    def message_send_json(client_sqs, resourse_sqs, id_queue, json_message):
        ToolsSqs.message_send(client_sqs, resourse_sqs, id_queue, json.dumps(json_message))
