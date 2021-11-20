"""Getting Started Example for Python 2.7+/3.3+"""
import os
import sys
import boto3

from botocore.exceptions import BotoCoreError, ClientError
from contextlib import closing

import subprocess
from tempfile import gettempdir

from breaker_core.datasource.bytessource import Bytessource

class ClientPolly(object):

    def __init__(self, config) -> None:
        super().__init__()
        self.config = config
        self.client_polly = boto3.client(
            'polly',
            region_name=self.config['aws_name_region'], 
            aws_access_key_id=self.config['aws_access_key_id'], 
            aws_secret_access_key=self.config['aws_secret_access_key'])


    def list_language_code_aws(self):
        return ['en-GB']
        # list_lexicons
        # self.client_polly.describe_voices(Engine='neural', LanguageCode=language_code_aws)

    def list_id_voice(self, language_code_aws:str):
        self.client_polly.describe_voices(Engine='neural', LanguageCode=language_code_aws)


    def generate(self, text:str, id_voice:str, bytessource_output:Bytessource, id_language='en-GB', id_format='mp3'):
        try:
            response = self.client_polly.synthesize_speech(Text=text, VoiceId=id_voice, LanguageCode=id_language,  OutputFormat=id_format)
        except (BotoCoreError, ClientError) as error:
            raise error

        # Access the audio stream from the response
        if "AudioStream" in response:
            with closing(response["AudioStream"]) as stream:
                bytessource_output.write(stream.read())                
        else:
            raise Exception('No audiostream in result')

