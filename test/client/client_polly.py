import json
import os
from pathlib import Path

from breaker_core.datasource.bytessource import Bytessource
from breaker_core.tools_general import ToolsGeneral

from breaker_aws.client.client_polly import ClientPolly

path_file_config_breaker = Path(os.getenv('PATH_FILE_CONFIG_BREAKER_AWS_DEV', '/config/config.cfg'))
with open(path_file_config_breaker, 'r') as file:
    config_breaker = json.load(file)

client_polly = ClientPolly(config_breaker)
bytessource_output = Bytessource.from_dict(config_breaker, config_breaker['bytessource_output_voice_synthesizer'])

id_voice = 'Ivy'
language_code_aws = 'en-GB'
text = 'Hi my name is polly the tts bot'
request = {
    text:text,
    id_voice:id_voice,
    language_code_aws:language_code_aws
}

hash_request = ToolsGeneral.sha256_dict_json(request) 
bytessource_output.join(['polly', hash_request])

if not bytessource_output.exists():
    client_polly.generate(text, id_voice, bytessource_output)

os.startfile(bytessource_output.read_tempfile('.mp3'))
