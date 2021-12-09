import json
import os
from pathlib import Path

from breaker_core.datasource.bytessource import Bytessource
from breaker_core.tools_general import ToolsGeneral

from breaker_aws.tools_amp import ToolsAmp

path_file_config_breaker = Path(os.getenv('PATH_FILE_CONFIG_BREAKER_AWS_DEV', '/config/config.cfg'))
with open(path_file_config_breaker, 'r') as file:
    config_breaker = json.load(file)

name_workspace = 'workspace_amp_test'
client_amp = ToolsAmp.create_client_amp(config_breaker['aws_name_region'], config_breaker['aws_access_key_id'], config_breaker['aws_secret_access_key'])
print(ToolsAmp.workspace_has(client_amp, name_workspace))
if ToolsAmp.workspace_has(client_amp, name_workspace):
    ToolsAmp.workspace_delete_by_name(client_amp, name_workspace)
print(ToolsAmp.workspace_has(client_amp, name_workspace))
workspace = ToolsAmp.workspace_create(client_amp, name_workspace)
print(workspace)
print(ToolsAmp.workspace_has(client_amp, name_workspace))