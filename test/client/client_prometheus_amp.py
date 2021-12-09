import json
from pathlib import Path
from breaker_aws.client.client_prometheus_amp import ClientPrometheusAmp

from breaker_aws.tools_amp import ToolsAmp

path_file_config_breaker = Path(os.getenv('PATH_FILE_CONFIG_BREAKER_AWS_DEV', '/config/config.cfg'))
with open(path_file_config_breaker, 'r') as file:
    config_breaker = json.load(file)

name_workspace = 'workspace_amp_test'

client_prometheus = ClientPrometheusAmp(config_breaker, name_workspace)