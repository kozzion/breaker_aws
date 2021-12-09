import prometheus_client
from breaker_aws.tools_amp import ToolsAmp


class ClientPrometheusAmp(object):

    def __init__(self, config, name_workspace) -> None:
        super().__init__()
        self.config = config
        self.name_workspace = name_workspace

        self.aws_name_region = self.config['aws_name_region']
        self.aws_access_key_id = self.config['aws_access_key_id']
        self.aws_secret_access_key = self.config['aws_secret_access_key']

        self.client_amp = ToolsAmp.create_client_amp(self.aws_name_region, self.aws_access_key_id, self.aws_secret_access_key)
        if not ToolsAmp.workspace_has(name_workspace):
            ToolsAmp.workspace_create(name_workspace)

        url_prometheus_endpoint = ToolsAmp.workspace_load_by_name(name_workspace)['prometheusEndpoint']


    def push_value(self, value, table):
        pass

    @staticmethod
    def from_dict(config:dict, dict_client_prometheus) -> 'ClientPrometheusAmp' :
        if not dict_client_prometheus['type_client_prometheus'] == 'ClientPrometheusAmp':
            raise Exception('incorrect_dict_type')
        return ClientPrometheusAmp( 
            config,
            dict_client_prometheus['name_workspace'])
