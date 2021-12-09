import boto3

class ToolsAmp():

    @staticmethod
    def create_client_amp(name_region, aws_access_key_id, aws_secret_access_key):
        client_amp = boto3.client(
            'amp', 
            region_name=name_region, 
            aws_access_key_id=aws_access_key_id, 
            aws_secret_access_key=aws_secret_access_key)
        return client_amp



    @staticmethod
    def workspace_has(client_amp, name_workspace:str):
        list_workspace = ToolsAmp.list_workspace(client_amp, name_workspace)
        return len(list_workspace) == 1

    @staticmethod
    def workspace_create(client_amp, name_workspace:str, await_complete:bool=True):
        if ToolsAmp.workspace_has(client_amp, name_workspace):
            raise Exception('Workspace with name ' + name_workspace + ' exists')
        workspace = client_amp.create_workspace(
            alias=name_workspace
        )
        id_workspace = workspace['workspaceId']
        if await_complete and workspace['status']['statusCode'] != 'ACTIVE':
            waiter = client_amp.get_waiter('workspace_active')
            waiter.wait(workspaceId=id_workspace)
        return ToolsAmp.workspace_load_by_id(client_amp, id_workspace)

    @staticmethod
    def workspace_load_by_name(client_amp, name_workspace:str):
        list_workspace = ToolsAmp.list_workspace(client_amp, name_workspace)
        if len(list_workspace) != 1:
            raise Exception('no uniqe workspace for name ' + name_workspace)
        return list_workspace[0]

    @staticmethod
    def workspace_load_by_id(client_amp, id_workspace:str):
        return client_amp.describe_workspace(workspaceId=id_workspace)['workspace']

    @staticmethod
    def workspace_delete_by_name(client_amp, name_workspace:str, await_complete:bool=True):
        workspace = ToolsAmp.workspace_load_by_name(client_amp, name_workspace)
        id_workspace = workspace['workspaceId']
        client_amp.delete_workspace(workspaceId=id_workspace)
        if await_complete:
            waiter = client_amp.get_waiter('workspace_deleted')
            waiter.wait(workspaceId=id_workspace)

    @staticmethod
    def list_workspace(client_amp, prefix:str='', dict_tag:dict={}):
        #TODO paginate if more than a 100 workspaces are returned
        reponse = client_amp.list_workspaces(alias=prefix)
        list_workspace = []
        for workspace in reponse['workspaces']:
            for tag in dict_tag:
                if not tag in workspace['tags']:
                    continue
                if not tag in workspace['tags'][tag] == dict_tag[tag]:
                    continue
            list_workspace.append(workspace)
        return list_workspace