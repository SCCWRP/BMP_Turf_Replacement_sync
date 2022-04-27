import requests, json, os, wget
import pandas as pd
from sqlalchemy import create_engine

class OneDriveClient:
    def __init__(self, tenant_id, client_id, client_secret, token_scope = "https://graph.microsoft.com/.default", token_grant_type = "client_credentials"):
        self.token_endpoint = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.token_scope = token_scope
        self.token_grant_type = token_grant_type
        self.base_api_url = "https://graph.microsoft.com/v1.0/"
        self.download_links = None

        # initialize the token
        request_data = {
            "client_id" : self.client_id,
            "scope": self.token_scope,
            "client_secret" : self.client_secret,
            "grant_type" : self.token_grant_type
        }
        resp = requests.post(self.token_endpoint, data = request_data, headers = {"Content-Type":"application/x-www-form-urlencoded"})
        token = resp.content.decode('utf-8')
        
        self.token = json.loads(token)
        assert self.token.get('access_token') is not None, f"Error with access token. {token}"
        self.requests_header = {"Authorization": f"Bearer {self.token.get('access_token')}"}

    def update_token(self):
        request_data = {
            "client_id" : self.client_id,
            "scope": self.token_scope,
            "client_secret" : self.client_secret,
            "grant_type" : self.token_grant_type
        }
        resp = requests.post(self.token_endpoint, data = request_data, headers = {"Content-Type":"application/x-www-form-urlencoded"})
        token = resp.content.decode('utf-8')
        self.token = json.loads(token)
        return token

    def query(self, shorturl):
        url = f"{str(self.base_api_url)}/{str(shorturl)}"
        print(url)
        resp = requests.get(url, headers = self.requests_header)
        data = json.loads(resp.content.decode('utf-8'))
        if data.get('message') and data.get('message') == 'Access token has expired or is not yet valid.':
            self.update_token()
            assert self.token.get('access_token') is not None, f"Error with access token. {self.token}"
            self.requests_header = {"Authorization": f"Bearer {self.token.get('access_token')}"}
            resp = requests.get(url, headers = self.requests_header)
            data = json.loads(resp.content.decode('utf-8'))
        
        return data
    
    def get_user_id(self, email):
        userid = self.query(f'users/{email}/id').get('value')
        if userid is None:
            print(f"Warning: email address {email} may not be in the directory. This function will return a NoneType object.")
        return userid
    
    def get_user_driveid(self, email):
        data = self.query(f'users/{email}/drive/id').get('value')
        if data is None:
            print(f"Warning: email address {email} may not be in the directory. This function will return a NoneType object.")
        return data
    
    def get_user_folderid(self, email, folderpath):
        qryresult = self.query(f"/drives/{email}/root:/{folderpath}")
        if qryresult.get('error'):
            print(qryresult)
            raise Exception(qryresult.get('error').get('message'))
        return qryresult.get('id')
    
    def list_folder_contents(self, email, folderpath):
        folderid = self.get_user_folderid(email, folderpath)
        contents = self.query(f"/drives/{email}/items/{folderid}/children")
        return contents.get('value')

    def get_download_links(self, email, folderpath, file_ext = '*'):
        '''Doenst work recursively at this time, since theres no need to develop it in such a way at the moment'''
        assert isinstance(file_ext, (list, tuple, str)), "file_ext argument must be a list, tuple or string"
        if isinstance(file_ext, str):
            file_ext = [file_ext]
        
        assert len(file_ext) > 0, "file extension argument is empty"


        links = [
            l for l in self.list_folder_contents(email, folderpath)
            if l.get('@microsoft.graph.downloadUrl')
            and (l.get('name').rsplit('.', 1)[-1] in file_ext if file_ext[0] != '*' else True)
        ]
        self._email = email
        self._folderpath = folderpath
        self.download_links = links
        return links

    def download_data(self, dest_folder = os.getcwd()):
        if self.download_links is None:
            print("First call the get_download_links method to set the download links")
            return
        
        links = self.download_links
        for link in links:
            print(f"Downloading {link.get('name')} to {dest_folder}")
            
            try:
                wget.download(link.get('@microsoft.graph.downloadUrl'), dest_folder)
            except Exception as e:
                print(f"Error downloading {link.get('name')}:\n{e}")
            
            print("done")
        
        




client = OneDriveClient( os.environ.get('SCCWRP_TENANT_ID'),os.environ.get('ONEDRIVE_CLIENT_ID'), os.environ.get('ONEDRIVE_CLIENT_SECRET') )

folderpath = 'Rancho San Diego Turf Data/Data Logger Files'

data = client.download_folder('elizabethfb@sccwrp.org',folderpath, file_ext = ('dat','csv'), dest_folder = os.path.join(os.getcwd(), 'download'))