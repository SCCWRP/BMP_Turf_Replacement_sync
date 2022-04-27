import os, requests
from client import OneDriveClient
from shareplum import Site
from shareplum import Office365
from shareplum.site import Version


onedriveclient = OneDriveClient( os.environ.get('SCCWRP_TENANT_ID'), os.environ.get('ONEDRIVE_CLIENT_ID'), os.environ.get('ONEDRIVE_CLIENT_SECRET') )

username = os.environ.get('MS_USERNAME')
password = os.environ.get('MS_PASSWORD')
url = os.environ.get('SHAREPOINT_SITE_URL')
teamname = 'SanDiegoCountyBMPMonitoring'
sitefolder = 'Shared%20Documents/Turf%20Replacement/Data/Raw'

authcookie = Office365(url, username=username, password=password).GetCookies()
site = Site(os.path.join(url, 'sites', teamname), version=Version.v2016, authcookie=authcookie)
folder = site.Folder(sitefolder)

# cant have the period in front of the extension, for now until i fix the class to not be sensitive to stuff like that
links = onedriveclient.get_download_links('elizabethfb@sccwrp.org','Rancho San Diego Turf Data/Data Logger Files', file_ext = 'dat')

links = [
    l for l in links if l.get('name') not in [f.get('Name') for f in folder.files]
]

for link in links:
    url = link.get('@microsoft.graph.downloadUrl')
    resp = requests.get(url)
    assert resp.headers.get('Content-Type') == 'application/octet-stream', f"data for {link.get('name')} is not what we expected. Expected octet stream but got {resp.headers.get('Content-Type')}\n"
    dat = resp.content.decode('utf-8')
    folder.upload_file(dat, link.get('name'))


