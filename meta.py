import os, json, re
from io import StringIO, BytesIO
from shareplum import Site
from shareplum import Office365
from shareplum.site import Version

import pandas as pd

from functions import exception_handler

@exception_handler
def sync_metadata(username, password, url, teamname, sitefolder, filename, tablename, eng):
    # As of now, the two meta data tables are the data file index table and the sensor IDs table, and the nearest raingauge table
    # (SDturf_DataFileIndex.xlsx and Sensor_IDs.xlsx and lu_nearestraingauge.xlsx in Teams)

    assert type(filename) == str, f"filename argument must be of type str, but the value for the filename arg {filename} is of type {type(filename)}"

    extension = filename.rsplit('.', 1)[-1]
    assert extension in ('xlsx'), f'Error in sync_metadata: filename must be an xlsx file (filename arg was {filename}'

    # initialize report
    report = []
    report.append(f"Report for sync to {tablename} from {os.path.join(url, 'sites', teamname, sitefolder, filename)}")

    # strings that we will consider as missing data
    MISSING_DATA_VALUES = ['','NA','NaN','NAN', '--']

    authcookie = Office365(url, username=username, password=password).GetCookies()
    site = Site(os.path.join(url, 'sites', teamname), version=Version.v2016, authcookie=authcookie)
    folder = site.Folder(sitefolder)

    metadata_filenames = [f.get('Name') for f in folder.files]

    assert filename in metadata_filenames, f"filename {filename} not found in {os.path.join(url, 'sites', teamname, sitefolder)}"

    fileIO = BytesIO(folder.get_file(filename))
    data = pd.read_excel(fileIO, skiprows=[0], na_values=MISSING_DATA_VALUES)
    eng.execute(f"DROP TABLE IF EXISTS {tablename} CASCADE;")
    data.to_sql(tablename, eng, if_exists = 'replace', index = False)

    report.append(f"\tTable {tablename} created successfully from the file {filename} in {os.path.join(url, 'sites', teamname, sitefolder)}")
    return report
    
