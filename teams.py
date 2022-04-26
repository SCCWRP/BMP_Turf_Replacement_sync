import os, json, re
from io import StringIO, BytesIO
from shareplum import Site
from shareplum import Office365
from shareplum.site import Version

import pandas as pd

from functions import csv_to_db

username = os.environ.get('MS_USERNAME')
pw = os.environ.get('MS_PASSWORD')
url = os.environ.get('TEAMS_URL')
relative_url = os.environ.get('TEAMS_RELATIVE_URL')

url = 'https://sccwrp.sharepoint.com/'
teamname = 'SanDiegoCountyBMPMonitoring'

# strings that we will consider as missing data
MISSING_DATA_VALUES = ['','NA','NaN','NAN']


authcookie = Office365(url, username=username, password=pw).GetCookies()
site = Site(os.path.join(url, 'sites', teamname), version=Version.v2016, authcookie=authcookie)
folder = site.Folder('Shared%20Documents/Turf%20Replacement/Data/Raw')

alldata = pd.DataFrame()
for file_ in folder.files:
    filename = file_.get('Name')
    print(filename)
    
    if filename.split('.')[-1] not in ('csv'):
        continue
    
    data = folder.get_file(filename)
    datastring = data.decode('utf-8')

    # I ran into an error assuming the files were all comma delimited, but some were tab delimited
    try:
        # https://stackoverflow.com/questions/22604564/create-pandas-dataframe-from-a-string     
        f = StringIO(datastring)
        
        # First line of the StringIO object disappears after we read it
        # Question: Is there important info that we need from this first line?
        firstline = f.readline().split(',')
        print(firstline)
        
        df = pd.read_csv(f, na_values = MISSING_DATA_VALUES)
        assert len(df.columns) > 1, "Only one column found. It is likely that the file is not comma delimited but rather tab delimited"
    
    except AssertionError as e:
        print(e)
        try:
            f = StringIO(datastring)
            # First line of the StringIO object disappears after we read it
            # Question: Is there important info that we need from this first line?
            firstline = f.readline().split('\t')
            print(firstline)
            
            df = pd.read_csv(f, sep = '\t', na_values = MISSING_DATA_VALUES)
        except Exception as e:
            print(f'Unable to read file {filename} for an unexpected reason')
            print(e)
            # print("This is the string it was trying to read")
            # print(datastring)
            continue
    

    print(df)

    
    # It's zero based indexing so row "1" is actually that second row of useless blank values and "Smp" so we will drop that row
    df = df.drop(1, axis = 'rows')

    # What this will do is essentially combine that first row into the column headers. We will then need to drop that first row of the dataframe
    df.columns = [
        f"{c}_{unit}" for c, unit in df.iloc[0].reset_index(name = 'unit').apply(lambda row: (row['index'], row['unit']), axis = 1)
    ]
    print(df)

    # Drop that first row with unit names which corrupts each column's datatype
    df.drop(0, axis = 'rows', inplace = True)

    # make the dataframe a json string and read it back in to let pandas automatically correct the corrupted datatypes
    df = pd.read_json(StringIO(json.dumps(df.to_dict('records'))))

    print(df)
    print(df.dtypes)

    # clean up the column names
    df.columns = [re.sub('\s+','_', c.strip().lower().replace('^','')) for c in df.columns]

    # columns have now been lowercased
    df.rename(columns = {'timestamp_ts': 'timestamp'}, inplace = True)

    # get rid of unneccesary columns
    dropcols = [
        c for c in df.columns if not ( c == 'timestamp' or 'm3/m3' in c )
    ]
    df.drop(dropcols, axis = 'columns', inplace = True)

    # melt to long format
    df = df.melt(id_vars = ['timestamp'], var_name = 'sensor', value_name = 'result')

    # Take the unit to be everything after the underscore, if an underscore is in the column name (which it should always be, but we cant be too safe. Assumptions are dangerous)
    # Otherwise put the unit value as nothing
    df['unit'] = df.sensor.apply(lambda x: str(x).rsplit('_', 1)[-1] if str(x).count('_') > 0 else None)

    # in the sensor column, get rid of the unit value
    df.sensor = df.sensor.apply(lambda x: str(x).rsplit('_', 1)[0])

    # tack on the file name and put it as the first column in the dataframe for asthetic purposes
    metadatacols = {
        'origin_sharepointsite': folder.site_url,
        'origin_foldername' : folder.folder_name,
        'origin_filename': filename.rsplit('.',1)[0]
    }
    df[list(metadatacols.keys())] = pd.Series(list(metadatacols.values()))
    df = df[ [ *list(metadatacols.keys()), *[c for c in df.columns if c not in ('origin_filename', 'origin_foldername', 'origin_sharepointsite') ] ] ]

    print(df)
    print(df[~pd.isnull(df.result)])
    

    alldata = pd.concat(
        [alldata, df],
        ignore_index = True
    )

# throw the data in the tmp folder
tmpcsvpath = '/tmp/tmpwatervolume.csv'
alldata.to_csv(tmpcsvpath, index = False, header = False)

tblname = 'tbl_watervolume'
print(f"Loading data to {tblname}")
exitcode = csv_to_db(os.environ.get("DB_HOST"), os.environ.get("DB_NAME"), os.environ.get("DB_USER"), tblname, alldata.columns, tmpcsvpath)
print(f"Exit code {exitcode}")


# remove the clutter from tmp folder
os.remove(tmpcsvpath)

#print(alldata)




