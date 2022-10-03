import os, json, re
from io import StringIO
from shareplum import Site
from shareplum import Office365
from shareplum.site import Version

import pandas as pd

from functions import csv_to_db, exception_handler

@exception_handler
def sync_watervolume(username, password, url, teamname, sitefolder, acceptable_file_extensions = ('dat')):

    # initialize report
    report = []
    report.append(f"Report for sync of watervolume data from {os.path.join(url, 'sites', teamname, sitefolder)}")

    # strings that we will consider as missing data
    MISSING_DATA_VALUES = ['','NA','NaN','NAN']


    authcookie = Office365(url, username=username, password=password).GetCookies()
    site = Site(os.path.join(url, 'sites', teamname), version=Version.v2016, authcookie=authcookie)
    folder = site.Folder(sitefolder)

    alldata = pd.DataFrame()

    valid_files = [f for f in folder.files if f.get('Name').split('.')[-1] in acceptable_file_extensions]
    if len(valid_files) == 0:
        report.append(f"\tNo files found with extension {acceptable_file_extensions}")
        return report

    # We may possibly only grab files that are not found in the tbl_watervolume table, in which case we need to disable the overwrite argument in csv_to_db

    for file_ in valid_files:
        filename = file_.get('Name')
        print(filename)
        
        # assert data type is in the tuple of acceptable file types. Based on the code that makes the valid_files list, this assertion should never fail
        # However if the assertion does fail then we know our code doesnt work
        assert filename.split('.')[-1] in acceptable_file_extensions, f"file {filename} has an extension which is not one of: {acceptable_file_extensions}"
        
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
        
        # if line 49 raises an error, the csv is not comma separated and it is possibly tab separated. 
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
                msg = f'\tUnable to read file {filename} for an unexpected reason\n{str(e)[:1000]}'
                print(msg)
                report.append(msg)
                # print("This is the string it was trying to read")
                # print(datastring)
                continue
        

        print(df)

        
        # It's zero based indexing so row "1" is actually that second row of useless blank values and "Smp" so we will drop that row
        df = df.drop(1, axis = 'rows')

        # What this will do is essentially combine that first row into the column headers. We will then need to drop that first row of the dataframe
        # grabs first row and makes cx2 array then renames columns as index n unit, then creates series with tuples.
        df.columns = [
            f"{c}_{unit}" for c, unit in df.iloc[0].reset_index(name = 'unit').apply(lambda row: (row['index'], row['unit']), axis = 1)
        ]

        # Drop that first row with unit names which corrupts each column's datatype
        df.drop(0, axis = 'rows', inplace = True)
        # make the dataframe a json string and read it back in to let pandas automatically correct the corrupted datatypes
        # ask rob later
        df = pd.read_json(StringIO(json.dumps(df.to_dict('records'))))

        print(df)
        print(df.dtypes)

        # clean up the column names
        # First lower case namesa and replace '^' with ''. Then replace space with underscore.
        df.columns = [re.sub('\s+','_', c.strip().lower().replace('^','')) for c in df.columns]

        # columns have now been lowercased
        df.rename(columns = {'timestamp_ts': 'timestamp'}, inplace = True)

        # get rid of unneccesary columns
        # this iterative notation comes up again
        # [f(iter) FOR iter IN list] evaluates f on iter as it iterates through list and puts into a list
        # Here, f is the identity function and instead include an if statement at the end that saves
        # iter in the new list if the current entry meets the condition.
        dropcols = [
            c for c in df.columns if not ( c == 'timestamp' or 'm3/m3' in c )
        ]
        df.drop(dropcols, axis = 'columns', inplace = True)

        # melt to long format
        df = df.melt(id_vars = ['timestamp'], var_name = 'sensor', value_name = 'result')

        # Take the unit to be everything after the underscore, if an underscore is in the column name (which it should always be, but we cant be too safe. Assumptions are dangerous)
        # Otherwise put the unit value as nothing
        # rsplit begins search from the right and arg after is number of splits.
        df['unit'] = df.sensor.apply(lambda x: str(x).rsplit('_', 1)[-1] if str(x).count('_') > 0 else None)

        # in the sensor column, get rid of the unit value and the "W" at the end
        df.sensor = df.sensor.apply(lambda x: str(x).upper().rsplit('_', 1)[0][:-1] )

        # tack on the file name and put it as the first column in the dataframe for asthetic purposes
        metadatacols = {
            'origin_sharepointsite': folder.site_url,
            'origin_foldername' : folder.folder_name,
            'origin_filename': filename
            #'origin_filename': filename.rsplit('.',1)[0]
        }
        df[list(metadatacols.keys())] = pd.Series(list(metadatacols.values()))
        # The order of the columns are determined before "building" the reorderd dataframe then saving into 'df'
        # * unwraps each inner list and puts it into an outer list. The outer list is the input within the brackets to indicate which order we want the columns.
        df = df[ [ *list(metadatacols.keys()), *[c for c in df.columns if c not in ('origin_filename', 'origin_foldername', 'origin_sharepointsite') ] ] ]

        print(df)
        print(df[~pd.isnull(df.result)])
        
        # stacks clean data from each file. we ignore index because the columns are ordered already
        alldata = pd.concat(
            [alldata, df],
            ignore_index = True
        )
        print(f"{len(alldata)} water volume records before dropping duplicates")
        alldata = alldata.drop_duplicates(subset = ['timestamp','sensor'], keep = 'first')
        print(f"{len(alldata)} water volume records after dropping duplicates")

    # throw the data in the tmp folder
    tmpcsvpath = '/tmp/tmpwatervolume.csv'
    alldata.to_csv(tmpcsvpath, index = False, header = False)

    tblname = 'tbl_watervolume'
    print(f"Loading data to {tblname}")
    # NOTE overwrite = True argument may have to go away. We may only pull files that are not found in tbl_watervolume, or only files in the metadata table called "datafileindex"
    # That datafileindex table may come from survey123 in the future
    exitcode = csv_to_db(os.environ.get("DB_HOST"), os.environ.get("DB_NAME"), os.environ.get("DB_USER"), tblname, alldata.columns, tmpcsvpath, overwrite = True)
    print(f"Exit code {exitcode}")
    if exitcode == 0:
        report.append(f"\tsuccessfully loaded {len(alldata)} rows to {tblname}")
        # remove the clutter from tmp folder
        os.remove(tmpcsvpath)
    else:
        report.append(f"\tError loading data to {tblname}. The CSV file attempted to load to the table is at {tmpcsvpath}")
    
    return report







