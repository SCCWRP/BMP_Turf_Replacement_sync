import os, json, re
from io import StringIO
from shareplum import Site
from shareplum import Office365
from shareplum.site import Version
import time

import pandas as pd

from functions import csv_to_db, exception_handler, permittivitycalc, calibration

@exception_handler
def sync_watervolume(username, password, url, teamname, sitefolder, acceptable_file_extensions = ('dat')):

    # initialize report
    report = []
    report.append(f"Report for sync of watervolume data from {os.path.join(url, 'sites', teamname, sitefolder)}")

    # strings that we will consider as missing data
    MISSING_DATA_VALUES = ['','NA','NaN','NAN']


    authcookie = Office365(url, username=username, password=password).GetCookies()
    site = Site(os.path.join(url, 'sites', teamname), version=Version.v2016, authcookie=authcookie)
    print(os.path.join(url, 'sites', teamname))
    print(sitefolder)
    folder = site.Folder(sitefolder)

    alldata = pd.DataFrame()

    valid_files = [f for f in folder.files if f.get('Name').split('.')[-1] in acceptable_file_extensions]
    if len(valid_files) == 0:
        report.append(f"\tNo files found with extension {acceptable_file_extensions}")
        return report


    # We may possibly only grab files that are not found in the tbl_watervolume table, in which case we need to disable the overwrite argument in csv_to_db


    for i, file_ in enumerate(valid_files):
       
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
            print("An error occurred")
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
        df.rename(columns = {'timestamp_ts': 'timestamp', 'battv_volts':'battv'}, inplace = True)

        # Some data files have duplicates, 
        df = df.drop_duplicates(subset = 'timestamp', keep = 'last')

        # get rid of unneccesary columns
        # this iterative notation comes up again
        # [f(iter) FOR iter IN list] evaluates f on iter as it iterates through list and puts into a list
        # Here, f is the identity function and instead include an if statement at the end that saves
        # iter in the new list if the current entry meets the condition.

        # added in 3 columns for EC, PA, and K to be used later in a calculation if VWC is 7999
        dropcols = [
            c for c in df.columns if not ( c in ('timestamp', 'battv') or 'm3/m3' in c or 'ds/m' in c or 'usec' in c or 'dper' in c or 'vr' in c)
        ]
        df.drop(dropcols, axis = 'columns', inplace = True)

        print("df")
        print(df)



        # get lists of columns for each variable to melt on later 
        # wv is water volume
        wvcols = [
            c for c in df.columns if ( c in ('timestamp', 'battv') or 'm3/m3' in c)
        ]

        # ec is electric conductivity
        eccols = [
            c for c in df.columns if ( c in ('timestamp', 'battv') or 'ds/m' in c)
        ]

        # pa is .... ?
        pacols = [
            c for c in df.columns if ( c in ('timestamp', 'battv') or 'usec' in c)
        ]

        # ka is .... ? (Dper)
        kacols = [
            c for c in df.columns if ( c in ('timestamp', 'battv') or 'dper' in c)
        ]
        
        # vr is voltage ratio
        vrcols = [
            c for c in df.columns if ( c in ('timestamp', 'battv') or 'vr' in c)
        ]

        print(wvcols)

        # 4 different melts then combine into one dataframe
        dfwv = df[wvcols].melt(id_vars = ['timestamp', 'battv'], var_name = 'sensor', value_name = 'wvc_raw')
        # Take the unit to be everything after the underscore, if an underscore is in the column name (which it should always be, but we cant be too safe. Assumptions are dangerous)
        # Otherwise put the unit value as nothing
        # rsplit begins search from the right and arg after is number of splits.
        dfwv['wvcunit'] = dfwv.sensor.apply(lambda x: str(x).rsplit('_', 1)[-1] if str(x).count('_') > 0 else None)
        

        # in the sensor column, get rid of the unit value and the "W" at the end
        dfwv.sensor = dfwv.sensor.apply(lambda x: str(x).upper().rsplit('_', 1)[0][:-1] )
        #dfwv['sensor'] = dfwv['sensor'].astype("category")


        # do this 3 more times for the other measurements
        dfec = df[eccols].melt(id_vars = ['timestamp', 'battv'], var_name = 'sensor', value_name = 'ec')
        dfec['ecunit'] = dfec.sensor.apply(lambda x: str(x).rsplit('_', 1)[-1] if str(x).count('_') > 0 else None)
        dfec.sensor = dfec.sensor.apply(lambda x: str(x).upper().rsplit('_', 1)[0][:-1] )
        #dfec['sensor'] = dfec['sensor'].astype("category")
        # dfec.set_index(['sensor','timestamp'], inplace = True)


        dfpa = df[pacols].melt(id_vars = ['timestamp', 'battv'], var_name = 'sensor', value_name = 'pa')
        dfpa['paunit'] = dfpa.sensor.apply(lambda x: str(x).rsplit('_', 1)[-1] if str(x).count('_') > 0 else None)
        dfpa.sensor = dfpa.sensor.apply(lambda x: str(x).upper().rsplit('_', 1)[0][:-1] )
        # dfpa['sensor'] = dfpa['sensor'].astype("category")


        dfka = df[kacols].melt(id_vars = ['timestamp', 'battv'], var_name = 'sensor', value_name = 'ka_raw')
        dfka['kaunit'] = dfka.sensor.apply(lambda x: str(x).rsplit('_', 1)[-1] if str(x).count('_') > 0 else None)
        dfka.sensor = dfka.sensor.apply(lambda x: str(x).upper().rsplit('_', 1)[0][:-1] )
        
        # voltage ratio
        dfvr = df[vrcols].melt(id_vars = ['timestamp', 'battv'], var_name = 'sensor', value_name = 'vr')
        dfvr['vrunit'] = dfvr.sensor.apply(lambda x: str(x).rsplit('_', 1)[-1] if str(x).count('_') > 0 else None)
        dfvr.sensor = dfvr.sensor.apply(lambda x: str(x).upper().rsplit('_', 1)[0][:-1] )
        #dfvr['sensor'] = dfvr['sensor'].astype("category")

        # df_merge1 = pd.merge(dfwv, dfec, on = ['sensor','timestamp'])
        # df_merge2 = pd.merge(dfpa, dfka, on = ['timestamp','sensor'])
        # df = pd.merge(df_merge1, df_merge2, on = ['timestamp','sensor'])
        df = dfwv.merge(dfec, on = ['sensor','timestamp', 'battv'], how = 'inner') \
                .merge(dfpa, on = ['sensor','timestamp', 'battv'], how = 'inner') \
                .merge(dfka, on = ['sensor','timestamp', 'battv'], how = 'inner') \
                .merge(dfvr, on = ['sensor','timestamp', 'battv'], how = 'inner')
        

        # initialize other calculated fields that will assist with the QA process
        
        df['wvc_prelim_calc'] = pd.NA
        df['wvc_calc'] = pd.NA
        df['ka_calc'] = pd.NA
        df['wvc_final'] = df['wvc_raw']
        df['ka_final'] = df['ka_raw']
        
        df['highka'] = pd.NA
        df['lowka'] = pd.NA
        df['kalimit'] = pd.NA
        df['kalimit80pct'] = pd.NA

        # initialize a False column for the calculated column to indicate actual measured value vs calculated value if watervolume is 7999
        df['calculated'] = False
        

        # update dtypes on text columns to more efficient types
        df['sensor'] = df['sensor'].astype("category")
        df['wvcunit'] = df['wvcunit'].astype("category")
        df['ecunit'] = df['ecunit'].astype("category")
        df['paunit'] = df['paunit'].astype("category")
        df['kaunit'] = df['kaunit'].astype("category")

        print(df.dtypes)

        # get subset of df
        df7999subset = df[df['wvc_raw'] == 7999]
        print(df7999subset)
        dfrestsubset = df[df['wvc_raw'] != 7999]

        # constants to give to permittivity calc function
        lowKaconstant = pd.Series({'C0':6.19697, 'C1':-8.12137, 'C2':7.36456, 'C3':4.10614, 'C4':-39.3171, 'C5':53.2833, 'C6':-38.2849, 'C7':-9.18995, 'C8':47.832, 'C9':-110.294, 'C10':41.1352, 'C11':5.89699})
        highKaconstant = pd.Series({'C0':13.8638, 'C1':-21.5214, 'C2':5.29937, 'C3':4.35371, 'C4':-136.878, 'C5':222.28, 'C6':-48.9962, 'C7':-10.5421, 'C8':336.265, 'C9':-584.727, 'C10':118.829, 'C11':7.07995})
        limitKaconstant = pd.Series({'C0':1.87467, 'C1':27.79005, 'C2':-10.3748, 'C3':11.7403, 'C4':0, 'C5':0})

        # apply permittivitycalc function to subset of df with only 7999 wv value
        start_time = time.time()
        print("Start the permittivity calculation")
        df7999subset = df7999subset.apply(permittivitycalc, args = (lowKaconstant, highKaconstant, limitKaconstant), axis = 1)
        print(time.time() - start_time)
        print(df7999subset.dtypes)

        # recombine 7999 subset and rest
        df = pd.concat([df7999subset, dfrestsubset])
        print(df)

        # apply Elizabeth's new calibration, to all records
        print('''# apply Elizabeth's new calibration, to all records''')
        df = df.apply(calibration, axis = 1)

        # tack on the file name and put it as the first column in the dataframe for aesthetic purposes
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
    print('writing data to the tmp folder')
    alldata.to_csv(tmpcsvpath, index = False, header = False)
    print('Finished writing data to the tmp folder')

    tblname = 'tbl_watervolume'
    # NOTE overwrite = True argument may have to go away. We may only pull files that are not found in tbl_watervolume, or only files in the metadata table called "datafileindex"
    # That datafileindex table may come from survey123 in the future

    # Temporarliy disable for development

    print(f"Loading data to {tblname}")
    result = csv_to_db(os.environ.get("DB_HOST"), os.environ.get("DB_NAME"), os.environ.get("DB_USER"), tblname, alldata.columns, tmpcsvpath, overwrite = True)
    print(f"Exit code {result}")
    
    report.append(f"Attempt to load data to {tblname}")
    report.append("Here is the result")
    report.append(f"""\tSTDOUT: {result.get('out')}""")
    report.append(f"""\tSTDERR: {result.get('err')}""")
    
    return report







