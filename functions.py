import pandas as pd
import subprocess as sp
import requests, os, inspect, traceback, re
from io import StringIO
from executing import Source # executing library depends on asttokens which must be installed separately
from datetime import datetime, timedelta
import numpy as np

site_id = 15
device_id = 1
long_site_id = '91047666-b9ea-48a7-bb25-f3d0b974a428'
long_device_id = '566cde92-8697-4632-8396-d0a7fdedeb18'
start_date = '2022-04-11'
end_date = '2022-04-18'

# This function can probably be used elsewhere also. It might even be generalizable for other data types
def check_date_arg(date):
    # https://stackoverflow.com/questions/2749796/how-to-get-the-original-variable-name-of-variable-passed-to-a-function
    stack = traceback.extract_stack()
    filename, _, function_name, _ = stack[-2]

    callframe = inspect.currentframe().f_back
    callnode = Source.executing(callframe).node
    source = Source.for_frame(callframe)
    argname = source.asttokens().get_text(callnode.args[0])

    if type(date) == str:
        try:
            date = pd.Timestamp(date).strftime('%Y-%m-%d')
        except ValueError as e:
            raise Exception(f'Error in {function_name} imported from {filename.split("/")[-1]}: The argument {argname} with a value of {date} is not in a correct date format.')
    elif type(date) in (pd.Timestamp, datetime):
        date = date.strftime('%Y-%m-%d')
    else:
        raise Exception(f'Error in {function_name} imported from {filename.split("/")[-1]}: The argument {argname} (whose value was {date}) is of type {type(date)} but it must be a string, a pandas Timestamp, or a datetime object.')
    
    return date



# This basically just takes information to create the url for the API call
def fetch_raindata(
    start_date,
    end_date,
    sitename,
    # Default to grabbing data from Roads Div I from sandiego onerain
    site_id = 15,
    long_site_id = '91047666-b9ea-48a7-bb25-f3d0b974a428',
    long_device_id = '566cde92-8697-4632-8396-d0a7fdedeb18',
    device_id = 1, # This was strange and im not sure what this number means, but it was a query string argument
    baseurl = "https://sandiego.onerain.com/export/file/" # I see absolutely no reason why this would change
):
    start_date = check_date_arg(start_date)
    end_date = check_date_arg(end_date)

    url = "{}?{}".format(
        baseurl,
        '&'.join([
            f"site_id={site_id}",
            f"site={long_site_id}",
            f"device_id={device_id}",
            f"device={long_device_id}",
            "mode=",
            "hours=",
            f"data_start={start_date}",
            f"data_end={end_date}",
            "tz=US%2FPacific",
            "format_datetime=%25Y-%25m-%25d+%25H%3A%25i%3A%25S",
            "mime=txt",
            "delimiter=comma"
        ])
    )
    # The url is used to grab the rain data
    # print(url)

    req = requests.get(url)
    rain = pd.read_csv(StringIO(req.content.decode("utf-8")))
    rain.columns = [c.lower() for c in rain.columns]

    # Append the long site id to the rain data
    rain['sitename'] = sitename

    # This is basically just to put siteid as the first column for asthetic purposes
    rain = rain[ [ 'sitename', *[c for c in rain.columns if c != 'sitename'] ]]

    rain.columns = [re.sub('\s+', '_', c.strip()) for c in rain.columns]

    return rain

# Accepts `gis` engine and pulls the first table from survey with ID:`surv_key` from the Survey123 website. 
def fetch_survey123data(gis, surv_name, surv_key, cols = None):
    print(f"Accessing item: {surv_key}")
    collection = gis.content.get(surv_key)
    tables = collection.tables
    print('Pulling the main table called repeat_a')
    df = tables[0].query().sdf # We have only one layer for each 'main'
    df.columns = list(map(str.lower, df.columns))
    
    # subset the dataframe only if certain columns are specified
    # With the default value for cols set to None, it means that the function will grab all columns by default
    if cols is not None:
        df = df[cols] 
    
    print("survey_df")
    print(df)
    return df

# Accepts rain data (should have column for sitename but only one site) and returns start and end of rain events conditioned on:
# Start: Has rained more than 0.2 inches in less than 2 hours
# End: Has not rained for more than 6 hours
def get_rainevents(rain):
    rainevent = pd.DataFrame()
    rainswitch = False
    start = None
    for day in rain.iterrows():
        if ~rainswitch & (day[1].value!=0) & (rain[(day[1].reading<=rain.reading) & (day[1].reading+timedelta(hours=2)>rain.reading)].value.sum() >= 0.2): 
            rainswitch = True
            start = day[1].reading
        elif rainswitch & (rain[(day[1].reading<rain.reading) & (day[1].reading+timedelta(hours=6)>rain.reading)].value.sum() == 0):
            rainswitch = False
            rainevent = pd.concat([rainevent, pd.DataFrame([day[1].sitename, start, day[1].reading, day[1].unit]).transpose()], axis=0)
        else:
            continue
    rainevent.columns = ['sitename', 'rainstart', 'rainend', 'unit']
    return rainevent


# Send mail function along with its imports
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.utils import COMMASPACE, formatdate
from email import encoders
import smtplib

# Function to be used later in sending email
def send_mail(send_from, send_to, subject, text, filename=None, server="localhost"):
    msg = MIMEMultipart()
    
    msg['From'] = send_from
    msg['To'] = COMMASPACE.join(send_to)
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject
    
    msg_content = MIMEText(text)
    msg.attach(msg_content)
    
    if filename is not None:
        attachment = open(filename,"rb")
        p = MIMEBase('application','octet-stream')
        p.set_payload((attachment).read())
        encoders.encode_base64(p)
        p.add_header('Content-Disposition','attachment; filename= %s' % filename.split("/")[-1])
        msg.attach(p)

    smtp = smtplib.SMTP(server)
    smtp.sendmail(send_from, send_to, msg.as_string())
    smtp.close()


def csv_to_db(DB_HOST, DB_NAME, DB_USER, tablename, columns, csvpath, overwrite = False):
    out = ''
    err = ''
    # This will ensure the data is copied with correct corresponding columns
    # psql can execute since it authenticates with PGPASSWORD environment variable
    # sqlcmd = f'psql|-h|{DB_HOST}|-d|{DB_NAME}|-U|{DB_USER}|-c|"DELETE FROM {tablename}";'
    delcmd = f'psql|-h|{DB_HOST}|-d|{DB_NAME}|-U|{DB_USER}|-c|DELETE FROM {tablename}'.split('|')
    print(delcmd)

    # f'psql|-h|{DB_HOST}|-d|{DB_NAME}|-U|{DB_USER}|-c|"\copy {tablename} ({",".join(columns)}) FROM \'{csvpath}\' csv\"'
    insertcmd = f'psql|-h|{DB_HOST}|-d|{DB_NAME}|-U|{DB_USER}|-c|\copy {tablename} ({",".join(columns)}) FROM \'{csvpath}\' csv'.split('|')
    print(insertcmd)
    
    if overwrite:
        proc = sp.run(delcmd, stdout=sp.PIPE, stderr=sp.PIPE, universal_newlines = True)
        out += proc.stdout
        err += proc.stderr
    
    proc = sp.run(insertcmd, stdout=sp.PIPE, stderr=sp.PIPE, universal_newlines = True)
    out += proc.stdout
    err += proc.stderr

    return { "out": out, "err": err }


def exception_handler(func):
    def callback(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            return [f'Unexpected error in {func.__name__}:\nArguments: {args}\n{str(e)[:1000]}']
    return callback


def permittivitycalc(row, lowKaconstant, highKaconstant, limitKaconstant):
    # function from Dario to calculate ToppVWC values
    # test_df = pd.DataFrame({'timestamp':['2022-06-03 12:53:00','2022-06-03 12:54:00','2022-06-03 12:55:00'], 'sensor':['S3TC1T01','S3TC1T01',s3tc1t01w], 'wv':[7999.0,7999.0,0.361], 'ec':[1.261,1.260,0.202], 'pa':[5.274,5.274,3.454], 'ka':[7999,7999,21.32]})
    # test_df['timestamp'] = pd.to_datetime(test_df['timestamp'])
    # test_df.apply(permittivitycalc, axis = 1)
    if (row['wvc_raw'] == 7999.0 and row['ka_raw'] == 7999.0):
        results = row

        EC = row['ec']
        PA = row['pa']

        lowKa = (lowKaconstant['C0']*EC**3*PA**2) + (lowKaconstant['C1']*EC**2*PA**2) + (lowKaconstant['C2']*EC*PA**2) + (lowKaconstant['C3']*PA**2) + (lowKaconstant['C4']*EC**3*PA)+(lowKaconstant['C5']*EC**2*PA) \
            + (lowKaconstant['C6']*EC*PA)+(lowKaconstant['C7']*PA)+(lowKaconstant['C8']*EC**3)+(lowKaconstant['C9']*EC**2) + (lowKaconstant['C10']*EC) + lowKaconstant['C11']
        highKa = (highKaconstant['C0']*EC**3*PA**2) + (highKaconstant['C1']*EC**2*PA**2) + (highKaconstant['C2']*EC*PA**2) + (highKaconstant['C3']*PA**2) + (highKaconstant['C4']*EC**3*PA) + (highKaconstant['C5']*EC**2*PA) \
            + (highKaconstant['C6']*EC*PA) + (highKaconstant['C7']*PA) + (highKaconstant['C8']*EC**3) + (highKaconstant['C9']*EC**2) + (highKaconstant['C10']*EC) + highKaconstant['C11']
        
        limitKa = limitKaconstant['C0'] + (limitKaconstant['C1']*EC) + (limitKaconstant['C2']*EC**2) + (limitKaconstant['C3']*EC**3) + (limitKaconstant['C4']*EC**4) + (limitKaconstant['C5']*EC**5)
        limitKa80 = 0.8*limitKa
        if (EC <= 1.09 and lowKa > 40):
            newKa = highKa
        else:
            newKa = lowKa
        Kacorrectionconstant = pd.Series({'Kamult':1.03, 'Kaoffset':-0.3})
        
        # Based on instructions in the excel file that Dario gave
        correctedKa = (newKa*Kacorrectionconstant['Kamult']) + Kacorrectionconstant['Kaoffset']
        

        # So we can see the calculated values as they were, before making decisions on whether or not to keep them
        results['ka_calc'] = correctedKa
        correctedTopp = (-0.053)+(0.0292*(correctedKa))-(0.00055*(correctedKa)**2)+(0.0000043*(correctedKa)**3) 
        results['wvc_prelim_calc'] = correctedTopp


        # if certain conditions are met, we are instructed to change KA and Topp to NULL or 1 or 0 sometimes
        if (correctedKa < 0) or (correctedKa > 88) or (correctedKa < limitKa80) or (PA < 1.18) or (np.round(EC, 2) > 1.09):
            correctedKa = pd.NA
        elif correctedKa < 1:
            correctedKa = 1
        
        # So we can see the calculated values as they were, before making decisions on whether or not to keep them
        correctedTopp = (-0.053)+(0.0292*(correctedKa))-(0.00055*(correctedKa)**2)+(0.0000043*(correctedKa)**3) 
        results['wvc_calc'] = correctedTopp

        if not pd.isnull(correctedKa):
            if 1 <= correctedKa <= 1.881:
                correctedTopp = 0
            elif correctedKa > 40:
                correctedTopp = pd.NA

        
        results['ka_final'] = correctedKa
        results['wvc_final'] = correctedTopp
        results['highka'] = highKa
        results['lowka'] = lowKa
        results['kalimit'] = limitKa
        results['kalimit80pct'] = limitKa80
        results['calculated'] = True
    else:
        results['calculated'] = False

    return results



def calibration(row):
    results = row
    
    if (row['timestamp'] >= pd.Timestamp('2022-04-01 00:00:00')) and (row['timestamp'] < pd.Timestamp('2022-05-17 00:00:00')) and (row['sensor'].startswith('S1')):

        EC = row['ec'] # Electric Conductivity
        VR = row['vr'] # Voltage Ratio
        
        # not sure what KA stands for, but the units are Dper (and i dont know what that means either)
        Dper = row['ka_final'] 

        if VR >= 17:
            return results

        if EC > 0.8:
            results['wvc_prelim_calc'] = (
                (0.00003 * (Dper ** 3)) - (0.0025 * (Dper ** 2)) + (0.0675 * Dper) - 0.872
            )
        else:
            results['wvc_prelim_calc'] = (0.001 + (0.53116 * EC)) ** 0.4418
        
        # for now just accept the value as final
        results['wvc_calc'] = results['wvc_prelim_calc']
        results['wvc_final'] = results['wvc_prelim_calc']
    
    
    return results

