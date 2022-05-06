import pandas as pd
import requests, os, inspect, traceback, re
from io import StringIO
from executing import Source # executing library depends on asttokens which must be installed separately
from datetime import datetime

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
def fetch_survey123data(surv_name, surv_key, gis):
    print(f"Accessing item: {surv_key}")
    collection = gis.content.get(surv_key)
    tables = collection.tables
    print('Pulling the main table called repeat_a')
    df = tables[0].query().sdf # We have only one layer for each 'main'
    df.columns = list(map(str.lower, df.columns))
    print("survey_df")
    print(df)
    return df


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
    # This will ensure the data is copied with correct corresponding columns
    # psql can execute since it authenticates with PGPASSWORD environment variable
    sqlcmd = f'psql -h {DB_HOST} -d {DB_NAME} -U {DB_USER} -c "DELETE FROM {tablename}";' if overwrite else ''

    sqlcmd += (
        f'psql -h {DB_HOST} -d {DB_NAME} -U {DB_USER} -c "\copy {tablename} ({",".join(columns)}) FROM \'{csvpath}\' csv\"'
    )
    print(sqlcmd)
    
    # At least we can catch if it failed, and which datatype was the one that failed, which is a start
    # we can email if the exitcode is non zero and include which datatype failed
    # TODO capture some kind of error message (That's probably a low priority as it)
    code = os.system(sqlcmd)
    return code


def exception_handler(func):
    def callback(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            return [f'Unexpected error in {func.__name__}:\nArguments: {args}\n{str(e)[:1000]}']
    return callback