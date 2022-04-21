import os
from sqlalchemy import create_engine

from rain import sync_rain
from functions import send_mail

DB_PLATFORM = os.environ.get('DB_PLATFORM')
DB_HOST = os.environ.get('DB_HOST')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_PORT = os.environ.get('DB_PORT')
DB_NAME = os.environ.get('DB_NAME')

eng = create_engine(
    f"{DB_PLATFORM}://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# initialize report
report = []

# sync rain data from February 2022 and later
report = [*report, *sync_rain(SITENAME = 'Roads Div I', START_DATE = '2022-02-01', eng = eng)]

SEND_FROM = 'admin@checker.sccwrp.org'
SEND_TO = ['robertb@sccwrp.org']
SUBJECT = 'San Diego Turf BMP Sync Report'
BODY = '\n'.join(report)
SERVER = '192.168.1.18'
send_mail(SEND_FROM, SEND_TO, SUBJECT, BODY, server = SERVER)

