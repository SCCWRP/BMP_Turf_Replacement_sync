import os
from sqlalchemy import create_engine

from rain import sync_rain
from functions import send_mail
from watervolume import sync_watervolume
from meta import sync_metadata

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
report = [
    *report, 
    *sync_metadata(
        username = os.environ.get('MS_USERNAME'),
        password = os.environ.get('MS_PASSWORD'),
        url = os.environ.get('SHAREPOINT_SITE_URL'),
        teamname = 'SanDiegoCountyBMPMonitoring',
        sitefolder = 'Shared%20Documents/Turf%20Replacement/Data/Metadata',
        filename = 'SDturf_DataFileIndex.xlsx',
        tablename = 'tbl_datafileindex',
        eng = eng
    ),
    *sync_metadata(
        username = os.environ.get('MS_USERNAME'),
        password = os.environ.get('MS_PASSWORD'),
        url = os.environ.get('SHAREPOINT_SITE_URL'),
        teamname = 'SanDiegoCountyBMPMonitoring',
        sitefolder = 'Shared%20Documents/Turf%20Replacement/Data/Metadata',
        filename = 'Sensor_IDs.xlsx',
        tablename = 'tbl_sensorid',
        eng = eng
    ),
    *sync_metadata(
        username = os.environ.get('MS_USERNAME'),
        password = os.environ.get('MS_PASSWORD'),
        url = os.environ.get('SHAREPOINT_SITE_URL'),
        teamname = 'SanDiegoCountyBMPMonitoring',
        sitefolder = 'Shared%20Documents/Turf%20Replacement/Data/Metadata',
        filename = 'lu_nearestraingauge.xlsx',
        tablename = 'lu_nearestraingauge',
        eng = eng
    ),
    *sync_rain(SITENAME = 'Roads Div I', START_DATE = '2022-02-01', eng = eng),
    *sync_watervolume(
        username = os.environ.get('MS_USERNAME'),
        password = os.environ.get('MS_PASSWORD'),
        url = os.environ.get('SHAREPOINT_SITE_URL'),
        teamname = 'SanDiegoCountyBMPMonitoring',
        sitefolder = 'Shared%20Documents/Turf%20Replacement/Data/Raw'
    )
]

SEND_FROM = 'admin@checker.sccwrp.org'
SEND_TO = ['robertb@sccwrp.org']
SUBJECT = 'San Diego Turf BMP Sync Report'
BODY = '\n\n'.join(report)
SERVER = '192.168.1.18'
send_mail(SEND_FROM, SEND_TO, SUBJECT, BODY, server = SERVER)

