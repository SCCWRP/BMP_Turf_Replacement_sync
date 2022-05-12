import os
import json
from sqlalchemy import create_engine

from rain import sync_rain
from functions import send_mail
from watervolume import sync_watervolume
from meta import sync_metadata
from survey123 import sync_survey123_multiple
from views import create_views

from arcgis.gis import GIS

### SQL Engine ###
DB_PLATFORM = os.environ.get('DB_PLATFORM')
DB_HOST = os.environ.get('DB_HOST')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_PORT = os.environ.get('DB_PORT')
DB_NAME = os.environ.get('DB_NAME')

eng = create_engine(
    f"{DB_PLATFORM}://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)
##################

### Arcgis login ###
gis_username = os.environ.get('GIS_USERNAME')
gis_password = os.environ.get('GIS_PASSWORD')
gis = GIS("https://www.arcgis.com",gis_username,gis_password)
print("Logged in as " + str(gis.properties.user.username))
##################


# initialize report
report = []

tables = {
    "tbl_controltest" : {
        "cols": ['station','timeirrigationon','timeirrigationoff'],
        "surveys": {
            "SDturf_FieldForm_v1": "08e20b9f48b84662b55de474b950b958",
            #"SDturf_FieldForm_v2": "f31c0f02127647c5888c34bc32f017d3"
        }
    }
}



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
    ),
    *sync_survey123_multiple(eng, gis, tables),
    *create_views(eng)
]

SEND_FROM = 'admin@checker.sccwrp.org'
SEND_TO = ['robertb@sccwrp.org']
SUBJECT = 'San Diego Turf BMP Sync Report'
BODY = '\n\n'.join(report)
SERVER = '192.168.1.18'
send_mail(SEND_FROM, SEND_TO, SUBJECT, BODY, server = SERVER)

