from sqlalchemy import create_engine
from datetime import datetime, timedelta
from arcgis.gis import GIS
import pandas as pd
import numpy as np
import os, time
import json

from functions import fetch_survey123data, exception_handler

from sqlalchemy import create_engine
from datetime import datetime, timedelta
import pandas as pd
import os, time

from functions import fetch_survey123data, exception_handler



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

@exception_handler
def controltestcalcs(station, date):
    sensors = pd.read_sql(f"SELECT sensor FROM tbl_sensorid WHERE sitename='{station}'", eng)
    if len(sensors.sensor)==0:
        print(f"No sensors found for station:{station}")
        return
    test_info = pd.read_sql(f"SELECT station, timeirrigationon, controltestdate, controltest_starttime, controltest_endtime FROM tbl_controltest WHERE station = '{station}' AND controltestdate = '{date}'", eng).to_dict()
    if len(test_info['station'])==0:
        print(f"A control test at station {station} on {date} was not found in the database")
        return
    print(f"Station: {station}\nDate: {date}\nTest Information: {test_info}\nSensor List: {sensors}\n")

    sensors_tup = tuple(sensors.sensor)
    priorday = f"BETWEEN '{test_info['timeirrigationon'][0]-timedelta(days=1)}' AND '{test_info['timeirrigationon'][0]}'"
    priorhour = f"BETWEEN '{test_info['timeirrigationon'][0]-timedelta(hours=1)}' AND '{test_info['timeirrigationon'][0]}'"
    print("Finding prior day and hour averages for all sensors")
    prioravg1 = pd.read_sql(f"""
                            WITH tbl_priorday AS (
                                SELECT sensor, result, "timestamp"
                                FROM tbl_watervolume
                                WHERE ("timestamp" {priorday}) AND sensor IN {sensors_tup}
                            )
                            SELECT table1.sensor, priordayavg, priorhouravg
                            FROM (
                                SELECT 
                                    sensor, 
                                    AVG(result) AS priordayavg 
                                FROM tbl_priorday
                                GROUP BY sensor
                                ) AS table1 FULL JOIN (
                                SELECT
                                    sensor,
                                    AVG(result) AS priorhouravg
                                FROM tbl_priorday
                                WHERE ("timestamp" {priorhour})
                                GROUP BY sensor
                                ) AS table2
                                ON table1.sensor = table2.sensor
                                ORDER BY table1.sensor
                                """, eng)
    sensors_tup = tuple(prioravg1.sensor)
    print(prioravg1)
    main_df = prioravg1.copy()
    print("Complete\n")

    controltest_interval = f"BETWEEN '{test_info['timeirrigationon'][0]}' AND '{pd.Timestamp(datetime.combine(test_info['controltestdate'][0], test_info['controltest_endtime'][0])) + timedelta(days=1)}'"
    print("Finding maximum result during test for each sensor")
    testmax = pd.read_sql(f"""
                            WITH trunc_result AS (
                                SELECT sensor, "timestamp", result
                                FROM tbl_watervolume 
                                WHERE ("timestamp" {controltest_interval} AND sensor IN {sensors_tup}) 
                            )
                            SELECT table1.sensor, maxresult, "timestamp"
                            FROM (
                                SELECT
                                    sensor,
                                    MAX(result) AS maxresult
                                FROM trunc_result
                                GROUP BY sensor
                            ) AS table1 
                            LEFT JOIN trunc_result
                            ON table1.maxresult = trunc_result.result AND table1.sensor = trunc_result.sensor
                            """, eng)
    print(testmax)
    maxresult = []
    maxtime = []
    maxduration = []
    for sensor in sensors_tup:
        maxresult.append(testmax[testmax.sensor==sensor].maxresult.values[0])
        maxtime.append(str(testmax[testmax.sensor==sensor].timestamp.max()))
        maxduration.append(str(testmax[testmax.sensor==sensor].timestamp.max() - testmax[testmax.sensor==sensor].timestamp.min()))
    main_df['maxresult'] = maxresult
    main_df['maxtime'] = maxtime
    main_df['maxduration'] = maxduration
    print("Complete\n")

    elapsedtime = []
    controltest_end = pd.Timestamp(datetime.combine(test_info['controltestdate'][0], test_info['controltest_endtime'][0]))
    print("Finding time elapsed until results return to prior hour averages")
    for sensor, priordayavg, priorhouravg in prioravg1.itertuples(index=False):
        elapsedtime.append(pd.read_sql(f"""
                            SELECT "timestamp"
                            FROM tbl_watervolume
                            WHERE sensor = '{sensor}' AND "timestamp" > '{controltest_end}' AND result < {priorhouravg}
                            ORDER BY "timestamp"
                            LIMIT 1
                            """, eng).timestamp[0] - controltest_end)
    print(elapsedtime)
    main_df['elapsedtime'] = elapsedtime
    print("Complete\n")
    return main_df