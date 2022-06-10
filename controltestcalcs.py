from sqlalchemy import create_engine
from datetime import datetime, timedelta
from arcgis.gis import GIS
import pandas as pd
import numpy as np
import os, time
import json
import math

from sqlalchemy import create_engine
from datetime import datetime, timedelta
import pandas as pd
import os, time

from functions import exception_handler

@exception_handler
def sync_controltestcalcs(eng):
    report = []
    report.append("Starting the controlled test calculation script")
    tests = pd.read_sql("SELECT DISTINCT station, controltestdate FROM tbl_controltest ORDER BY controltestdate", eng)
    tests.columns = ["station", "date"]
    final_df = pd.DataFrame()
    for station, date in tests.itertuples(index=False):
        sensors = pd.read_sql(f"SELECT sensor FROM tbl_sensorid WHERE sitename='{station}'", eng)
        if len(sensors.sensor)==0:
            print(f"No active sensors found for station {station}")
            continue

        # Get controlled test records without any nulls
        test_info = pd.read_sql(f"""
                                SELECT 
                                    station, 
                                    timeirrigationon, 
                                    controltestdate, 
                                    controltest_starttime, 
                                    controltest_endtime 
                                FROM tbl_controltest 
                                WHERE 
                                    (
                                        station = '{station}' 
                                    AND 
                                        controltestdate = '{date}'
                                    AND station IS NOT NULL
                                    AND timeirrigationon IS NOT NULL
                                    AND controltestdate IS NOT NULL
                                    AND controltest_starttime IS NOT NULL
                                    AND controltest_endtime IS NOT NULL
                                    )
                                """, eng).to_dict()
        if len(test_info['station'])==0:
            print(f"A control test at station {station} on {date} was not found in the database")
            continue
        print(f"Computing metrics\n\nStation: {station}\nDate: {date}\nTest Information: {test_info}\nSensor List: {sensors}\n")

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
                                    SELECT sensor, "timestamp", result, unit
                                    FROM tbl_watervolume 
                                    WHERE ("timestamp" {controltest_interval} AND sensor IN {sensors_tup}) 
                                )
                                SELECT table1.sensor, maxresult, "timestamp", table1.unit
                                FROM (
                                    SELECT
                                        sensor,
                                        MAX(result) AS maxresult,
                                        unit
                                    FROM trunc_result
                                    GROUP BY sensor, unit
                                ) AS table1 
                                LEFT JOIN trunc_result
                                ON table1.maxresult = trunc_result.result AND table1.sensor = trunc_result.sensor
                                """, eng)
        print(testmax)
        maxresult = []
        maxtime = []
        maxduration = []
        watervolumeunit = []
        for sensor in sensors_tup:
            maxresult.append(testmax[testmax.sensor==sensor].maxresult.values[0])
            maxtime.append(str(testmax[testmax.sensor==sensor].timestamp.max())[:-3])
            maxduration.append(round((testmax[testmax.sensor==sensor].timestamp.max() - testmax[testmax.sensor==sensor].timestamp.min()).seconds/3600,3))
            watervolumeunit.append(testmax[testmax.sensor==sensor].unit.values[0])
        main_df['maxresult'] = maxresult
        main_df['watervolumeunit'] = watervolumeunit
        main_df['maxtime'] = maxtime
        main_df['maxduration'] = maxduration
        print("Complete\n")

        elapsedtime = []
        controltest_end = pd.Timestamp(datetime.combine(test_info['controltestdate'][0], test_info['controltest_endtime'][0]))
        print("Finding time elapsed until results return to prior hour averages")
        for sensor, priordayavg, priorhouravg in prioravg1.itertuples(index=False):
            if math.isnan(priordayavg) | math.isnan(priorhouravg) :
                elapsedtime.append(-88)
            # Use '<=' in the query for the case when readings are 7999 indefinitely
            else:
                temp = pd.read_sql(f"""
                                SELECT "timestamp"
                                FROM tbl_watervolume
                                WHERE sensor = '{sensor}' AND "timestamp" > '{controltest_end}' AND result <= {priorhouravg}
                                ORDER BY "timestamp"
                                LIMIT 1
                                """, eng)
                if temp.empty: elapsedtime.append(-88)
                else: elapsedtime.append(round((temp.timestamp[0]-controltest_end).days*24+(temp.timestamp[0]-controltest_end).seconds/3600,3))

        print(elapsedtime)
        if -88 in elapsedtime:
            report.append(f"For some sensors during the controlled test at {station} on {date}, the script failed to obtain time it takes for sensors to reach prior hour averages. They are indicated by '-88'")
        main_df['elapsedtime'] = elapsedtime
        main_df['timeunits'] = ['hrs'] * len(sensors_tup)
        main_df['station'] = [station] * len(sensors_tup)
        main_df['date'] = [date] * len(sensors_tup)
        main_df.priordayavg = main_df.priordayavg.round(3)
        main_df.priorhouravg = main_df.priorhouravg.round(3)
        print("Complete\n")
        final_df = pd.concat([final_df, main_df], axis=0, ignore_index=True)
    
    try:
        final_df.to_sql(f"tbl_controlcalcs", eng, index=False, if_exists='replace')
        print(final_df)
        report.append(f"Controlled test calculcations successfully loaded for")
    except Exception as e:
        print(f"Could not load the records due to an unexpected error.\n{e}")
        report.append(f"Could not load the records due to an unexpected error.\n{e}")
    return report

