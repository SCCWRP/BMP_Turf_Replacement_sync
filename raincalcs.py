from sqlalchemy import create_engine
from datetime import datetime, timedelta
from arcgis.gis import GIS
import pandas as pd
import numpy as np
import json, os, sys

from functions import get_rainevents, exception_handler

@exception_handler
def sync_raincalcs(station, eng):
    # Check if database has sensors for given station
    sensors = pd.read_sql(f"SELECT sensor FROM tbl_sensorid WHERE sitename='{station}'", eng)
    if len(sensors.sensor)==0:
        print(f"No sensors found for station:{station}")
        return
    
    # Generate rain events based on rain table from database
    print("Fetching rain events")
    rainevents = get_rainevents(pd.read_sql("SELECT * FROM tbl_rain ORDER BY reading;", eng))
    print(rainevents)
    print("Complete")

    # Initialize sensor list and priorday and priorhour sql conditions
    sensors_tup = tuple(sensors.sensor)
    print(sensors_tup)

    main_df = pd.DataFrame()
    for event in rainevents.iterrows():
        side_df = pd.DataFrame()
        priorhour = f"BETWEEN '{event[1].rainstart-timedelta(hours=1)}' AND '{event[1].rainstart}'"
        print("Finding prior day and hour averages for all sensors for some rain event")
        prioravg1 = pd.read_sql(f"""
                            SELECT
                                sensor,
                                AVG(result) AS priorhouravg
                            FROM tbl_watervolume
                            WHERE ("timestamp" {priorhour} AND sensor IN {sensors_tup})
                            GROUP BY sensor
                                """, eng)
        sensors_tup = tuple(prioravg1.sensor)
        prioravg1.priorhouravg = prioravg1.priorhouravg.round(3)

        # If database does not contain any readings for specified sensors, move on to next rain event.
        if (len(sensors_tup)==0): return

        print(prioravg1)
        side_df = prioravg1.copy()
        print("Complete\n")

        event_interval = f"BETWEEN '{event[1].rainstart}' AND '{event[1].rainend}'"
        print("Finding maximum result during rain event for each sensor")
        rainmax = pd.read_sql(f"""
                                WITH trunc_result AS (
                                    SELECT sensor, "timestamp", result
                                    FROM tbl_watervolume 
                                    WHERE ("timestamp" {event_interval} AND sensor IN {sensors_tup}) 
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
        print(rainmax)
        maxresult = []
        maxtime = []
        maxduration = []
        for sensor in sensors_tup:
            maxresult.append(rainmax[rainmax.sensor==sensor].maxresult.values[0])
            maxtime.append(str(rainmax[rainmax.sensor==sensor].timestamp.max()))
            maxduration.append(str(rainmax[rainmax.sensor==sensor].timestamp.max() - rainmax[rainmax.sensor==sensor].timestamp.min())[:-2])
        side_df['maxresult'] = pd.DataFrame(maxresult).round(3)
        side_df['maxtime'] = maxtime
        side_df['maxduration'] = maxduration
        print("Complete\n")


        print("Finding total depth of rain during rain event")
        totaldepth = pd.read_sql(f"""
                                SELECT SUM(value)
                                FROM tbl_rain
                                WHERE reading {event_interval} AND sitename = '{event[1].sitename}'
                                """, eng)
        print(totaldepth)
        print("Complete")

        elapsedtime = []
        print("Finding time elapsed until results return to prior hour averages")
        for sensor, priorhouravg in prioravg1.itertuples(index=False):
            elapsedtime.append(str(pd.read_sql(f"""
                                SELECT "timestamp"
                                FROM tbl_watervolume
                                WHERE sensor = '{sensor}' AND "timestamp" > '{event[1].rainend}' AND result < {priorhouravg}
                                ORDER BY "timestamp"
                                LIMIT 1
                                """, eng).timestamp[0] - event[1].rainend)[:-2])
        print(elapsedtime)
        side_df['elapsedtime'] = elapsedtime
        print("Complete\n")
        side_df['totaldepth'] = [totaldepth.values[0][0]] * len(sensors_tup)
        side_df['rainstart'] = [event[1].rainstart] * len(sensors_tup)
        side_df['rainend'] = [event[1].rainend] * len(sensors_tup)
        side_df['region'] = [event[1].sitename] * len(sensors_tup)
        side_df['unit'] = [event[1].unit] * len(sensors_tup)
        side_df['sitename'] = [station] * len(sensors_tup)
        main_df = pd.concat([main_df, side_df], axis=0)

# temporary load to database

DB_PLATFORM = os.environ.get('DB_PLATFORM')
DB_HOST = os.environ.get('DB_HOST')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_PORT = os.environ.get('DB_PORT')
DB_NAME = os.environ.get('DB_NAME')

eng = create_engine(
    f"{DB_PLATFORM}://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

field = sync_raincalcs('Fieldcrest', eng)
field.to_sql('tbl_rainevent', eng, index = False, if_exists = 'replace')


