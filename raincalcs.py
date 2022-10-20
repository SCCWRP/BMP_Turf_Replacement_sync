from sqlalchemy import create_engine
from datetime import datetime, timedelta
from arcgis.gis import GIS
import pandas as pd
import numpy as np
import json, os, sys

from functions import get_rainevents, exception_handler

@exception_handler
def sync_raincalcs(eng):
    stations = pd.read_sql("SELECT DISTINCT site FROM lu_nearestraingauge", eng).site.values
    report = []
    main_df = pd.DataFrame()
    for station in stations:
        # Check if database has sensors for given station
        sensors = pd.read_sql(f"SELECT sensor FROM tbl_sensorid WHERE sitename='{station}'", eng)
        if len(sensors.sensor)==0:
            print(f"No sensors found for station:{station}")
            report.append(f"No sensors found for station:{station}")
            continue
        
        # Generate rain events based on rain table from database
        print("Fetching rain events")
        rainevents = get_rainevents(pd.read_sql("SELECT * FROM tbl_rain ORDER BY reading;", eng))
        print(rainevents)
        print("Complete")

        # Initialize sensor list and priorday and priorhour sql conditions
        sensors_tup = tuple(sensors.sensor)
        print(sensors_tup)

        for event in rainevents.iterrows():
            side_df = pd.DataFrame()
            priorhour = f"BETWEEN '{event[1].rainstart-timedelta(hours=1)}' AND '{event[1].rainstart}'"
            event_interval = f"BETWEEN '{event[1].rainstart}' AND '{event[1].rainend}'"
            print(f"Finding prior day and hour averages for all sensors for the rain event {event_interval}")
            prioravg1 = pd.read_sql(f"""
                                SELECT
                                    sensor,
                                    AVG(wvc_final) AS priorhouravg,
                                    unit AS priorhouravgunit,
                                    COUNT(*) AS priorhour_n
                                FROM tbl_watervolume
                                WHERE ("timestamp" {priorhour} AND sensor IN {sensors_tup}  AND (ABS(wvc_final) < 1.01))
                                GROUP BY sensor, unit
                                    """, eng)

            # If database does not contain any readings for specified sensors, move on to next rain event.
            if (len(tuple(prioravg1.sensor))==0):
                print(f"No sensor readings found for the rain event {event_interval.lower()} at {station}")
                report.append(f"No sensor readings found for the rain event {event_interval.lower()} at {station}")
                continue

            sensors_tup = tuple(prioravg1.sensor)
            print(prioravg1)
            side_df = prioravg1.copy()
            print("Complete\n")

            print("Finding maximum result during rain event for each sensor")
            rainmax = pd.read_sql(f"""
                                    WITH trunc_result AS (
                                        SELECT sensor, "timestamp", wvc_final AS result
                                        FROM tbl_watervolume 
                                        WHERE ("timestamp" {event_interval} AND sensor IN {sensors_tup} AND (ABS(wvc_final) < 1.01) ) 
                                    )
                                    SELECT table1.sensor, maxresult, max_n, "timestamp"
                                    FROM (
                                        SELECT
                                            sensor,
                                            MAX(result) AS maxresult,
                                            COUNT(*) AS max_n
                                        FROM trunc_result
                                        GROUP BY sensor
                                    ) AS table1 
                                    LEFT JOIN trunc_result
                                    ON table1.maxresult = trunc_result.result AND table1.sensor = trunc_result.sensor
                                    """, eng)
            print(rainmax)
            maxresult = []
            max_n = []
            maxtime = []
            maxduration = []
            for sensor in sensors_tup:
                tmp = rainmax[rainmax.sensor==sensor]
                if not tmp.empty:
                    maxresult.append(tmp.maxresult.values[0])
                    max_n.append(tmp.max_n.values[0])
                    maxtime.append(str(tmp.timestamp.max())[:-3])
                    maxduration.append(round((tmp.timestamp.max() - tmp.timestamp.min()).seconds/3600,3))
                else:
                    maxresult.append(pd.NA)
                    max_n.append(pd.NA)
                    maxtime.append(pd.NA)
                    maxduration.append(pd.NA)

            side_df['maxresult'] = pd.DataFrame(maxresult).round(3)
            side_df['max_n'] = max_n
            side_df['maxresultUnit'] = side_df.priorhouravgunit
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
            for sensor, priorhouravg in prioravg1[['sensor','priorhouravg']].itertuples(index=False):
                temp = pd.read_sql(f"""
                                SELECT "timestamp"
                                FROM tbl_watervolume
                                WHERE sensor = '{sensor}' AND "timestamp" > '{event[1].rainend}' AND wvc_final <= {priorhouravg}
                                ORDER BY "timestamp"
                                LIMIT 1
                                """, eng)
                elapsedtime.append(round((temp.timestamp[0]-event[1].rainend).days*24+(temp.timestamp[0]-event[1].rainend).seconds/3600,3))
            print(elapsedtime)
            side_df['elapsedtime'] = elapsedtime
            side_df['timeunits'] = ['hrs'] * len(sensors_tup)
            print("Complete\n")
            side_df['totaldepth'] = [totaldepth.values[0][0]] * len(sensors_tup)
            side_df['totaldepthunit'] = [event[1].unit] * len(sensors_tup)
            side_df['rainstart'] = [str(event[1].rainstart)[:-3]] * len(sensors_tup)
            side_df['rainend'] = [str(event[1].rainend)[:-3]] * len(sensors_tup)
            side_df['region'] = [event[1].sitename] * len(sensors_tup)
            side_df['sitename'] = [station] * len(sensors_tup)
            side_df.priorhouravg= side_df.priorhouravg.round(3)
            main_df = pd.concat([main_df, side_df], axis=0)
            report.append(f"{len(sensors_tup)} sensors were active during the rainevent {event_interval.lower()} at {station}")
    
    try:
        eng.execute("DROP VIEW IF EXISTS vw_rainevent;")
        main_df.to_sql(f"tbl_rainevent", eng, index=False, if_exists='replace')
        print(main_df)
        report.append(f"Rain event calculcations successfully loaded for")
    except Exception as e:
        print(f"Could not load the records due to an unexpected error.\n{e}")
        report.append(f"Could not load the records due to an unexpected error.\n{e}")
    return report

