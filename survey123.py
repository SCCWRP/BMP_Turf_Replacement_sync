from sqlalchemy import create_engine
from datetime import datetime, timedelta
import pandas as pd
import os, time
import sys

from functions import fetch_survey123data, exception_handler

@exception_handler
def sync_survey123(eng, gis, db_list, cols):
    # msgs is what we will return for final report to email
    msgs = []
    for key in db_list:
        # Fetch data through survey123 API
        try:
            df = fetch_survey123data(gis, key, db_list[key], cols)
        except Exception as e:
            print(f"Something went wrong while fetching data from Survey123. Check your survey name, ID, or chosen columns \n{e}")

        stations = df['station'].unique()
        print(f'The unique sitenames on the Survey123 table are {stations}')
        
        latest_dates = {}
        # For each station, check if db has entries and return date of latest entry for station.
        for station in stations:
            latest_date = pd.read_sql(f"SELECT MAX(timeirrigationon) as last_date FROM tbl_survey123 WHERE sitename='{station}'", eng).last_date.values[0]
            print("In tbl_survey123\n")
            if latest_date:
                msg = f"For site {station}, latest entry is from {pd.Timestamp(latest_date).strftime('%Y-%m-%d')}"
            else:
                msg = f"There are no entries from site {station}"
                latest_date = 0
            latest_dates.update({f"{station}":pd.Timestamp(latest_date)})
            # print message to console and add to email report
            print(msg)
            msgs.append(msg)
        print(latest_dates)
        record_count_before = pd.read_sql('SELECT COUNT(*) AS recs FROM tbl_survey123', eng).recs.values[0]

        print(msgs)