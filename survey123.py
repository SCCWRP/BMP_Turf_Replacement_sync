from dataclasses import dataclass
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import pandas as pd
import os, time
import sys

from functions import fetch_survey123data, exception_handler

@exception_handler
def sync_survey123(eng, gis, db_list, cols = None):
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
        print("In tbl_survey123")
        for station in stations:
            latest_date = pd.read_sql(f"SELECT MAX(timeirrigationon) as last_date FROM tbl_survey123 WHERE sitename='{station}'", eng).last_date.values[0]
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



# NOTE
# @Kevin they made a new survey over the weekend - i forsee them making changes, so we should start preparing now to reduce our future pain
# I added a keyword arg that will just flush and reload data, so we dont have to worry about changes
# it works the same as the sync_survey123 function but designed to possibly sync to multiple tables
@exception_handler
def sync_survey123_multiple(eng, gis, tables):
    # eng is the database connection
    # gis is the connection to arc online to grab survey data
    # tables is essentially the config for looking for the survey dataclass

    # tables = {
    #     "tblname" : {
    #         "cols": ["colname", "col2"],
    #         "surveys": {
    #             "surveyname1": "surveyID1",
    #             "surveyname2": "surveyID2"
    #         }
    #     }
    # }

   
    # msgs is what we will return for final report to email
    msgs = []
    
    # There should only be one table to be honest. But i am writing it as if there could be other tables added, which is unlikely
    # Writing code in this kind of a way has paid off for me in the past, when they do unexpected things
    for tblname, survey_info in tables.items():
        all_survey_data = pd.DataFrame()
        for surveyname, surveyID in survey_info.get("surveys").items():
            df = fetch_survey123data(gis, surveyname, surveyID, cols = survey_info.get('cols'))

            # Translation should be consistent across all surveys - Dario said he will not change these columns
            df['controltestdate'] = df.timeirrigationon.apply(lambda x: pd.Timestamp(x).date())
            df['controltest_starttime'] = df.timeirrigationon.apply(lambda x: pd.Timestamp(x).time() if not pd.isnull(x) else pd.NaT)
            df['controltest_endtime'] = df.timeirrigationoff.apply(lambda x: pd.Timestamp(x).time() if not pd.isnull(x) else pd.NaT)

            all_survey_data = pd.concat([all_survey_data, df], ignore_index = True)

        # replace since sometimes they make edits to the data
        try:
            all_survey_data.to_sql(tblname, eng, if_exists = 'replace', index = False)
            msgs.append(f"Data added from survey(s) {', '.join(surv for surv in survey_info.get('surveys').keys())} to table {tblname}")
        except Exception as e:
            msgs.append(f"Something went wrong loading the survey data for survey(s) {', '.join(surv for surv in survey_info.get('surveys').keys())}\n{str(e)[:1000]}")
    
    return msgs

