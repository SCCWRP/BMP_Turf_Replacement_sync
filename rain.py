from sqlalchemy import create_engine
from datetime import datetime, timedelta
import pandas as pd
import os, time

from functions import fetch_raindata, check_date_arg, exception_handler

@exception_handler
def sync_rain(SITENAME, START_DATE, eng):
    # msgs is what we will return for final report to email
    msgs = []
    TODAY = pd.Timestamp(datetime.today())

    # check_date_arg returns a neatly formatted string of a date, which is nice, but not what we want here
    # We will call pd.Timestamp on it so we can do date operations with it
    START_DATE = pd.Timestamp(check_date_arg(START_DATE))


    site_info = pd.read_sql(f"SELECT * FROM lu_rainsensor WHERE sitename = '{SITENAME}'", eng).to_dict('records')[0]

    # check if data exists
    latest_date = pd.read_sql('SELECT MAX(reading) as last_date FROM tbl_rain', eng).last_date.values[0]

    # count number of records currently in rain table
    record_count_before = pd.read_sql('SELECT COUNT(*) as recs FROM tbl_rain', eng).recs.values[0]

    # if the table is empty it will come up null and we will start pulling data from the default start date specified up top
    if latest_date:
        msg = f"Latest data in tbl_rain is from {pd.Timestamp(latest_date).strftime('%Y-%m-%d')}"
        START_DATE = max(pd.Timestamp(latest_date), START_DATE)
    else:
        msg = "No data found in the table tbl_rain"
    
    # print message to console and add to email report
    print(msg)
    msgs.append(msg)


    n = 0

    print((START_DATE + timedelta(days = 59 * n)).date())
    print(START_DATE)
    print(TODAY.date() - timedelta(days=1))

    # Start date should be the max of the base start date specified at the top of the script, and the latest date in the table
    # pull records from that start date up until yesterday's date, otherwise we'll get a primary key violation every time
    # Plus we can think of it as waiting for the entire day to complete before pulling rain data for that date
    while ( (START_DATE + timedelta(days = 59 * n)).date() < TODAY.date() - timedelta(days = 1)):
        start = START_DATE + timedelta(days = 59 * n)
        end = min(START_DATE + timedelta(days = 59 * (n + 1)), TODAY)
        print(f"Getting data from {start} to {end}")
        rain = fetch_raindata(
            start_date = start, 
            end_date = end,
            sitename=SITENAME, 
            site_id = site_info.get('site_id'),
            long_site_id = site_info.get('site'),
            long_device_id = site_info.get('device_id')
        )

        rain = rain.sort_values('reading')
        rain = rain[pd.to_datetime(rain['reading'])>START_DATE]

        try:
            print(f"Here are the records from {start} to {end}")
            print(rain)
        except Exception as e:
            # Sometimes it freaks out trying to print data if there are unicode or special characters
            # It would be a shame for the whole script to die because it couldnt execute a print statement
            print(f"Could not print the records to console due to an unexpected error.\n{e}")
        
        # Here goes nothing
        try:
            print("Loading records to tbl_rain")
            # The if_exists argument in to_sql indicates how to behave if the table already exists 
            # 'fail'-> raise ValueError, 'replace'->drop table before insering new values,'append' (self explanatory)
            rain.to_sql('tbl_rain', eng, index = False, if_exists = 'append')
            print(f"{len(rain)} records of data loaded successfully to tbl_rain")
        except Exception as e:
            # Sometimes it freaks out trying to print data if there are unicode or special characters
            # It would be a shame for the whole script to die because it couldnt execute a print statement
            print(f"Could not load the records due to an unexpected error.\n{e}")
        
        # if this next line gets deleted or commented out, we'll be stuck in an infinite loop, 
        # or at least a 10 minute loop provided the below code remains
        n += 1 

        if (pd.Timestamp(time.time(), unit = 's') - TODAY).total_seconds() > 600:
            # This should never take over 10 minutes.....
            raise Exception('Rain sync script took over 10 minutes. Something is not working correctly....')


    record_count_after = pd.read_sql('SELECT COUNT(*) as recs FROM tbl_rain', eng).recs.values[0]

    msg = f"Total of {record_count_after - record_count_before} records loaded to tbl_rain"
    print(msg)
    msgs.append(msg)

    return msgs

