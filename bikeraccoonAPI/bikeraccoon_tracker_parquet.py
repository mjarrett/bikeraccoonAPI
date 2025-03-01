import time
import pandas as pd
import pyarrow as pa
import datetime as dt
import sys
import json
import logging
from logging.handlers import TimedRotatingFileHandler

from .parquet_functions import (update_free_bikes_raw,
                         update_stations_raw, update_trips, update_stations,
                         )








def get_system_time(system):
    return pd.Timestamp(dt.datetime.utcnow()).tz_localize('UTC').tz_convert(system['tz'])








def tracker_parquet(systems_file='systems.json',log_file=None,
            update_interval=20, query_interval=20, station_check_hour=4,
            save_temp_data=False):
    
    ## SETUP LOGGING
    logger = logging.getLogger("Rotating Log")
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    
    if log_file is not None:
        handler = TimedRotatingFileHandler(log_file,
                                       when="d",
                                       interval=1,
                                       backupCount=5)
    else:
        handler  = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    
    ## Setup 
    ddf = bdf = pd.DataFrame()
    last_update = dt.datetime.now()
    query_time = dt.datetime.now()
    update_delta = dt.timedelta(minutes=update_interval)
    



    ## Do an initial station update on startup
    with open('systems.json') as f:
        systems = json.load(f)

    for system in systems:
        if system['tracking']:
            update_stations(system)


    logger.info("Daemon started successfully")
    
    while True:
        
        
        if dt.datetime.now() < query_time:
            time.sleep(1)  # Check whether it's time to update every second (actual query interval time determined by 
            continue
        else:
            query_time = dt.datetime.now() + dt.timedelta(seconds=query_interval)

        logger.info(f"start: {dt.datetime.now()}")

        for system in systems:
            if not system['tracking']:
                continue
           
            logger.info(f"***{system['name']} - querying station info")
            
            try:
                update_stations_raw(system)
            except Exception as e:
                logger.info(f"***{system['name']} - failed station update \n{e}")
                
            try:
                update_free_bikes_raw(system)
            except Exception as e:
                logger.info(f"***{system['name']} - failed free bike update \n{e}")

            system['tracking_end'] = dt.datetime.utcnow() # Last update

        logger.debug(f"Next DB update: {last_update + update_delta}")
        if dt.datetime.now() >  last_update + update_delta:
            last_update = dt.datetime.now()

            for system in systems:
                if not system['tracking']:
                    continue
                
                update_trips(system,save_temp_data=save_temp_data)

                    
                if get_system_time(system).hour == station_check_hour: # check stations at 4am local time
                    logger.info(f"***{system['name']} updating stations")
                    update_stations(system)

            
            
        logger.info(f"end: {dt.datetime.now()}")



if __name__ == '__main__':
    tracker()
