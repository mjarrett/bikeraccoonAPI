import time
import pandas as pd
import datetime as dt
import sys
import sqlite3
import json
import logging
from logging.handlers import TimedRotatingFileHandler

from .db_functions import (make_raw_tables, update_free_bikes_raw,
                         update_stations_raw, update_trips, update_stations,
                         )

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from .models import Base, System






def get_system_time(system):
    return pd.Timestamp(dt.datetime.utcnow()).tz_localize('UTC').tz_convert(system.tz)








def tracker(systems_file='systems.json',db_file='bikeraccoon.db', 
            db_file_raw='bikeraccoon-raw.db',log_file=None,
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
    
    engine = create_engine(f'sqlite:///{db_file}', echo=False)  
    # This is for the raw tracking to minimize access to the main db
    engine_raw = create_engine(f'sqlite:///{db_file_raw}', echo=False)  
    
    
   
    session = Session(engine)
    Base.metadata.create_all(engine)  # Create ORM tables if they don't exist


  # Do an initial station update on startup
    for system in session.query(System).filter(System.is_tracking==True):
        make_raw_tables(system, engine_raw)
        try:
            update_stations(system, session)
        except:
            session.rollback()

    session.close()
    logger.info("Daemon started successfully")
    
    while True:
        
        
        if dt.datetime.now() < query_time:
            time.sleep(1)  # Check whether it's time to update every second (actual query interval time determined by 
            continue
        else:
            query_time = dt.datetime.now() + dt.timedelta(seconds=query_interval)

        logger.info(f"start: {dt.datetime.now()}")

        session = Session(engine)
        for system in session.query(System).filter(System.is_tracking==True):
           
            logger.info(f"***{system.name} - querying station info")
            update_stations_raw(system, engine_raw)

            update_free_bikes_raw(system, engine_raw)

            system.tracking_end = dt.datetime.utcnow() # Last update

        logger.debug(f"Next DB update: {last_update + update_delta}")
        if dt.datetime.now() >  last_update + update_delta:
            last_update = dt.datetime.now()

            for system in session.query(System).filter(System.is_tracking==True):
                
                try:
                    update_trips(system, session, engine_raw, save_temp_data=save_temp_data)
                except:
                    session.rollback()
                    
                if get_system_time(system).hour == station_check_hour: # check stations at 4am local time
                    logger.info(f"***{system.name} updating stations")
                    try:
                        update_stations(system, session)
                    except:
                        session.rollback()
            
        session.close()
            
        logger.info(f"end: {dt.datetime.now()}")
        "query_interval"



if __name__ == '__main__':
    tracker()
