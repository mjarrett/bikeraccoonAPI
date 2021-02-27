import time
import pandas as pd
import datetime as dt
import sys
import sqlite3
import json
import logging
from logging.handlers import TimedRotatingFileHandler

from .db_functions import (make_raw_tables, update_free_bikes_raw, activate_system,
                         update_stations_raw, update_trips, update_stations, update_systems,
                         )

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from .models import Base, System






def get_system_time(system):
    return pd.Timestamp(dt.datetime.utcnow()).tz_localize('UTC').tz_convert(system.tz)


class BikeShareSystem(dict):
    pass

def load_systems(systems_file):

    with open(systems_file) as f:
        systems = json.load(f)
        systems = {k:BikeShareSystem(v) for k,v in systems.items()}

    return systems



def tracker(systems_file='systems.json',db_file='bikeraccoon.db', 
            db_file_raw='bikeraccoon-raw.db',log_file=None,
            update_interval=20, query_interval=20, station_check_hour=4):
    
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
    
    systems = load_systems(systems_file)
   
    session = Session(engine)
    Base.metadata.create_all(engine)  # Create ORM tables if they don't exist

    # Set all systems to "not_tracking". They will be turned on in the activate_system() call
    for sys_obj in session.query(System).all():
        sys_obj.is_tracking = False
    session.commit()
    
    for system in systems.values():
        activate_system(system, session)
        
        
    # This updates the metadata for each system
    update_systems(session)    
    
    # Do an initial station update on startup
    for system in session.query(System).filter(System.is_tracking==True):
        print(system)
        make_raw_tables(system, engine_raw)
        update_stations(system, session)

    session.close()
    logger.info("Daemon started successfully")
    
    while True:
        
        
        if dt.datetime.now() < query_time:
            continue
        else:
            query_time = dt.datetime.now() + dt.timedelta(seconds=query_interval)

        logger.info(f"start: {dt.datetime.now()}")

        session = Session(engine)
        for system in session.query(System).filter(System.is_tracking==True):
           
            logger.info(f"***{system.name} - querying station info")
            update_stations_raw(system, engine_raw)

            update_free_bikes_raw(system, engine_raw)
        

        logger.debug(f"Next DB update: {last_update + update_delta}")
        if dt.datetime.now() >  last_update + update_delta:
            last_update = dt.datetime.now()

            for system in session.query(System).filter(System.is_tracking==True):
                
                update_trips(system, session, engine_raw)

                if get_system_time(system).hour == station_check_hour: # check stations at 4am local time
                    logger.info(f"***{system.name} updating stations")
                    update_stations(system, session)
            
        session.close()
            
        logger.info(f"end: {dt.datetime.now()}")
        time.sleep(1)  # Check whether it's time to update every second (actual query interval time determined by "query_interval"



if __name__ == '__main__':
    tracker()
