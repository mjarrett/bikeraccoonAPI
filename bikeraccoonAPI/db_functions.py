import pandas as pd

from sqlalchemy import (Table, Column, Integer, String, MetaData, 
                        ForeignKey, Float, Date, Time, DateTime, Boolean, func)

from .query_functions import query_station_status, query_free_bikes, query_station_info

from .models import Measurement, System, Station, Trip

import logging
logger = logging.getLogger("Rotating Log")

import datetime as dt
import os
        

def make_raw_tables(system, engine):
    
    metadata = MetaData()
    

    stations_raw = Table(f"{system.name}_stations_raw", metadata,
        Column('datetime', DateTime),
        Column('num_bikes_available', Integer),
        Column('num_docks_available', Integer),
        Column('is_renting', Boolean),
        Column('station_id', String)
        )

    bikes_raw = Table(f"{system.name}_bikes_raw", metadata,
         Column('datetime', DateTime),
         Column('bike_id', String),
         Column('lat', Float),
         Column('lon',Float)
         )
    logger.debug(f"{system.name} - create raw table")
    metadata.create_all(engine)  # Doesn't overwrite tables if exist
    
    
def update_stations_raw(system, engine):
    # Query stations and save to temp table
    try:
        ddf = query_station_status(system.url)
        ddf['station_id'] = ddf['station_id'].astype(str) 
    except Exception as e:
        logger.debug(f"{system.name} gbfs query error, skipping stations_raw db update: {e}")
        return 
        
    ddf.to_sql(f"{system.name}_stations_raw",engine,if_exists='append',index=False)
    
def update_free_bikes_raw(system, engine):
    try:
        bdf = query_free_bikes(system.url)
    except Exception as e:
        logger.debug(f"{system.name} gbfs query error, skipping free_bikes_raw db update: {e}")
        return 

    bdf.to_sql(f"{system.name}_bikes_raw",engine,if_exists='append',index=False)
    
    
    
    
def map_station_id_to_station(station_id,system,session):
    """
    Given a station_id string and a system obj, return a station ORM object
    with the latest 'created_by' date for that station_id and system
    """
    
    qry = session.query(Station).filter(System.name==system.name)
    qry = qry.filter_by(station_id=station_id).order_by(Station.created_date.desc())
    
    return qry.first()
    
def update_trips(system, session, engine_raw, save_temp_data=False):
    
    """
    Pulls raw data from raw db, computes trips, saves trip data to main db via session
    """
    

    logger.info(f"Updating {system.name} tables")


    ## Compute hourly station trips, append to trips table
    ddf = pd.read_sql(f"select * from {system.name}_stations_raw",engine_raw, parse_dates='datetime')   
    ## Compute hourly free bike trips, append to trips table
    bdf = pd.read_sql(f"select * from {system.name}_bikes_raw",engine_raw, parse_dates='datetime')  
   
    thdf = pd.concat([make_free_bike_trips(bdf), make_station_trips(ddf)], sort=True)

    
    # save temp data to CSV files
    if save_temp_data:
        if not os.path.exists('./logs'):
            os.makedirs('./logs')

        time_slug = dt.datetime.now().strftime('%Y%m%d%H%M')
        ddf.to_csv(f'./logs/station_data-{system.name}-{time_slug}.csv', index=False)
        bdf.to_csv(f'./logs/bike_data-{system.name}-{time_slug}.csv', index=False)
        
    ## Get station objects to match to records  
    try:
        thdf['station'] = thdf['station_id'].apply(map_station_id_to_station,args=(system,session))
    except Exception as e:
        logger.debug(f"Updating {system.name} tables failed due to exception: {e}")
        return 
      
    del thdf['station_id'] # Not needed anymore
    
    records = thdf.to_dict('records')  # convert dataframe as to a list of dicts
    
    def update_existing_record(record):
        """
        For a given Measurement record, update an existing record
        or create a new Measurement object
        """
        # Try to get existing record
        existing_record = session.query(Measurement).filter_by(datetime=record['datetime'],station=record['station']).first()

        if existing_record is not None:
            existing_record.trips = existing_record.trips + record['trips']
            existing_record.returns = existing_record.returns + record['returns']
            return existing_record
        else:
            return Measurement(**record)
    
    # Add rows to measurements table
    session.add_all([update_existing_record(r) for r in records])
    session.commit()

    
    
    
    # Drop records in raw tables except for most recent query
    trim_raw(f"{system.name}_stations_raw", engine_raw)
    trim_raw(f"{system.name}_bikes_raw", engine_raw)
    
    
    

def trim_raw(tablename, engine_raw):
    """
    Only keep the latest query, drop older queries
    """
    
    m = MetaData(bind=engine_raw, reflect=True)  # Reflect pulls in tables that sqla doesn't know about
   
    try:
        table = m.tables[tablename]
    except:
        logger.debug(f"{tablename} table missing, skip update")
        return
    
    maxdatetime = func.max(table.c.datetime).execute().fetchall()[0][0]
    s = table.delete().where(table.c.datetime != maxdatetime)
    
    
    s.execute()    
    
 
    
def make_station_trips(ddf):
    
    if len(ddf) == 0:
        return pd.DataFrame()
    
    pdf = pd.pivot_table(ddf,columns='station_id',index='datetime',values='num_bikes_available')
    df = pdf.copy()
    for col in pdf.columns:
        df[col] = pdf[col] - pdf[col].shift(-1)
    df = df.fillna(0.0).astype(int)

    df_stack = df.stack(level=0).reset_index()
    df_stack.columns = ['datetime','station_id','trips']

    df_stack['returns'] = df_stack['trips']
    df_stack.loc[df_stack['returns']<0,'returns'] = 0

    df_stack.loc[df_stack['trips']>0,'trips'] = 0
    df_stack['trips'] = -1*df_stack['trips']

    df_stack = df_stack.set_index('datetime').groupby([pd.Grouper(freq='h'),'station_id']).sum().reset_index()
    
    # Add available bikes and docks
    num_bikes_xw = ddf.groupby('station_id').max()['num_bikes_available'].to_dict()
    num_docks_xw = ddf.groupby('station_id').max()['num_docks_available'].to_dict()

    df_stack['num_bikes_available'] = df_stack['station_id'].map(num_bikes_xw)
    df_stack['num_docks_available'] = df_stack['station_id'].map(num_docks_xw)
    
    return df_stack



def make_free_bike_trips(bdf):
    
    if len(bdf) == 0:
        return pd.DataFrame()
    
    
    
    n_bikes = 0
    bikes = {}
    for t,df in bdf.groupby('datetime'):
        active_bikes = set(df['bike_id'])
        bikes[t] = active_bikes
        n_bikes = n_bikes if len(active_bikes) < n_bikes else len(active_bikes)

    t = {}
    keys = sorted(bikes.keys())
    for i in range(len(keys)-1):

        time = keys[i]
        trips_started = len(bikes[keys[i]].difference(bikes[keys[i+1]]))
        trips_ended = len(bikes[keys[i+1]].difference(bikes[keys[i]]))

        t[i] = {'trips':trips_started,'returns':trips_ended,'datetime':time}


    df = pd.DataFrame(t).T
    df = df.set_index('datetime')
    df.index = pd.to_datetime(df.index)



    df = df.groupby(pd.Grouper(freq='h')).sum()

    df['station_id'] = 'free_bikes'
    df['num_bikes_available'] = n_bikes
                
    df = df.reset_index()
    
    return df
                
                
def update_stations(system, session):
    """
    Update stations table
    Adds station if doesn't exist, updates active status
    """
    logger.info(f"{system.name} Station Update")
    try:
        sdf = query_station_info(system.url)
    except Exception as e:
        logger.debug(f"{system.name} failed to load stations: {e}")
        return 
    
    try:
        ddf = query_station_status(system.url)
    except Exception as e:
        logger.debug(f"{system.name} failed to load station status: {e}")
        return 
    
    
    
    
    # Get all current stations
    station_objs = session.query(Station).join(System).filter(System.id==system.id).all()
    station_objs_ids = [x.station_id for x in station_objs]
    
    # Run through station info data to find new stations
    for station in sdf.to_dict('records'):
        # If station doesn't exist, create it
        if station['station_id'] not in station_objs_ids:
            session.add(Station(**station, system_id=system.id, active=True))

    # Run through station status data to label disabled stations
    for station in ddf.to_dict('records'):
        
        try:
            station_obj = [x for x in station_objs if x.station_id == station['station_id']][0]
        except:
            print(station)
            continue
                
        if station['is_renting'] == 0:
        
            station_obj.active = False
        elif station['is_renting'] == 1 and not station_obj.active:
            station_obj.active = True
            
        session.add(station_obj)
        
    #Run through current stations and disable any that aren't found in station info
    for station_obj in station_objs:
        if station_obj.station_id not in list(sdf['station_id']):
            station_obj.active = False
            session.add(station_obj)
            

            
    session.commit()
    logger.info(f"{system.name} Station Update Complete")
    
    

    
    

    
# def update_systems(session):
#     logger.debug("Updating system table")
#     for sys_obj in session.query(System).all():

#         qry = session.query(Measurement.datetime).join(Station).join(System)
#         qry = qry.filter(System.name==sys_obj.name).order_by(Measurement.datetime.desc())
#         last = qry.first()
             
#         qry = session.query(Measurement.datetime).join(Station).join(System)
#         qry = qry.filter(System.name==sys_obj.name).order_by(Measurement.datetime)
#         first = qry.first()

#         if first is not None:
#             sys_obj.tracking_start = first[0]
#             sys_obj.tracking_end   = None if sys_obj.is_tracking else last[0]

#         # check if system data exists
#         first = session.query(Trip.departure_time).join(Station,Trip.departure_station_id==Station.id).join(System).filter(System.name==sys_obj.name).order_by(Trip.departure_time.desc()).first()
#         last = session.query(Trip.departure_time).join(Station,Trip.departure_station_id==Station.id).join(System).filter(System.name==sys_obj.name).order_by(Trip.departure_time).first()

#         if first is not None:
#             sys_obj.has_system_data = True
#             sys_obj.system_data_start = first
#             sys_obj.system_data_end = last
#         else:
#             sys_obj.has_system_data = False

#         session.add(sys_obj)  

#         # Add a "free_bikes" station if it doesn't exist
#         fb_station = session.query(Station).join(System).filter(System==sys_obj,Station.station_id=='free_bikes').all()
#         if len(fb_station) == 0:
#             fb_station = Station(name='free_bikes',station_id='free_bikes', system_id=sys_obj.id)
#             session.add(fb_station)


#     session.commit()
    
        