import pandas as pd

from sqlalchemy import (Table, Column, Integer, String, MetaData, 
                        ForeignKey, Float, Date, Time, DateTime, Boolean, func)
from sqlalchemy.exc  import OperationalError

from .query_functions import query_station_status, query_free_bikes, query_station_info

from .models import Measurement, System, Station, Trip

import logging
logger = logging.getLogger("Rotating Log")

import datetime as dt
import os
        


    
def update_stations_raw(system):
    # Query stations and save to temp table
    ddf_file = f"{system['name']}_stationbikes_raw.parquet"
    try:
        ddf = pd.read_parquet(ddf_file)
    except:
        ddf = None
    try:
        ddf_query = query_station_status(system['url'])
        ddf_query['datetime'] = ddf_query['datetime'].dt.tz_convert(system['tz'])
        ddf_query['station_id'] = ddf_query['station_id'].astype(str)
        ddf = pd.concat([ddf,ddf_query])
         
        
    except Exception as e:
        logger.debug(f"{system['name']} gbfs query error, skipping stations_raw db update: {e}")
        return 
        
    ddf.to_parquet(ddf_file,index=False)
    
def update_free_bikes_raw(system):
    bdf_file = f"{system['name']}_freebikes_raw.parquet"
    try:
        bdf = pd.read_parquet(bdf_file)
    except:
        bdf = None
    try:
        bdf = pd.concat([bdf,query_free_bikes(system['url'])])
        ddf['datetime'] = ddf['datetime'].dt.tz_convert(system['tz'])
    except Exception as e:
        logger.debug(f"{system['name']} gbfs query error, skipping free_bikes_raw db update: {e}")
        return 

    bdf.to_parquet(bdf_file,index=False)
    
    
    
def map_station_id_to_station(station_id,system,session):
    """
    Given a station_id string and a system obj, return a station ORM object
    with the latest 'created_by' date for that station_id and system
    """
    
    qry = session.query(Station).join(System).filter(System.name==system.name)
    qry = qry.filter(Station.station_id==station_id).order_by(Station.created_date.desc())
    
    return qry.first()
    
def update_trips(system,save_temp_data=False):
    
    """
    Pulls raw data from raw db, computes trips, saves trip data to main db via session
    """
    

    logger.info(f"Updating {system['name']} tables")


    ## Compute hourly station trips, append to trips table
    try:
        ddf = pd.read_parquet(f"{system['name']}_stationbikes_raw.parquet")   
    except:
        ddf = pd.DataFrame()
        
    ## Compute hourly free bike trips, append to trips table
    try:
        bdf = pd.read_parquet(f"{system['name']}_freebikes_raw.parquet")  
    except:
        bdf = pd.DataFrame()
        
    thdf = pd.concat([make_free_bike_trips(bdf), make_station_trips(ddf)], sort=True)

    
    # save temp data to CSV files
    if save_temp_data:
        if not os.path.exists('./logs'):
            os.makedirs('./logs')

        time_slug = dt.datetime.now().strftime('%Y%m%d%H%M')
        ddf.to_csv(f'./logs/station_data-{system.name}-{time_slug}.csv', index=False)
        bdf.to_csv(f'./logs/bike_data-{system.name}-{time_slug}.csv', index=False)
        
#     ## Get station objects to match to records  
#     try:
#         thdf['station'] = thdf['station_id'].apply(map_station_id_to_station,args=(system,session))
#     except Exception as e:
#         logger.debug(f"Updating {system['name']} tables failed due to exception: {e}")
#         return 
      
#     del thdf['station_id'] # Not needed anymore
    
    #records = thdf.to_dict('records')  # convert dataframe as to a list of dicts
    
#     def update_existing_record(record):
#         """
#         For a given Measurement record, update an existing record
#         or create a new Measurement object
#         """
#         # Try to get existing record
#         existing_record = session.query(Measurement).filter_by(datetime=record['datetime'],station=record['station']).first()

#         if existing_record is not None:
#             existing_record.trips = existing_record.trips + record['trips']
#             existing_record.returns = existing_record.returns + record['returns']
#             return existing_record
#         else:
#             return Measurement(**record)
    
    # Add rows to measurements table
    try:
        thdf_historical = pd.read_parquet(f"{system['name']}_hourly.parquet")
    except:
        thdf_historical = None
    thdf = pd.concat([thdf,thdf_historical])
        
    print(thdf.head())
    print(thdf.columns)
    

    
    
    
    
    # Drop records in raw tables except for most recent query
    trim_raw(f"{system['name']}_stationbikes_raw.parquet")
    trim_raw(f"{system['name']}_freebikes_raw.parquet")
    
    thdf.to_parquet(f"{system['name']}_hourly.parquet", index=False)
    

def trim_raw(fname):
    """
    Only keep the latest query, drop older queries
    """
    
    try:
        df = pd.read_parquet(fname)
    except FileNotFoundError:
        return
    
    df = df[df['datetime']== df.iloc[-1].loc['datetime']]
    
    df.to_parquet(fname,index=False)
    
        
 
    
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
                
                
def update_stations(system):
    """
    Update stations table
    Adds station if doesn't exist, updates active status
    """
    logger.info(f"{system['name']} Station Update")
    
    
#     #Add a "free_bikes" station if it doesn't exist
#     fb_stations = session.query(Station).join(System).filter(System.id==system.id,Station.station_id=='free_bikes').all()
#     if len(fb_stations) == 0:
#         fb_station = Station(name='free_bikes',station_id='free_bikes', system_id=system.id)
#         session.add(fb_station)
            
#         session.commit()
#         logger.info(f"{system.name} add free_bikes station")
    
#     # If there's more than one fb station for a system, delete extras and assign trips to first instance
#     elif len(fb_stations) > 1:
#         fb_station = fb_stations[0]
#         ms = session.query(Measurement).join(Station).filter(Station.id.in_( [fb_station.id for fb_station in fb_stations])).all()
#         for m in ms:
#             m.station_id = fb_station.id

#         for fb_station in fb_stations[1:]:
#             session.delete(fb_station)
#         session.commit()
#         logger.info(f"{system.name} removed extra free_bikes stations")
    
    
    #-- Load stations file if exists
    stations_file = f"{system['name']}_stations.parquet"
    stations_file_bak = f"{system['name']}_stations_BAK.parquet"
    try:
        sdf_current = pd.read_parquet(stations_file)
    except:
        sdf_current = None
    

    #-- Query stations
    try:
        sdf = query_station_info(system['url'])
#         sdf.to_parquet(f"{system['name']}_stations.parquet",index=False)
    except Exception as e:
        logger.debug(f"{system['name']} failed to load stations: {e}")
        return 
    
    #-- Query station status
    try:
        ddf = query_station_status(system['url'])
    except Exception as e:
        logger.debug(f"{system['name']} failed to load station status: {e}")
        return 
    
    
    #-- Add any legacy stations that aren't in current station query
    if sdf_current is not None:
        legacy_stations_df = sdf_current[~sdf_current['station_id'].isin(sdf['station_id'])]
        legacy_stations_df['active'] = False
        sdf = pd.concat([sdf,legacy_stations_df])
    

    

    #-- Run through station status data to label disabled stations
    sdf[sdf['station_id'].isin(ddf['is_renting']==0)]['active'] = False
    
    
    #-- Save stations file
    try:
        os.rename(stations_file,stations_file_bak)
    except FileNotFoundError:
        pass
    sdf.to_parquet(stations_file,index=False)
    
    logger.info(f"{system['name']} Station Update Complete")
    
    

    
    

    
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
    
        