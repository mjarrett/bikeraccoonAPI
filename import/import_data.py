import csv, sqlite3, sqlalchemy
import pandas as pd

from sqlalchemy import create_engine
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from sqlalchemy.sql import func

import difflib
import sys

engine = create_engine('sqlite:///bikeraccoon.db', echo=False)
session = Session(engine)

from models import Measurement, Station, System, Base
Base.metadata.create_all(engine) 

def import_file(sys_name, path_to_data):

    print(sys_name)
    print(path_to_data)
    
    tdf = pd.read_csv(f'{path_to_data}/data/taken_hourly.csv')
    tdf = tdf.set_index('time')
    tdf.index = pd.to_datetime(tdf.index)

    rdf = pd.read_csv(f'{path_to_data}/data/returned_hourly.csv')
    rdf = rdf.set_index('time')
    rdf.index = pd.to_datetime(rdf.index)

    df_stations = pd.read_csv(f'{path_to_data}/data/stations.csv', dtype=str)

    tdf.index = tdf.index.tz_localize(None) # Strip TZ info, flat UTC time
    rdf.index = rdf.index.tz_localize(None) # Strip TZ info, flat UTC time

    tdf = tdf.stack(level=0).reset_index()
    tdf.columns = ['datetime','station_id','trips']

    rdf = rdf.stack(level=0).reset_index()
    rdf.columns = ['datetime','station_id','returns']

    df = pd.merge(tdf,rdf,how='outer',on=['datetime','station_id'])

    del tdf, rdf
    
    name_id_xwalk = df_stations.set_index('name')['station_id'].to_dict()



    def convert_name_to_id(station_name):
        # Convert station name to closest match in xwalk
        try:
            station_name = difflib.get_close_matches(station_name,name_id_xwalk.keys(),cutoff=0.8)[0]
            return name_id_xwalk[station_name]
        except:
            return station_name


    name_id_xwalk2 = {x:convert_name_to_id(x) for x in set(df.station_id)}

    name_id_xwalk2

    df['station_id'] = df['station_id'].map(name_id_xwalk2)

    # Make a station for stations in old records that aren't mapped to a station object
    sys_id = session.query(System.id).filter_by(name=sys_name).first()[0]
    bad_station = Station(station_id='-1', name='Unknown Station', system_id=sys_id)
    session.add(bad_station)
    session.commit()

    def get_station_pk(station_id):
        try:
            return session.query(Station.id).join(System).filter(System.name==sys_name, Station.station_id==station_id).first()[0]
        except:
            return session.query(Station.id).join(System).filter(System.name==sys_name, Station.station_id=='-1').first()[0]


    station_id_pk_xwalk = {x:get_station_pk(x) for x in set(df.station_id)}

    df['station_id'] = df['station_id'].map(station_id_pk_xwalk)


    df.to_sql('measurement', engine, if_exists='append', index=False)
    
    
    
if __name__ == '__main__':
    import_file(sys.argv[1], sys.argv[2])
