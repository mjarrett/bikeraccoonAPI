from flask import Flask, request, make_response, send_from_directory

import json
import hashlib
import sqlite3
import pytz
import datetime as dt
import itertools
import os

from sqlalchemy import create_engine, func
from sqlalchemy.orm import Session

from .models import System, Station, Measurement
from .api_functions import *


def get_station_trips(session, t1,t2,sys_name,station_id,frequency,tz):
    qry = session.query(Measurement)
    qry = qry.filter(Measurement.datetime >= t1, Measurement.datetime <= t2)
    qry = qry.filter(System.name == sys_name)
    qry = qry.join(Station).join(System)
    qry = qry.filter(Station.station_id==station_id)
    res = [x.as_dict() for x in qry.all()]
    for r in res:
        r['datetime'] = to_local_time(r['datetime'],tz)
    res = _dict_groupby(res,frequency)
    return json_response(res)   


def get_all_stations_trips(session, t1,t2,sys_name,frequency,tz):
    qry = session.query(Measurement)
    qry = qry.filter(Measurement.datetime >= t1, Measurement.datetime <= t2)
    qry = qry.filter(System.name == sys_name)
    qry = qry.join(Station).join(System)
    qry = qry.filter(Station.station_id!='free_bikes')
    res = [x.as_dict() for x in qry.all()]
    for r in res:
        r['datetime'] = to_local_time(r['datetime'],tz)
    res = _dict_groupby(res,frequency)
    return json_response(res)    
    
    
    
    
def get_system_trips(session, t1,t2, sys_name, frequency,tz):
    # First, query trips from stations
    qry = session.query(Measurement.datetime,func.sum(Measurement.trips), func.sum(Measurement.returns))
    qry = qry.filter(Measurement.datetime >= t1, Measurement.datetime <= t2)
    qry = qry.filter(System.name == sys_name)
    qry = qry.filter(Station.id != 'free_bikes')
    qry = qry.join(Station).join(System)


    qry = qry.group_by(Measurement.datetime)

    res =  [{'datetime':to_local_time(time,tz),'trips':trips,'returns':returns} for time,trips,returns in qry.all()]
    res = _dict_groupby(res,frequency)
    res = [ {'datetime':x['datetime'], 'station trips':x['trips']} for x in res] # Don't need returns for the whole system

    # Then query trips from free bikes
    qry2 = session.query(Measurement.datetime,func.sum(Measurement.trips), func.sum(Measurement.returns))
    qry2 = qry2.filter(Measurement.datetime >= t1, Measurement.datetime <= t2)
    qry2 = qry2.filter(System.name == sys_name)
    qry2 = qry2.filter(Station.station_id == 'free_bikes')
    qry2 = qry2.join(Station).join(System)


    qry2 = qry2.group_by(Measurement.datetime)
    res2 = qry2.all()

    if len(res2) == 0:
        return json_response(res)

    res2 =  [{'datetime':to_local_time(time,tz),'trips':trips,'returns':returns} for time,trips,returns in res2]
    res2 = _dict_groupby(res2,frequency)

    # Add free bikes to main response
    for r in res:

        r['free bike trips'] = [r2 for r2 in res2 if r2['datetime']==r['datetime']]
        if len(r['free bike trips']) > 0:
            r['free bike trips'] = r['free bike trips'][0]['trips']
        else:
            r['free bike trips'] = 0
    res = json_response(res)
    return res



def string_to_datetime(t):
    y = int(t[:4])
    m = int(t[4:6])
    d = int(t[6:8])
    h = int(t[8:10])
    return dt.datetime(y,m,d,h)

def to_utc(t,tz):
    return pytz.timezone(tz).localize(t).astimezone(pytz.utc)

def to_local_time(t,tz):
    return t.replace(tzinfo=pytz.utc).astimezone(pytz.timezone(tz)) 

def json_response(r):
    r = make_response(json.dumps(r, default=str, indent=4))
    r.mimetype = "text/plain"
    return r
    
    res.mimetype = "text/plain"

def _dict_groupby(res, frequency):
    
    if frequency == 'h':
        key = None
    if frequency == 'd':
        key = lambda x: x['datetime'].replace(hour=0)
    if frequency == 'm':
        key = lambda x: x['datetime'].replace(hour=0, day=1)
    if frequency == 'y':
        key = lambda x: x['datetime'].replace(hour=0, day=1, month=1)

    if key is not None:
        # This is just a fancy groupby date
        
        # This is special case function to calc mean of a generator 
        def _mean(x,n):
            s = sum(y[n] for y in x['data'] if y[n] is not None)
            l = len(list(y[n] for y in x['data'] if y[n] is not None))
            if l == 0:
                return 0
            return int(s/l)
        
        if 'num_bikes_available' in res[0]: 
            # First, gather each field for each grouped date
            res = [{'datetime':k,'data':[(x['trips'],x['returns'],
                                          x['num_bikes_available'],
                                          x['num_docks_available'],
                                          x['station_id'],x['station']) 
                                         for x in group]} 
                     for k, group 
                     in itertools.groupby(res, key=key)]



            # Finally, aggregate each field as appropriate. 
            res = [{'datetime':x['datetime'], 
                    'trips':sum(y[0] for y in x['data'] if y[0] is not None),
                    'returns':sum(y[1] for y in x['data'] if y[1] is not None),
                    'num_bikes_available':_mean(x,2),
                    'num_docks_available':_mean(x,3),
                    'station_id':x['data'][0][4],
                    'station':x['data'][0][5]
                   } 
                   for x in res]
            
        else:
            # this is the simple case where we're not breaking down by station
            res = [{'datetime':k,'data':[(x['trips'],x['returns']) 
                                         for x in group]} 
                     for k, group 
                     in itertools.groupby(res, key=key)]



            # Finally, aggregate each field as appropriate. 
            res = [{'datetime':x['datetime'], 
                    'trips':sum(y[0] for y in x['data'] if y[0] is not None),
                    'returns':sum(y[1] for y in x['data'] if y[1] is not None)
                   } 
                   for x in res]
    return res
 
    
def return_api_error():

    content = "Invalid API request :("
    return content, 400
