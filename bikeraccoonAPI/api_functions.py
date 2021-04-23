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
    
    # for special case, set all times to initial time
    if frequency == 't':
        t = to_local_time(res[0]['datetime'],tz)
        for r in res:
            r['datetime'] = t
    else:
        for r in res:
            r['datetime'] = to_local_time(r['datetime'],tz)
            r['datetime'] = trim_datetime(r['datetime'],frequency)
        
    key_fields = ['datetime']

    agg_key = {'trips':_sum, 'returns':_sum, 'num_bikes_available': _mean, 'num_docks_available':_mean,
              'station':_first, 'station_id':_first}
    res = _dict_groupby(res,key_fields,agg_key)
    return json_response(res)   

def get_all_stations_trips(session, t1,t2,sys_name,frequency,tz):
    qry = session.query(Measurement)
    qry = qry.filter(Measurement.datetime >= t1, Measurement.datetime <= t2)
    qry = qry.filter(System.name == sys_name)
    qry = qry.join(Station).join(System)
    qry = qry.filter(Station.station_id!='free_bikes')
    res = [x.as_dict() for x in qry.all()]

    # for special case, set all times to initial time
    if frequency == 't':
        t = to_local_time(res[0]['datetime'],tz)
        for r in res:
            r['datetime'] = t
    else:
        for r in res:
            r['datetime'] = to_local_time(r['datetime'],tz)
            r['datetime'] = trim_datetime(r['datetime'],frequency)    
    
    key_fields = ['datetime','station_id']

    agg_key = {'trips':_sum, 'returns':_sum, 'num_bikes_available': _mean, 'num_docks_available':_mean, 'station':_first}
    res = _dict_groupby(res,key_fields,agg_key)
    
    return json_response(res)    
    
    
    
def get_system_trips(session, t1,t2, sys_name, frequency,tz):
    # First, query trips from stations
    qry = session.query(Measurement.datetime,func.sum(Measurement.trips), func.sum(Measurement.returns))
    qry = qry.filter(Measurement.datetime >= t1, Measurement.datetime <= t2)
    qry = qry.filter(System.name == sys_name)
    qry = qry.filter(Station.id != 'free_bikes')
    qry = qry.join(Station).join(System)


    qry = qry.group_by(Measurement.datetime)

    res = qry.all()
    
    #Special case for freq=t
    if frequency == 't':
        res = [{'datetime':to_local_time(res[0][0],tz),'trips':trips,'returns':returns} for time,trips,returns in res]
    else:
        res =  [{'datetime':to_local_time(time,tz),'trips':trips,'returns':returns} for time,trips,returns in res]
        for r in res:
            r['datetime'] = trim_datetime(r['datetime'],frequency)
        
    key_fields = ['datetime']
    agg_key = {'trips':_sum, 'returns':_sum, 'num_bikes_available': _mean, 'num_docks_available':_mean,
          'station_id': _first, 'station':_first}
    res = _dict_groupby(res,key_fields,agg_key)
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
    for r in res2:
        r['datetime'] = trim_datetime(r['datetime'],frequency)
    res2 = _dict_groupby(res2,key_fields,agg_key)
    
    
    # Add free bikes to main response
    for r in res:

        r['free bike trips'] = [r2 for r2 in res2 if r2['datetime']==r['datetime']]
        if len(r['free bike trips']) > 0 and 'trips' in r['free bike trips'][0].keys():
            r['free bike trips'] = r['free bike trips'][0]['trips']
        else:
            r['free bike trips'] = 0
    return  json_response(res)



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

def _first(x):

    return x[0]

def _sum(x):
    return sum(y for y in x if y is not None)

def _mean(x):
    s = sum(y for y in x if y is not None)
    l = len(x)
    if l == 0:
        return 0
    return int(s/l)
    
    
def _dict_groupby(res, key_fields, agg_key):

    
    # We need to group datetimes without tz info to avoid a bug during DST changes
    if 'datetime' in key_fields:
        for r in res:
            r['datetime_notz'] = r['datetime'].strftime('%Y-%m-%d %H')
            
        key_fields = [x for x in key_fields if x!='datetime']  + ['datetime_notz']
        agg_key['datetime']=_first
    
    key = lambda x: [x[field] for field in key_fields]

    res = sorted(res,key=key) # itertools.groupby takes a *sorted* iterable

    # First, gather each field for each grouped date
    res = [{'key':k,'data':[{y:x[y] for y in x.keys() if y not in key_fields} 
                                 for x in group]} 
             for k, group 
             in itertools.groupby(res, key=key)]



    def agg(r):
        return {field:agg_key[field]([y[field] for y in r['data']])  for field in agg_key.keys() if field in r['data'][0].keys()}

    def agg_keys(r):
        return {field:r['key'][i] for i,field in enumerate(key_fields) if field!='datetime_notz'}


    return [{**agg_keys(r), **agg(r)} for r in res]
 
def trim_datetime(datetime,frequency):
    if frequency == 'h':
        pass
    elif frequency == 'd':
        datetime = datetime.replace(hour=0)
    elif frequency == 'm':
        datetime = datetime.replace(hour=0, day=1)
    elif frequency == 'y':
        datetime = datetime.replace(hour=0, day=1, month=1)
        

    
    return datetime
    
def return_api_error():

    content = "Invalid API request :("
    return content, 400
