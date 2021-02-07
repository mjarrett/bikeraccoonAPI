#!/usr/bin/env python3

from flask_cors import CORS
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

engine = create_engine(f'sqlite:///bikeraccoon.db', echo=False) 

app = Flask(__name__)
CORS(app) #Prevents CORS errors 

def validate_key(pubkey):

    with open('keys.txt') as f:
        for line in f.readlines():
            salt = line[256:].strip()
            key = line[:256].strip()
            computed_key = hashlib.pbkdf2_hmac('sha256',pubkey.encode('utf-8'),salt.encode('utf-8'),100000,dklen=128)
            if line[:256] == computed_key.hex():
                return True
    return False
    
@app.route('/favicon.ico')
def favicon():
    return send_from_directory(os.path.join(app.root_path, 'static'),
                          'favicon.ico',mimetype='image/vnd.microsoft.icon')    

@app.route('/')
def default():
    return "Default landing page"

@app.route('/systems', methods=['GET'])
def get_systems():
    
    session = Session(engine)


    qry =  session.query(System)
    res = qry.all()
    res = [x.as_dict() for x in res]
    
    return json_response(res)

@app.route('/stations', methods=['GET'])
def get_stations():
    
    session = Session(engine)

    
    sys_name = request.args.get('system', default=None,type=str)
    
    if sys_name is None:
        return # Add a 404
    qry =  session.query(Station).join(System).filter(System.name==sys_name,Station.station_id!='free_bikes').all()
    res = [x.as_dict() for x in qry]

    return json_response(res)

@app.route('/activity', methods=['GET'])
def get_activity():
    
    
    session = Session(engine)

    
    sys_name = request.args.get('system', default=None,type=str)
    t1 = request.args.get('start', default=None, type=str)
    t2 = request.args.get('end', default=None, type=str)
    frequency = request.args.get('frequency', default='h', type=str)
    station = request.args.get('station', default=None, type=str)
   
    # Assume provided time is in system timezone, convert to UTC
    tz = session.query(System.tz).filter_by(name=sys_name).first()[0]

    
    # Convert times to UTC
    try:
        t1 = to_utc(string_to_datetime(t1),tz)
        t2 = to_utc(string_to_datetime(t2),tz)
    except:
        return return_api_error()
    
    if station is None:
        
        # First, query trips from stations
        qry = session.query(Measurement.datetime,func.sum(Measurement.trips), func.sum(Measurement.returns))
        qry = qry.filter(Measurement.datetime >= t1, Measurement.datetime <= t2)
        qry = qry.filter(System.name == sys_name)
        qry = qry.filter(Station.id != 'free_bikes')
        qry = qry.join(Station).join(System)


        qry = qry.group_by(Measurement.datetime)
        
        res =  [{'datetime':to_local_time(time,tz),'trips':trips,'returns':returns} for time,trips,returns in qry.all()]
        res = dict_groupby(res,frequency)
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
        res2 = dict_groupby(res2,frequency)
      
        # Add free bikes to main response
        for r in res:

            r['free bike trips'] = [r2 for r2 in res2 if r2['datetime']==r['datetime']]
            if len(r['free bike trips']) > 0:
                r['free bike trips'] = r['free bike trips'][0]['trips']
            else:
                r['free bike trips'] = 0
        res = json_response(res)
        return res
  

    qry = session.query(Measurement)
    qry = qry.filter(Measurement.datetime >= t1, Measurement.datetime <= t2)
    qry = qry.filter(System.name == sys_name)
    qry = qry.join(Station).join(System)
        
    if station == 'all':
        qry = qry.filter(Station.station_id!='free_bikes')
        res = [x.as_dict() for x in qry.all()]
        for r in res:
            r['datetime'] = to_local_time(r['datetime'],tz)
        res = dict_groupby(res,frequency)
        return json_response(res)
    
    # get list of station ids for system
    station_ids = session.query(Station.station_id).join(System).filter(System.name==sys_name,Station.id!='free_bikes').all()
    station_ids = [ x[0] for x in station_ids]
    
    if station in station_ids:
        qry = qry.filter(Station.station_id==station)
        res = [x.as_dict() for x in qry.all()]
        res = dict_groupby(res,frequency)
        return json_response(res)
    
    return return_api_error()

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

def dict_groupby(res, frequency):
    

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
        res = [{'datetime':k,'trips':[(x['trips'],x['returns']) for x in group]} for k, group 
                 in itertools.groupby(res, key=key)]
        res = [{'datetime':x['datetime'], 'trips':sum(y[0] for y in x['trips']),'returns':sum(y[1] for y in x['trips'])} for x in res]
    return res
 
    
def return_api_error():

    content = "Invalid API request :("
    return content, 400
    
if __name__ == '__main__':
    app.run(debug=True, host = '127.0.0.1', port = 8001)


    
