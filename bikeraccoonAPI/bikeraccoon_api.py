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
from .api_functions import *

engine = create_engine(f'sqlite:///bikeraccoon.db', echo=False) 

app = Flask(__name__)
CORS(app) #Prevents CORS errors 


    
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
    station_id = request.args.get('station', default=None, type=str)
   
    # Assume provided time is in system timezone, convert to UTC
    tz = session.query(System.tz).filter_by(name=sys_name).first()[0]
    try:
        t1 = to_utc(string_to_datetime(t1),tz)
        t2 = to_utc(string_to_datetime(t2),tz)
    except:
        return return_api_error()
    
    if station_id is None:      
        return get_system_trips(session, t1,t2, sys_name, frequency,tz)
  
    if station_id == 'all':
        return get_all_stations_trips(session, t1,t2,sys_name,frequency,tz)
    
    # get list of station ids for system
    station_ids = session.query(Station.station_id).join(System).filter(System.name==sys_name,Station.id!='free_bikes').all()
    station_ids = [ x[0] for x in station_ids]
    
    if station_id in station_ids:
        return get_station_trips(session,t1,t2,sys_name,station_id,frequency,tz)

    
    return return_api_error()


    
if __name__ == '__main__':
    app.run(debug=True, host = '127.0.0.1', port = 8001)


    
