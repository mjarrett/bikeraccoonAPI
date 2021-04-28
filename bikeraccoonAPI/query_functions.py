import pandas as pd
import json
import urllib.request
import datetime as dt
import timeout_decorator
import ssl

import logging
logger = logging.getLogger("Rotating Log")

def get_station_status_url(sys_url):
    with urllib.request.urlopen(sys_url,context=ssl._create_unverified_context()) as url:
        data = json.loads(url.read().decode())
    return [x for x in data['data']['en']['feeds'] if x['name']=='station_status'][0]['url']      

def get_station_info_url(sys_url):
    with urllib.request.urlopen(sys_url,context=ssl._create_unverified_context()) as url:
        data = json.loads(url.read().decode())
    return [x for x in data['data']['en']['feeds'] if x['name']=='station_information'][0]['url']   


def get_system_info_url(sys_url):
    with urllib.request.urlopen(sys_url,context=ssl._create_unverified_context()) as url:
        data = json.loads(url.read().decode())
    return [x for x in data['data']['en']['feeds'] if x['name']=='system_information'][0]['url']

@timeout_decorator.timeout(30) 
def query_system_info(sys_url):
    url = get_system_info_url(sys_url)

    with urllib.request.urlopen(url, context=ssl._create_unverified_context()) as data_url:
        data = json.loads(data_url.read().decode())  

    return data

    
@timeout_decorator.timeout(30) 
def query_station_status(sys_url):
    """
    Query station_status.json
    """
    
    url = get_station_status_url(sys_url)


    with urllib.request.urlopen(url, context=ssl._create_unverified_context()) as data_url:
        data = json.loads(data_url.read().decode())

    try:
        df = pd.DataFrame(data['data']['stations'])
    except KeyError:
        df = pd.DataFrame(data['stations'])
        
    df = df.drop_duplicates(['station_id','last_reported'])
    try:
        df['datetime'] = data['last_updated']
        df['datetime'] = df['datetime'].map(lambda x: dt.datetime.utcfromtimestamp(x))
    except KeyError:
        df['datetime'] = dt.datetime.utcnow()
    
    df['datetime'] = df['datetime'].dt.tz_localize('UTC')
    
    df = df[['datetime','num_bikes_available','num_docks_available','is_renting','station_id']]


    return df

@timeout_decorator.timeout(30) 
def query_station_info(sys_url):
    
    """
    Query station_information.json
    """
    url = get_station_info_url(sys_url)

    with urllib.request.urlopen(url, context=ssl._create_unverified_context()) as data_url:
        data = json.loads(data_url.read().decode())  

    try:
        df =  pd.DataFrame(data['data']['stations'])
    except KeyError:
        df =  pd.DataFrame(data['stations'])
    return df[['name','station_id','lat','lon']]

@timeout_decorator.timeout(30) 
def query_free_bikes(sys_url):
    
    """
    Query free_bikes.json
    """
    
    url = get_free_bike_url(sys_url)

    with urllib.request.urlopen(url, context=ssl._create_unverified_context()) as data_url:
        data = json.loads(data_url.read().decode())

    try:    
        df = pd.DataFrame(data['data']['bikes'])
    except KeyError:
        df = pd.DataFrame(data['bikes'])
        
    df['bike_id'] = df['bike_id'].astype(str)

    try:
        df['datetime'] = data['last_updated']
        df['datetime'] = df['datetime'].map(lambda x: dt.datetime.utcfromtimestamp(x))
    except KeyError:
        df['datetime'] = dt.datetime.utcnow()
    
    df['datetime'] = df['datetime'].dt.tz_localize('UTC')
    
    
    df = df[['bike_id','lat','lon','datetime']]

    return df

    
def get_free_bike_url(sys_url):
    with urllib.request.urlopen(sys_url, context=ssl._create_unverified_context()) as url:
        data = json.loads(url.read().decode())
    return [x for x in data['data']['en']['feeds'] if x['name']=='free_bike_status'][0]['url']