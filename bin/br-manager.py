#!/usr/bin/env python3

import sys
import json
import datetime as dt

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from bikeraccoonAPI import Measurement, System, Station, Trip, Base


def load_system_interactive():
    name = input('name: ')
    tz = input('timezone: ')
    url = input('url: ')
   
    brand = input('brand: ')
    city = input('city: ')
    province = input('province: ')
    country = input('country: ')
    
    system = {'name':name, 'tz':tz, 'url':url,'brand':brand, 'city':city,'province':province,'country':country}
    return system



def load_systems(systems_file):

    with open(systems_file) as f:
        systems = json.load(f)

    return systems

def add_system(system,session):

    # This one uses the system dictionary, NOT the system ORM object
    
    
    # If system exists, raise error
    sys_objs = session.query(System).filter_by(name=system['name']).all()
    if len(sys_objs) > 0:
        raise ValueError("System already exists in database")
        
    


    # If system doesn't exist, create it
    elif len(sys_objs) == 0:
        sys_obj = System(**system)
        
    else:
        #update system object to match content in systems.json
        sys_obj = sys_objs[0]
        for key, value in system.items():
            setattr(sys_obj, key, value)

        

    # set tracking to on
    sys_obj.is_tracking = True
    sys_obj.tracking_start = dt.datetime.utcnow() 
    
    session.add(sys_obj)
    
    session.commit()
    

if __name__ == '__main__':
    
    import argparse

    parser = argparse.ArgumentParser(description='Tools to manage a bikeraccoon instance')
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--init", action="store_true",
                      help="Initialize a new bikeraccoon database")
    group.add_argument("--add", action="store_true",
                      help="Add a new system to DB")
    group.add_argument("--activate", action="store_true",
                      help="Activate a deactivated system")
    group.add_argument("--deactivate", action="store_true",
                      help="Deactivate an active system")
    parser.add_argument("-d", "--database", type=str,
                    help="specify path to database", default='bikeraccoon.db')
    parser.add_argument("-f", "--file", type=str,
                    help="file with system information", default=None)
    parser.add_argument("-s", "--system", type=str,
                    help="system name")


    args = parser.parse_args()
    
    # Connect to DB
    engine = create_engine(f"sqlite:///{args.database}")
    session = Session(engine)
    
    
    if args.init:
        Base.metadata.create_all(engine)  # Create ORM tables if they don't exist
    
    elif args.add:
    
        # If we provided a file
        if args.file is not None:

            systems = load_systems(args.file)

            for system in systems.values():
                add_system(system, session)
            
        
        # If we didn't provide a file, go interactive
        system = load_system_interactive()   
        add_system(system,session)
            
            
    elif args.activate:
        
        sys_obj = session.query(System).filter_by(name=args.system).first()
        
        sys_obj.is_tracking = True
        session.add(sys_obj)
        session.commit()
            
    elif args.deactivate:
        
        sys_obj = session.query(System).filter_by(name=args.system).first()
        
        sys_obj.is_tracking = False
        session.add(sys_obj)
        session.commit()
        