import sys

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from bikeraccoonAPI import Measurement, System, Station, Trip

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
    
    db_file = sys.argv[3]
    engine = create_engine(f'sqlite:///{db_file}', echo=False)     
    session = Session(engine)
    

    if sys.argv[1] == 'add':
    
        systems_file = sys.argv[2]
        systems = load_systems(systems_file)

        for system in systems.values():
            add_system(system, session)
            
            
            
    elif sys.argv[1] == 'activate':
        sys_name = sys.argv[2]
        
        sys_obj = session.query(System).filter_by(name=sys_name).first()
        
        sys_obj.is_tracking = True
        session.add(sys_obj)
        session.commit()
            
    elif sys.argv[1] == 'deactivate':
        sys_name = sys.argv[2]
        
        sys_obj = session.query(System).filter_by(name=sys_name).first()
        
        sys_obj.is_tracking = False
        session.add(sys_obj)
        session.commit()
        