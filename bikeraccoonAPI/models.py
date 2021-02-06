from sqlalchemy import Table, Column, Integer, ForeignKey, String, DateTime, Float, Boolean
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
import datetime as dt

Base = declarative_base()

# Raw tables are kept for each system separately


class System(Base):
    __tablename__ = 'system'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    tz = Column(String)
    url = Column(String)
    is_tracking = Column(Boolean)
    tracking_start = Column(DateTime)
    tracking_end = Column(DateTime)
    has_system_data = Column(Boolean)
    system_data_start = Column(DateTime)
    system_data_end = Column(DateTime)
    stations = relationship("Station", back_populates='system')
    
    def __repr__(self):
        return f"<System: name={self.name}>"
    
    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

class Station(Base):
    __tablename__ = 'station'
    id = Column(Integer, primary_key=True)
    created_date = Column(DateTime, default=dt.datetime.utcnow)  
    station_id = Column(String)
    name = Column('name',String)
    lat = Column('lat', Float)
    lon = Column('lon', Float)
    measurements = relationship("Measurement")
    system_id = Column(Integer, ForeignKey('system.id'), index=True)
    system = relationship("System", back_populates='stations')
    measurements = relationship("Measurement", back_populates='station')
    
    def __repr__(self):
        return f"<Station: name={self.name} system={self.system.name}>"
    
    def as_dict(self):
        cols = ["created_date", "name",'lat','lon','station_id']
        r =  {c.name: getattr(self, c.name) for c in self.__table__.columns if c.name in cols}
        r['system'] = self.system.name
        r['created_date'] = str(self.created_date)
        return r

class Measurement(Base):
    __tablename__ = 'measurement'
    id = Column(Integer, primary_key=True)
    datetime = Column('datetime',DateTime, index=True)
    trips = Column('trips',Integer)
    returns = Column('returns',Integer)
    num_bikes_available = Column('num_bikes_available',Integer)
    num_docks_available = Column('num_docks_available',Integer)
    station_id = Column('station_id',Integer, ForeignKey('station.id'), index=True)
    station = relationship("Station", back_populates='measurements')
    
    def __repr__(self):
        return f"<Measurement: {self.station.system.name} {self.station.name} {self.datetime}>"
    
    def as_dict(self):
        r =  {c.name: getattr(self, c.name) for c in self.__table__.columns}
        r['station'] = self.station.name
        r['station_id'] = self.station.station_id
        r['datetime'] = self.datetime
        del r['id']
        return r
    
    
    
class Trip(Base):
    __tablename__ = 'trip'
    id = Column(Integer, primary_key=True)
    departure_time = Column('departure_time',DateTime, index=True)
    return_time = Column('return_time',DateTime, index=True)
    member = Column('member',Boolean)
    member_type = Column('member_type',String)
    duration = Column('duration',Integer) #seconds
    distance = Column('distance',Integer) #meters
    stopovers = Column('stopovers',Integer)
    stopover_duration = Column('stopover_duration',Integer) # seconds
    departure_station_id = Column('departure_station_id',Integer, ForeignKey('station.id'), index=True)
    departure_station = relationship("Station", foreign_keys=[departure_station_id])
    return_station_id = Column('return_station_id',Integer, ForeignKey('station.id'), index=True)
    return_station = relationship("Station", foreign_keys=[return_station_id])
    system_id = Column('system_id', Integer, ForeignKey('system.id'), index=True)
    system = relationship('System', foreign_keys=[system_id])
    
    
    
    def __repr__(self):
        return f"<Trip: {self.id} {self.departure_station.system.name} {self.departure_station.name} {self.departure_time}>"
