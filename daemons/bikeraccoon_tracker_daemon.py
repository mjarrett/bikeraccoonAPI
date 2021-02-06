import sys
import traceback

from daemoner import Daemon, log
import twitterer

from bikeraccoonAPI import tracker





def g():
    return
    twitterer.warning("gbfs-tracker is shutting down")

fkwargs = {'systems_file':'systems.json',
           'db_file':'bikeraccoon.db',
           #'log_file':'logs/bikeracoon_tracker.log', 
           'log_file':None,
           'interval':20,
           'station_check_hour':12
          }


d = Daemon(f=tracker, g=g, fkwargs=fkwargs, pidfilename='bikeraccoon_tracker_daemon.pid')

try:
    d.run()
except Exception as e:
    tb = traceback.format_exc()
    print(f"Daemon exiting with error: {e}")
    print(tb)
    
    g()
