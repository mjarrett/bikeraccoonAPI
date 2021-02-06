import sys


from daemoner import Daemon, log
import twitterer

from bikeracoonAPI import app





def g():
    twitterer.warning(f"{__name__} is shutting down")

fkwargs = {'debug':False,
           'host':'127.0.0.1',
           'port':8001
          }

try:
    d = Daemon(f=app.run, g=g, fkwargs=fkwargs, pidfilename='bikeracoon-api-daemon.pid')
    d.run()
except:
    g()
    
