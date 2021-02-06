import sys


from daemoner import Daemon, log
import twitterer

from bikeracoon_api import app





def g():
    twitterer.warning("bikeracoon_api is shutting down")

fkwargs = {'debug':False,
           'host':'127.0.0.1',
           'port':8001
          }


d = Daemon(f=app.run, g=g, fkwargs=fkwargs, pidfilename='bikeracoon-api-daemon.pid')

d.run()
