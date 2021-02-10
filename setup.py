#!/usr/bin/env python

from distutils.core import setup

setup(name='bikeraccoonAPI',
      version='0.1',
      description='Python utilities for tracking bikeshare systems',
      author='Mike Jarrett',
      author_email='msjarrett@gmail.com',
      url='raccoon.bike',
      packages=['bikeraccoonAPI'],
      scripts=[],
      install_requires = [
        'daemoner @ https://api.github.com/repos/mjarrett/daemoner/tarball/',
        'twitterer @  https://api.github.com/repos/mjarrett/twitterer/tarball/',
        'Flask==1.1.2',
        'Flask-Cors==3.0.10',
        'oauthlib==3.1.0',
        'pandas==1.2.1',
        'psutil==5.8.0',
        'pytz==2020.5',
        'requests==2.25.1',
        'requests-oauthlib==1.3.0',
        'SQLAlchemy==1.3.22',
        'timeout-decorator==0.5.0',
        'tweepy==3.10.0',
        'urllib3==1.26.3',
      ]
     )
