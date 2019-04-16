#!/usr/bin/env python3

from setuptools import setup

setup(name='dhtfs',
      version='1.0',
      description='DHT tool and peer for dhtfs',
      scripts=['bin/dhtfs', 'bin/dhtfs-peer', 'bin/dhtfs-spawn-peers'],
      packages=['dhtfs'],
      zip_safe=False)
