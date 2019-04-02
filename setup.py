#!/usr/bin/env python3

from setuptools import setup

setup(name='dhtfs-peer',
      version='1.0',
      description='DHT peer for dhtfs',
      scripts=['bin/dhtfs-peer'],
      packages=['dhtfs'],
      zip_safe=False)
