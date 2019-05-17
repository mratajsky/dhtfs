#!/usr/bin/env python3

from setuptools import setup, find_packages

setup(name='dhtfs',
      version='1.0',
      description='DHT tool and peer for dhtfs',
      scripts=['bin/dhtfs', 'bin/dhtfs-peer', 'bin/dhtfs-spawn-peers', 'bin/test-peer'],
      packages=find_packages(),
      zip_safe=False)
