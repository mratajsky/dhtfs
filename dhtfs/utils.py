import logging
import os
import random
from collections import namedtuple

import appdirs

__all__ = ['defaults',
           'get_default_database_path',
           'get_random_port',
           'parse_node_id',
           'setup_logging']

DEFAULT_HOST = '0.0.0.0'
DEFAULT_ALPHA = 2
DEFAULT_KSIZE = 5

Defaults = namedtuple('Defaults', ['host', 'alpha', 'ksize'])
defaults = Defaults(host=DEFAULT_HOST,
                    alpha=DEFAULT_ALPHA,
                    ksize=DEFAULT_KSIZE)


def get_default_database_path(port):
    '''Get the default database path based on the given port.'''
    # This becomes something like /home/user/.cache/dhtfs/db-port
    return os.path.join(appdirs.user_cache_dir(), 'dhtfs', f'db-{port}')


def get_random_port():
    '''Pick a random port from a reasonable range.'''
    return random.randint(10000, 65000)


def parse_node_id(node_id):
    '''If the node ID is not None, convert it from a hex string to a bytes object.'''
    if node_id is not None:
        return bytes.fromhex(node_id)


def setup_logging(port, verbosity):
    logformat = f'[{port}] %(asctime)s: %(message)s'
    if verbosity:
        if verbosity > 1:
            loglevel = logging.DEBUG
        else:
            loglevel = logging.INFO
        logging.basicConfig(level=loglevel, format=logformat)
    else:
        # Use the default logging level
        logging.basicConfig(format=logformat)
