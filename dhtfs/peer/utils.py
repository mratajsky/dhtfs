import logging
import os
import random
from collections import namedtuple

import appdirs
import plyvel
from kademlia.utils import digest

__all__ = ['defaults',
           'get_default_database_path',
           'get_random_port',
           'parse_node_id',
           'setup_database']

DEFAULT_HOST = '0.0.0.0'
DEFAULT_ALPHA = 2
DEFAULT_KSIZE = 5

DATABASE_DIR_MODE = 0o755

Defaults = namedtuple('Defaults', ['host', 'alpha', 'ksize'])
defaults = Defaults(host=DEFAULT_HOST,
                    alpha=DEFAULT_ALPHA,
                    ksize=DEFAULT_KSIZE)


def get_default_database_path(port):
    '''Get the default database path based on the given port.'''
    # This becomes something like /home/user/.cache/dhtfs/db-port
    return os.path.join(appdirs.user_cache_dir(), 'dhtfs', f'db-{port}')


def get_random_node_id():
    # Taken from kademlia.network.Server
    return digest(random.getrandbits(255))


def get_random_port():
    '''Pick a random port from a reasonable range.'''
    return random.randint(10000, 65000)


def parse_node_id(node_id):
    '''Return the given hex node ID as a bytes object.'''
    # Make sure the node ID is exactly 20 bytes, left padding it with
    # zeros if needed
    return bytes.fromhex(node_id)[:20].rjust(20, b'\x00')


def setup_database(port, node_id=None, path=None, clean=False):
    if path is None:
        path = get_default_database_path(port)
    # Make sure the database folder exists
    os.makedirs(path, mode=DATABASE_DIR_MODE, exist_ok=True)
    if clean:
        plyvel.destroy_db(path)
    database = plyvel.DB(path, create_if_missing=True)
    logging.debug(f'Using database directory: {path}')

    # Node ID is persisted in the database
    current_node_id = database.get(b'dhtfs-node-id')
    if current_node_id is not None:
        if node_id is not None and current_node_id != node_id:
            logging.warning(
                'Ignoring the provided node ID as it is already stored')
        node_id = current_node_id
    else:
        if node_id is None:
            node_id = get_random_node_id()
        database.put(b'dhtfs-node-id', node_id)

    return database, node_id
