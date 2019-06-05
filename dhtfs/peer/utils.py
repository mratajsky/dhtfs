import logging
import os
import random
from collections import namedtuple

import appdirs
from kademlia.utils import digest

__all__ = ['defaults',
           'get_default_database_path',
           'get_pid_path',
           'parse_node_id',
           'setup_database']

DEFAULT_HOST = '0.0.0.0'
DEFAULT_PORT = 9090
DEFAULT_ALPHA = 2
DEFAULT_KSIZE = 5
DEFAULT_DB_TYPE = 'redis'

DATABASE_DIR_MODE = 0o755

Defaults = namedtuple('Defaults', ['host', 'port', 'alpha', 'ksize', 'db_type'])
defaults = Defaults(host=DEFAULT_HOST,
                    port=DEFAULT_PORT,
                    alpha=DEFAULT_ALPHA,
                    ksize=DEFAULT_KSIZE,
                    db_type=DEFAULT_DB_TYPE)


class Database:
    '''Wrapper around Redis/LevelDB.'''

    def __init__(self, db_type='redis', path=None, flush=False):
        self._type = db_type
        self._path = path
        if db_type == 'redis':
            import redis
            self._db = redis.Redis.from_url(self._path)
            if flush:
                self._db.flushdb()
            logging.debug(f'Using Redis URL: {self._path}')
        elif db_type == 'leveldb':
            import plyvel
            # Make sure the database folder exists
            os.makedirs(path, mode=DATABASE_DIR_MODE, exist_ok=True)
            if flush:
                plyvel.destroy_db(self._path)
            self._db = plyvel.DB(path, create_if_missing=True)
            logging.debug(f'Using LevelDB directory: {self._path}')
        else:
            raise ValueError(f'Invalid database type: {db_type}')

    def get(self, key):
        return self._db.get(key)

    def put(self, key, value):
        if self._type == 'redis':
            return self._db.set(key, value)
        else:
            return self._db.put(key, value)


def get_default_database_path(port):
    '''Get the default database path based on the given port.'''
    # This becomes something like /home/user/.cache/dhtfs/db-port
    return os.path.join(appdirs.user_cache_dir(), 'dhtfs', f'db-{port}')


def get_pid_path(app, host, port):
    '''Get PID file path.'''
    return os.path.join(appdirs.user_cache_dir(), 'dhtfs', f'{app}-{host}-{port}.pid')


def get_random_node_id():
    # Taken from kademlia.network.Server
    return digest(random.getrandbits(255))


def parse_node_id(node_id):
    '''Return the given hex node ID as a bytes object.'''
    # Make sure the node ID is exactly 20 bytes, left padding it with
    # zeros if needed
    return bytes.fromhex(node_id)[:20].rjust(20, b'\x00')


def setup_database(port, db_type, db_conn=None, db_clean=False, node_id=None):
    if db_type == 'redis':
        path = db_conn or 'redis://localhost/0'
    elif db_type == 'leveldb':
        path = db_conn or get_default_database_path(port)
    else:
        raise ValueError(f'Invalid database type: {db_type}')

    database = Database(db_type, path, db_clean)

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
