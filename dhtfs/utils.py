import logging
from collections import namedtuple

__all__ = ['defaults',
           'setup_logging']

DEFAULT_HOST = '127.0.0.1'
DEFAULT_PORT = 9090
DEFAULT_NAME = 'default'
DEFAULT_BLOCK_SIZE = 4096
DEFAULT_MODEL = 0

Defaults = namedtuple(
    'Defaults', ['host', 'port', 'name', 'block_size', 'model'])
defaults = Defaults(host=DEFAULT_HOST,
                    port=DEFAULT_PORT,
                    name=DEFAULT_NAME,
                    block_size=DEFAULT_BLOCK_SIZE,
                    model=DEFAULT_MODEL)


def setup_logging(verbosity):
    logformat = '[%(process)d] %(asctime)s: %(message)s'
    if verbosity:
        if verbosity > 1:
            loglevel = logging.DEBUG
        else:
            loglevel = logging.INFO
        logging.basicConfig(level=loglevel, format=logformat)
    else:
        # Use the default logging level
        logging.basicConfig(format=logformat)
