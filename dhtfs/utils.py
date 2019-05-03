import logging
from collections import namedtuple

from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

__all__ = ['defaults',
           'setup_logging']

DEFAULT_HOST = '127.0.0.1'
DEFAULT_PORT = 9090
DEFAULT_NAME = 'default'
DEFAULT_BLOCK_SIZE = 65536
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


def thrift_serialize(obj):
    '''Serialize the given Thrift object and return a binary value.'''
    transport = TTransport.TMemoryBuffer()
    protocol = TBinaryProtocol.TBinaryProtocolAccelerated(transport)

    obj.write(protocol)
    return transport.getvalue()


def thrift_unserialize(value, obj):
    '''Unserialize the given binary value into the given object.'''
    transport = TTransport.TMemoryBuffer(value)
    protocol = TBinaryProtocol.TBinaryProtocolAccelerated(transport)

    obj.read(protocol)
    return obj


def naming_func(label):
    '''Return the DHT key of the label'''
    last = label[-1]
    for i in range(len(label)-1,-1,-1):
        if(last != label[i]):
            break
    
    return label[:i+1]
