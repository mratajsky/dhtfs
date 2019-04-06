import logging
import multiprocessing
import os
import queue
import threading

import plyvel

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer, TNonblockingServer

from .thriftrpc.Rpc import Processor
from .thriftrpc.ttypes import Peer, StorageException

__all__ = ['RPC']

DATABASE_DIR_MODE = 0o755

logger = logging.getLogger(__name__)


class Handler:
    def __init__(self, db, pipe, results, condition):
        self._db = db
        self._dht_pipe = pipe
        self._results = results
        self._condition = condition

    def FindClosestPeers(self, key):
        logger.debug(f"RPC: FindClosestPeers({key.hex()})")
        ident = threading.get_ident()
        # Ask the DHT process to do the lookup and use the thread identifier to
        # find the result when it's sent back
        self._dht_pipe.send(('FindClosestPeers', ident, key))
        result = self._wait_for_result(ident)

        # The nodes returned are instances of kademlia.node.Node
        return [Peer(n.ip, n.port) for n in result]

    def Put(self, key, value):
        logger.debug(f"RPC: Put({key.hex()}, ...)")
        self._db.put(key, value)

    def Add(self, key, value):
        # logger.debug(f"RPC: Add({key.hex()}, ...)")
        # TODO: add to leaf bucket
        pass

    def Get(self, key):
        logger.debug(f"RPC: Get({key.hex()})")
        value = self._db.get(key)
        if value is None:
            # get() returns None when key is not in the database
            raise StorageException(404, 'Key not found')
        return value

    def GetLatest(self, key):
        # TODO: get item from leaf bucket with latest search key
        pass

    def GetRange(self, key, searchKeyLow, searchKeyHigh):
        # TODO: get items in range from leaf bucket
        pass

    def _wait_for_result(self, wanted_ident):
        with self._condition:
            while self._results.get(wanted_ident) is None:
                self._condition.wait()
            return self._results.pop(wanted_ident)


class RPC:
    def __init__(self, pipe, options):
        self._dht_pipe = pipe
        self._host = options.host
        self._port = options.port
        self._process_queue = multiprocessing.Queue()
        self._process = multiprocessing.Process(
            target=self._worker, args=(self._process_queue,))

        # Make sure the database folder exists
        os.makedirs(options.database,
                    mode=DATABASE_DIR_MODE,
                    exist_ok=True)
        if options.database_clean:
            plyvel.destroy_db(options.database)
        self._db = plyvel.DB(options.database, create_if_missing=True)

        logger.debug(f'Using database directory: {options.database}')

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port

    @property
    def running(self):
        return self._process.is_alive()

    def run(self):
        '''Run the RPC process.'''
        self._process.start()

    def stop(self):
        '''Stop the RPC process.'''
        self._process_queue.put(1)

    def _worker(self, process_queue):
        condition = threading.Condition()
        results = {}

        def server():
            '''Main thrift server thread.'''
            handler = Handler(self._db, self._dht_pipe, results, condition)
            processor = Processor(handler)
            transport = TSocket.TServerSocket(host=self._host, port=self._port)
            tfactory = TTransport.TBufferedTransportFactory()
            pfactory = TBinaryProtocol.TBinaryProtocolFactory()

            server = TServer.TThreadPoolServer(
                processor, transport, tfactory, pfactory, daemon=True)
            server.serve()
        threading.Thread(target=server, daemon=True).start()

        def pipe_watch():
            '''Thread to collect responses from DHT.'''
            while True:
                try:
                    ident, result = self._dht_pipe.recv()
                    with condition:
                        results[ident] = result
                        condition.notify_all()
                except EOFError:
                    break
        threading.Thread(target=pipe_watch, daemon=True).start()

        try:
            # Reading something from the process queue signals the process
            # to quit, which is done by simply returning as the threads above
            # are marked as daemon threads
            process_queue.get()
        except:
            pass
