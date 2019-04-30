import bisect
import logging
import multiprocessing
import os
import queue
import threading
from contextlib import contextmanager

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

from ..thrift.rpc.Rpc import Processor
from ..thrift.rpc.ttypes import Peer, Bucket, BucketValue, StorageException
from ..utils import thrift_serialize, thrift_unserialize

logger = logging.getLogger(__name__)

# Make BucketValue sortable
BucketValue.__lt__ = lambda self, other: self.search_key < other.search_key


class Handler:
    def __init__(self, db, pipe, results, condition):
        self._db = db
        self._dht_pipe = pipe
        self._bucket_locks = {}
        self._bucket_lock_global = threading.Lock()
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

    def Add(self, key, search_key_min, search_key_max, value: BucketValue):
        logger.debug(f"RPC: Add({key.hex()}, ...)")
        with self._lock_bucket(key):
            bucket = self._db.get(key)
            if bucket is not None:
                bucket = thrift_unserialize(bucket, Bucket())
                # TODO: this is not total ordering
                bisect.insort_right(bucket.values, value)
            else:
                bucket = Bucket(search_key_min=search_key_min,
                                search_key_max=search_key_max,
                                values=[value])
            self._db.put(key, thrift_serialize(bucket))

    def Get(self, key):
        logger.debug(f"RPC: Get({key.hex()})")
        value = self._db.get(key)
        if value is None:
            # get() returns None when key is not in the database
            raise StorageException(404, 'Key not found')
        return value

    def GetLatest(self, key):
        return self._get_nonempty_bucket(key).values[-1]

    def GetLatestMax(self, key, search_key_max):
        bucket = self._get_nonempty_bucket(key)
        idx = bisect.bisect_right(bucket.values, search_key_max)
        if idx == 0:
            raise StorageException(404, 'No matching value found')
        return bucket.values[idx - 1]

    def GetRange(self, key, search_key_min, search_key_max):
        try:
            bucket = self._get_nonempty_bucket(key)
            idx_min = bisect.bisect_left(bucket.values, search_key_min)
            if idx_min == len(bucket.values):
                return []
            idx_max = bisect.bisect_right(bucket.values, search_key_max)
            if idx_max == 0:
                return []
            return bucket.values[idx_min:idx_max]
        except StorageException:
            # Empty bucket
            return []

    def _get_nonempty_bucket(self, key) -> Bucket:
        bucket = self._db.get(key)
        if bucket is not None:
            bucket = thrift_unserialize(bucket, Bucket())
        if bucket is None or len(bucket.values) == 0:
            raise StorageException(404, 'Bucket is empty')
        return bucket

    @contextmanager
    def _lock_bucket(self, key):
        with self._bucket_lock_global:
            lock = self._bucket_locks.get(key)
            if lock is None:
                lock = self._bucket_locks[key] = threading.Lock()
        with lock:
            yield

    def _wait_for_result(self, wanted_ident):
        with self._condition:
            while self._results.get(wanted_ident) is None:
                self._condition.wait()
            return self._results.pop(wanted_ident)


class RPC:
    def __init__(self, pipe, options):
        self._dht_pipe = pipe
        self._db = options.database
        self._host = options.host
        self._port = options.port
        self._process_queue = multiprocessing.Queue()
        self._process = multiprocessing.Process(
            target=self._worker, args=(self._process_queue,))

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
            pfactory = TBinaryProtocol.TBinaryProtocolAcceleratedFactory()

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
