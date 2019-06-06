import logging
import multiprocessing
import os
import queue
import threading
from contextlib import contextmanager
import random

from kademlia.utils import digest

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol, TCompactProtocol
from thrift.server import TServer

from ..thrift.rpc.Rpc import Processor
from ..thrift.rpc.ttypes import Peer, Bucket, BucketValue, BucketKeys, StorageException
from ..client import Client
from ..utils import *

logger = logging.getLogger(__name__)


class Handler:
    def __init__(self, db, pipe, results, condition):
        self._db = db
        self._dht_pipe = pipe
        self._bucket_locks = {}
        self._bucket_lock_global = threading.Lock()
        self._put_latest_lock = threading.Lock()
        self._results = results
        self._condition = condition

    def FindClosestPeers(self, key):
        logger.info(f"RPC: FindClosestPeers({key.hex()})")
        return self._find_closest_peers(key)

    def _find_closest_peers(self, key):
        ident = threading.get_ident()
        # Ask the DHT process to do the lookup and use the thread identifier to
        # find the result when it's sent back
        self._dht_pipe.send(('FindClosestPeers', ident, key))
        result = self._wait_for_result(ident)

        # The nodes returned are instances of kademlia.node.Node
        return [Peer(n.ip, n.port) for n in result]

    def FindKey(self, ident, search_key):
        logger.info(f"RPC: FindKey({ident}, {search_key})")
        key = self._lookup(ident, search_key)
        logger.debug(f'RPC: FindKey({ident}, {search_key}) -> {key}')
        return digest(key)

    def _find_search_keys(self, key):
        closest = self._find_closest_peers(key)
        for peer in closest:
            client = Client(peer.host, peer.port)
            client.connect()
            try:
                return client.GetBucketKeys(key)
            except StorageException:
                pass

    def _lookup(self, ident, search_key):
        lower_bound = 2
        upper_bound = DEFAULT_TREE_DEPTH + 1
        label = float_to_bin_no_dot(search_key_to_interval(search_key))

        while upper_bound >= lower_bound:
            mid_point = (lower_bound + upper_bound) // 2
            prefix = label[0:mid_point]
            dht_key = naming_func(f'{ident}:{prefix}')

            bucket_keys = self._find_search_keys(digest(dht_key))
            if bucket_keys is None:
                if upper_bound == len(prefix):
                    break
                upper_bound = len(prefix)
            else:
                key_min, key_max = bucket_keys.search_key_min, bucket_keys.search_key_max
                if search_key >= key_min and search_key < key_max:
                    return dht_key
                lower_bound = len(next_naming_func(prefix, label))

        return f'{ident}:#'

    def bucketSplitter(self, bucket, name):
        # split
        if (bucket.search_key_min + 1) >= bucket.search_key_max:
            return bucket

        midPoint = (bucket.search_key_max + bucket.search_key_min) // 2 
        if midPoint * 2 < bucket.search_key_max:
            midPoint = midPoint + 1
        bucketLeft = Bucket(search_key_min=bucket.search_key_min,
                            search_key_max=midPoint,
                            values = [])
        bucketRight = Bucket(search_key_min=midPoint,
                            search_key_max=bucket.search_key_max,
                            values = []) 
        for x in bucket.values:
            if x.search_key < midPoint:
                bucketLeft.values.append(x)
            elif x.search_key < bucket.search_key_max:
                bucketRight.values.append(x)
            else:
                logger.error(f'Error: key {x.search_key} in a wrong bucket: {bucket.search_key_min}-{bucket.search_key_max}')

        label = get_label(bucket.search_key_min, bucket.search_key_max)
  
        dht_key = digest(f'{name}:{label}')
        peers = self._find_closest_peers(dht_key)
  
        indx =random.randint(0,len(peers)-1)
        client = Client(peers[indx].host, peers[indx].port)

        if label[-1] == '0':
            if len(bucketLeft.values) > DEFAULT_BUCKET_SIZE:
                client.connect()
                client.Put(dht_key, thrift_serialize(bucketRight))
                client.disconnect()
                return self.bucketSplitter(bucketLeft, name)
            if len(bucketRight.values) > DEFAULT_BUCKET_SIZE:
                buck = thrift_serialize(self.bucketSplitter(bucketRight,name))
                client.connect()
                client.Put(dht_key, buck)
                return bucketLeft

            client.connect()
            client.Put(dht_key, thrift_serialize(bucketRight))

            return bucketLeft
        else:
            if len(bucketRight.values) > DEFAULT_BUCKET_SIZE:
                client.connect()
                client.Put(dht_key, thrift_serialize(bucketLeft))
                client.disconnect()
                return self.bucketSplitter(bucketRight,name)
            if len(bucketLeft.values) > DEFAULT_BUCKET_SIZE:
                buck = thrift_serialize(self.bucketSplitter(bucketLeft,name))
                client.connect()
                client.Put(dht_key, buck)
                return bucketRight
            client.connect()
            client.Put(dht_key, thrift_serialize(bucketLeft))
            return bucketRight


    def GetBucketKeys(self, key):
        logger.debug(f"RPC: GetBucketKeys({key.hex()})")
        bucket = self._get_existing_bucket(key)
        return BucketKeys(bucket.search_key_min, bucket.search_key_max)

    def Put(self, key, value):
        logger.debug(f"RPC: Put({key.hex()}, ...)")
        self._db.put(key, value)

    def PutLatest(self, key, search_key, value):
        key_hex = key.hex()
        logger.debug(f"RPC: PutLatest({key_hex}, {search_key}, ...)")
        with self._put_latest_lock:
            current_search_key = self._db.get(f'latest:{key_hex}')
            if current_search_key is not None:
                if int(current_search_key) > search_key:
                    return
            self._db.put(key, value)
            self._db.put(f'latest:{key_hex}', search_key)

    def Add(self, key, value: BucketValue, name, search_key_min, search_key_max):
        logger.debug(f"RPC: Add({key.hex()}, {search_key_min}, {search_key_max}, ...)")
        with self._lock_bucket(key):
            bucket = self._db.get(key)
            if bucket is not None:
                bucket = thrift_unserialize(bucket, Bucket())
                if (bucket.search_key_min + 1) >= bucket.search_key_max:
                    logger.warning(f'Storing search key {value.search_key} in a full atomic bucket')
                    return
                if value.search_key < bucket.search_key_min or value.search_key >= bucket.search_key_max:
                    raise StorageException(500, 'Trying to add to a wrong bucket')

                self.insort_right(bucket.values, value)
                if len(bucket.values) > DEFAULT_BUCKET_SIZE:
                    bucket = self.bucketSplitter(bucket, name)                    
            else:
                bucket = Bucket(search_key_min=0,
                                search_key_max=DEFAULT_SEARCH_KEY_MAX,
                                values=[value])
                logger.debug(f'Storing {value.search_key}/{name} in {bucket.search_key_min}-{bucket.search_key_max}')
            self._db.put(key, thrift_serialize(bucket))

    def Get(self, key):
        logger.debug(f"RPC: Get({key.hex()})")
        value = self._db.get(key)
        if value is None:
            # get() returns None when key is not in the database
            raise StorageException(404, 'Key not found')
        return value

    def LatestMaxRecursiveBackwards(self, bucket, name):
        label = get_label(bucket.search_key_min, bucket.search_key_max)
        bLabel = get_left_neighbour(label)
        if label == bLabel: # Stopping condition
                return
        else:
            label = bLabel

        dht_key = digest(f'{name}:{bLabel}')
        peers = self.FindClosestPeers(dht_key)
        nextBucket = None
        for peer in peers:
            client = Client(peer.host, peer.port)
            client.connect()
            try:
                nextBucket = thrift_unserialize(client.Get(dht_key), Bucket())
                break
            except:
                pass
        if nextBucket is None:
            dht_key = digest(f'{name}:{naming_func(bLabel)}')
            peers = self.FindClosestPeers(dht_key)
            nextBucket = None
            for peer in peers:
                client = Client(peer.host, peer.port)
                client.connect()
                try:
                    nextBucket = thrift_unserialize(client.Get(dht_key), Bucket())
                    break
                except:
                    logger.error(f'Error: GetLatestMax: {peer.port} {name}:{naming_func(bLabel)}')
                    pass

        if nextBucket is not None:
            if len(nextBucket.values) != 0:
                return nextBucket.values[-1]
            else:
                return self.LatestMaxRecursiveBackwards(nextBucket, name)
        else:
            logger.error('Error: GetLatestMax: Missing bucket')
            return

    def GetLatestMax(self, name, search_key_max):
        logger.info(f"RPC: GetLatestMax({name}, {search_key_max})")
        label = self._lookup(name, search_key_max)
        logger.debug(f'GetLatestMax: key {name} -> {label}')
        dht_key = digest(label)
        closest = self._find_closest_peers(dht_key)
        bucket = None
        for peer in closest:
            client = Client(peer.host, peer.port)
            client.connect()
            try:
                bucket = client.Get(dht_key)
                if bucket is not None:
                    bucket = thrift_unserialize(bucket, Bucket())      
            except StorageException:
                pass
        if bucket is None:
            logger.info(f'GetLatestMax: Bucket {name}/{search_key_max} is empty')
            return
        for value in reversed(bucket.values):
            if value.search_key <= search_key_max:
                return value

        return self.LatestMaxRecursiveBackwards(bucket, name)

    def RangeRecursiveForward(self, bucket, name, search_key_min, search_key_max):
        label = get_label(bucket.search_key_min, bucket.search_key_max)
        result = []
        for item in bucket.values:
            if item.search_key >= search_key_min and item.search_key <= search_key_max:
                result = result + [item]

        if label[-1] == '1' and label[-2] == '1':
            leftwards = True
        else:
            leftwards = False

        while True:
            if leftwards:
                bLabel = get_left_neighbour(label)
            else:
                bLabel = get_right_neighbour(label)
            
            if label == bLabel: # Stopping condition
                return result
            else:
                label = bLabel

            inteval = get_label_range(bLabel)
            if inteval[0] > search_key_max or inteval[1] <= search_key_min:  # intersection is NULL, stop recursion & iteration
                return result
            elif inteval[0] >= search_key_min and  inteval[1] - 1 <=  search_key_max:   # Range totaly covers the interval, recurse down, then iterate left/right
                dht_key = digest(f'{name}:{naming_func(bLabel)}')
                peers = self.FindClosestPeers(dht_key)
                nextBucket = None
                for peer in peers:
                    client = Client(peer.host, peer.port)
                    client.connect()
                    try:
                        nextBucket = thrift_unserialize(client.Get(dht_key), Bucket())
                        break
                    except:
                        pass
                if nextBucket is None:
                    logger.error(f'Error: Recursive forward: {peer.port} {name}:{naming_func(bLabel)}')
                    return result
                else:
                    result = result + self.RangeRecursiveForward(nextBucket,name,inteval[0],inteval[1]-1)
            else:   #range partially covers , recurse down, stop iterating
                dht_key = digest(f'{name}:{bLabel}')
                peers = self.FindClosestPeers(dht_key)
                nextBucket = None
                for peer in peers:
                    client = Client(peer.host, peer.port)
                    client.connect()
                    try:
                        nextBucket = thrift_unserialize(client.Get(dht_key), Bucket())
                        break
                    except:
                        pass
                if nextBucket is None:
                    dht_key = digest(f'{name}:{naming_func(bLabel)}')
                    peers = self.FindClosestPeers(dht_key)
                    nextBucket = None
                    for peer in peers:
                        client = Client(peer.host, peer.port)
                        client.connect()
                        try:
                            nextBucket = thrift_unserialize(client.Get(dht_key), Bucket())
                            break
                        except:
                            logger.error(f'Error: Recursive forward/2: {peer.port} {name}:{naming_func(bLabel)}')
                            pass
                result = result + self.RangeRecursiveForward(nextBucket, name, max(inteval[0],search_key_min), min(inteval[1] - 1, search_key_max))
                return result

    def GetRange(self, name, search_key_min, search_key_max): 
        label = self._lookup(name, search_key_min)
        dht_key = digest(label)
        closest = self._find_closest_peers(dht_key)
        bucket = None
        for peer in closest:
            client = Client(peer.host, peer.port)
            client.connect()
            try:
                bucket = client.Get(dht_key)
                if bucket is not None:
                    bucket = thrift_unserialize(bucket, Bucket())      
            except StorageException:
                pass
        if bucket is None:
            logger.debug(f'GetRange: Bucket {name}/{search_key_min}/{search_key_max} is empty')
            return []
        return self.RangeRecursiveForward(bucket, name, search_key_min, search_key_max)
 
    def _get_nonempty_bucket(self, key) -> Bucket:
        bucket = self._db.get(key)
        if bucket is not None:
            bucket = thrift_unserialize(bucket, Bucket())
            if not isinstance(bucket.values, list):
                raise StorageException(405, 'Key contains non-bucket value')
        if bucket is None or len(bucket.values) == 0:
            raise StorageException(404, 'Bucket is empty')
        return bucket

    def _get_existing_bucket(self, key) -> Bucket:
        bucket = self._db.get(key)
        if bucket is not None:
            bucket = thrift_unserialize(bucket, Bucket())
            if not isinstance(bucket.values, list):
                raise StorageException(405, 'Key contains non-bucket value')
        if bucket is None:
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

    # Bisection functions adapted from the bisect module

    @staticmethod
    def insort_right(a, x: BucketValue, lo=0, hi=None):
        if lo < 0:
            raise ValueError('lo must be non-negative')
        if hi is None:
            hi = len(a)
        while lo < hi:
            mid = (lo + hi) // 2
            if x.search_key < a[mid].search_key:
                hi = mid
            else:
                lo = mid + 1
        a.insert(lo, x)

    @staticmethod
    def bisect_left(a, x: int, lo=0, hi=None):
        if lo < 0:
            raise ValueError('lo must be non-negative')
        if hi is None:
            hi = len(a)
        while lo < hi:
            mid = (lo + hi) // 2
            if a[mid].search_key < x:
                lo = mid + 1
            else:
                hi = mid
        return lo

    @staticmethod
    def bisect_right(a, x: int, lo=0, hi=None):
        if lo < 0:
            raise ValueError('lo must be non-negative')
        if hi is None:
            hi = len(a)
        while lo < hi:
            mid = (lo + hi) // 2
            if x < a[mid].search_key:
                hi = mid
            else:
                lo = mid + 1
        return lo


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
            tfactory = TTransport.TFramedTransportFactory()
            pfactory = TCompactProtocol.TCompactProtocolAcceleratedFactory()

            server = TServer.TThreadPoolServer(
                processor, transport, tfactory, pfactory, daemon=True)
            server.setNumThreads(100)
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
