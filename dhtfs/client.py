import datetime
import logging
import random

from kademlia.utils import digest

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from .peer.utils import thrift_serialize, thrift_unserialize
from .thrift.metadata.ttypes import FileSystem, FileSystemModel, Inode, InodeType, DirData
from .thrift.rpc import Rpc
from .thrift.rpc.ttypes import Bucket, BucketValue, Peer, StorageException

logger = logging.getLogger(__name__)


class Client:
    def __init__(self, host, port):
        self._host = host
        self._port = port
        socket = TSocket.TSocket(host, port)
        # Buffering is critical. Raw sockets are very slow
        self._transport = TTransport.TBufferedTransport(socket)
        # Wrap in a protocol
        protocol = TBinaryProtocol.TBinaryProtocolAccelerated(self._transport)
        # Create a client to use the protocol encoder
        self._client = Rpc.Client(protocol)
        self._connected = False

    @property
    def connected(self):
        return self._connected

    def connect(self):
        self._transport.open()
        self._connected = True

    # Iterative methods
    def add_iterative(self, key, bucket_value, search_key_min, search_key_max):
        assert self._connected
        kd = digest(key)
        logging.info(f'Storing {key} -> {kd.hex()}')
        return self.add_iterative_digest(kd, bucket_value, search_key_min, search_key_max)

    def add_iterative_digest(self, kd, bucket_value, search_key_min, search_key_max):
        assert self._connected
        peers = self._client.FindClosestPeers(kd)
        for peer in peers:
            client = Client(peer.host, peer.port)
            client.connect()
            # Store the value at every peer
            client.Add(kd, bucket_value, search_key_min, search_key_max)

    def get_iterative(self, key, method='Get', *args):
        assert self._connected
        kd = digest(key)
        logging.debug(f'Getting {key} -> {kd.hex()}')
        return self.get_iterative_digest(kd, method, *args)

    def get_iterative_digest(self, kd, method='Get', *args):
        peers = self._client.FindClosestPeers(kd)
        for peer in peers:
            client = Client(peer.host, peer.port)
            client.connect()
            try:
                # Return the value as soon as we find it
                return getattr(client, method)(kd, *args)
            except StorageException as e:
                logging.info(f'Get(): {e.error_code} -> {e.error_message}')
                if e.error_code != 404:
                    raise e

    def put_iterative(self, key, bin_value):
        assert self._connected
        kd = digest(key)
        logging.debug(f'Storing {key} -> {kd.hex()}')
        return self.put_iterative_digest(kd, bin_value)

    def put_iterative_digest(self, kd, bin_value):
        assert self._connected
        peers = self._client.FindClosestPeers(kd)
        for peer in peers:
            client = Client(peer.host, peer.port)
            client.connect()
            # Store the value at every peer
            client.Put(kd, bin_value)

    # Get file system description by name
    def get_filesystem(self, name):
        value = self.get_iterative(f'F:{name}')
        if value is not None:
            return thrift_unserialize(value, FileSystem())

    # Store file system description
    def put_filesystem(self, fs_name, model, block_size, inumber):
        assert self._connected
        # Store the root inode first
        value = Inode(
            id=random.getrandbits(63),
            type=InodeType.DIRECTORY,
            inumber=inumber,
            mtime=0,
            directory_data=DirData(entries={}))
        if model == FileSystemModel.PASTIS:
            self.put_inode(fs_name, inumber, value)
        else:
            # TODO: min-max should be 0-chosen period
            self.add_inode(fs_name, inumber, value, 0, 0, 0)

        # Store the description
        value = FileSystem(
            name=fs_name,
            model=model,
            block_size=block_size,
            inception=int(datetime.datetime.now().timestamp()),
            root=inumber)
        self.put_iterative(f'F:{fs_name}', thrift_serialize(value))

    # Get inode by file system name and inode number
    def get_inode(self, fs_name, inumber):
        value, bucket = self._get_inode_value_or_bucket(fs_name, inumber)
        if value is not None:
            return value
        if bucket is not None:
            for bucket_value in bucket.values:
                # Unserialize the BucketValues as well
                bucket_value.value = thrift_unserialize(bucket_value.value, Inode())
            return bucket

    def get_inode_latest(self, fs_name, inumber, max_key=None):
        if max_key is None:
            bucket_value = self.get_iterative(
                f'I:{fs_name}:{inumber}', 'GetLatest')
        else:
            bucket_value = self.get_iterative(
                f'I:{fs_name}:{inumber}', 'GetLatestMax', max_key)
        if bucket_value is not None:
            bucket_value.value = thrift_unserialize(bucket_value.value, Inode())
        return bucket_value

    def _get_inode_value_or_bucket(self, fs_name, inumber):
        fs_desc = self.get_filesystem(fs_name)
        if fs_desc is None:
            return None
        value = self.get_iterative(f'I:{fs_name}:{inumber}')
        if value is not None:
            if fs_desc.model == FileSystemModel.PASTIS:
                return thrift_unserialize(value, Inode()), None
            # Inodes are stored in a bucket
            return None, thrift_unserialize(value, Bucket())
        return None, None

    # Store inode
    def add_inode(self, fs_name, inumber, inode, search_key,
                  search_key_min,
                  search_key_max):
        assert self._connected
        value = thrift_serialize(inode)
        self.add_iterative(f'I:{fs_name}:{inumber}',
                           BucketValue(value=value, search_key=search_key),
                           search_key_min,
                           search_key_max)

    def put_inode(self, fs_name, inumber, inode):
        assert self._connected
        self.put_iterative(f'I:{fs_name}:{inumber}', thrift_serialize(inode))

    # Wrapper methods
    def Add(self, kd, bucket_value, search_key_min, search_key_max):
        assert self._connected
        logging.debug(f'Add: {self._host}:{self._port}')
        self._client.Add(kd, bucket_value, search_key_min, search_key_max)

    def Get(self, kd):
        assert self._connected
        logging.debug(f'Get: {self._host}:{self._port}')
        return self._client.Get(kd)

    def GetLatest(self, kd):
        assert self._connected
        logging.debug(f'GetLatest: {self._host}:{self._port}')
        return self._client.GetLatest(kd)

    def GetLatestMax(self, kd, max_key):
        assert self._connected
        logging.debug(f'GetLatestMax: {self._host}:{self._port}')
        return self._client.GetLatestMax(kd, max_key)

    def Put(self, kd, bin_value):
        assert self._connected
        logging.debug(f'Put: {self._host}:{self._port}')
        self._client.Put(kd, bin_value)
