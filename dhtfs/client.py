import datetime
import logging

from kademlia.utils import digest

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from .peer.utils import thrift_serialize
from .thrift.metadata.ttypes import FileSystem, Inode, InodeType, DirData
from .thrift.rpc import Rpc
from .thrift.rpc.ttypes import Peer, StorageException

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

    def get(self, kd):
        assert self._connected
        logging.debug(f'Getting {kd.hex()} from {self._host}:{self._port}')
        return self._client.Get(kd)

    def put(self, kd, bin_value):
        assert self._connected
        logging.debug(f'Storing {kd.hex()} at {self._host}:{self._port}')
        self._client.Put(kd, bin_value)

    # High-level API
    def get_iterative(self, key):
        assert self._connected
        return self.get_iterative_digest(digest(key))

    def get_iterative_digest(self, kd):
        peers = self._client.FindClosestPeers(kd)
        for peer in peers:
            client = Client(peer.host, peer.port)
            client.connect()
            try:
                # Return the value as soon as we find it
                return client.get(kd)
            except StorageException as e:
                logging.info(f'Get(): {e.errorCode} -> {e.errorMessage}')
                if e.errorCode != 404:
                    raise e

    # Store value at the K closest nodes
    def put_iterative(self, key, value):
        assert self._connected
        kd = digest(key)
        bin_value = thrift_serialize(value)
        logging.debug(f'Storing {key} -> {kd.hex()} -> {value}')
        peers = self._client.FindClosestPeers(kd)
        for peer in peers:
            client = Client(peer.host, peer.port)
            client.connect()
            # Store the value at every peer
            client.put(kd, bin_value)

    # Store file system description
    def put_filesystem(self, fs_name, model, block_size, inumber):
        assert self._connected
        # Store the root inode first
        value = Inode(
            type=InodeType.DIRECTORY,
            inumber=inumber,
            mtime=int(datetime.datetime.now().timestamp()),
            directory_data=DirData(entries=[]))
        self.put_inode(fs_name, inumber, value)

        # Store the file system description
        value = FileSystem(
            name=fs_name,
            model=model,
            block_size=block_size,
            root=inumber)
        self.put_iterative(f'F:{fs_name}', value)

    # Store inode
    def put_inode(self, fs_name, inumber, inode):
        assert self._connected
        self.put_iterative(f'I:{fs_name}:{inumber}', inode)
