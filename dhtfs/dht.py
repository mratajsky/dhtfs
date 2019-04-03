import asyncio
import logging
import threading

from kademlia.network import Server
from kademlia.node import Node
from kademlia.crawling import NodeSpiderCrawl

logger = logging.getLogger(__name__)


class DHT:
    def __init__(self, pipe, options):
        self._rpc_pipe = pipe
        self._host = options.host
        self._port = options.port
        self._bootstrap_peers = options.bootstrap
        self._node = Server(
            ksize=options.ksize,
            alpha=options.alpha, node_id=options.node_id)
        self._running = False

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port

    @property
    def bootstrap_peers(self):
        return self._bootstrap_peers

    @property
    def running(self):
        return self._running

    async def run(self):
        '''Start the DHT service.'''
        if self._running:
            return
        self._running = True
        await self._listen(self._host, self._port)
        if self._bootstrap_peers is not None:
            await self._bootstrap(self._bootstrap_peers)

        def worker(loop):
            while True:
                try:
                    # Blocks until there is a message to read from the pipe
                    op, ident, params = self._rpc_pipe.recv()
                    asyncio.run_coroutine_threadsafe(
                        self._process_rpc(op, ident, params), loop)
                except EOFError:
                    break
        # Multiprocessing pipe doesn't integrate with asyncio, use a separate
        # thread to read RPC requests and schedule a coroutine in the asyncio loop
        threading.Thread(target=worker,
                         args=(asyncio.get_event_loop(),)).start()

    def stop(self):
        '''Stop the DHT service.'''
        if not self._running:
            return
        self._running = False
        self._node.stop()

    async def _listen(self, host, port):
        await self._node.listen(port, interface=host)

    async def _bootstrap(self, peers):
        '''Bootstrap the node by connecting to other known nodes.'''
        if not isinstance(peers, list):
            peers = [peers]

        def parse_peer(peer):
            parts = peer.split(':', 2)
            if len(parts) != 2:
                raise ValueError('Peer must be in the host:port format')
            return (parts[0], int(parts[1]))

        await self._node.bootstrap(list(map(parse_peer, peers)))

    async def _process_rpc(self, op, ident, params):
        if op == 'FindClosestPeers':
            # params is the DHT key
            nearest = self._node.protocol.router.find_neighbors(
                Node(params), self._node.alpha)
            crawler = NodeSpiderCrawl(
                self._node.protocol,
                self._node.node,
                nearest,
                self._node.ksize,
                self._node.alpha)
            nodes = await crawler.find()
            self._rpc_pipe.send((ident, nodes))
