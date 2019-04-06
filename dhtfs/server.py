import asyncio
import logging
import signal
from collections import namedtuple
from contextlib import suppress
from multiprocessing import Pipe

from .dht import DHT
from .rpc import RPC

logger = logging.getLogger(__name__)

OptionsDHT = namedtuple('OptionsDHT', [
    'host', 'port', 'alpha', 'ksize', 'node_id', 'bootstrap'
])
OptionsRPC = namedtuple('OptionsRPC', [
    'host', 'port', 'database', 'database_clean'
])


class Server:
    def __init__(self, options_dht, options_rpc):
        self._conn1, self._conn2 = Pipe()
        self._dht = DHT(self._conn1, options_dht)
        self._rpc = RPC(self._conn2, options_rpc)
        self._running = False

    @property
    def dht(self):
        return self._dht

    @property
    def rpc(self):
        return self._rpc

    @property
    def running(self):
        return self._running

    def start(self):
        '''Start the server.'''
        if self._running:
            return
        self._running = True
        self._rpc.run()
        loop = asyncio.get_event_loop()
        # Call our stop() function when killed by a signal
        for signame in ('SIGINT', 'SIGTERM'):
            loop.add_signal_handler(getattr(signal, signame), self.stop)
        try:
            loop.create_task(self._dht.run())
            loop.run_forever()
            # Cancel asyncio tasks that are still running
            pending = asyncio.Task.all_tasks()
            for task in pending:
                task.cancel()
                with suppress(asyncio.CancelledError):
                    loop.run_until_complete(task)
        finally:
            loop.close()

    def stop(self):
        '''Stop the server.'''
        if not self._running:
            return
        self._running = False
        # Close the pipe so that threads which block on reading from them
        # have a chance to quit
        self._conn1.close()
        self._conn2.close()
        self._dht.stop()
        self._rpc.stop()
        asyncio.get_event_loop().stop()
