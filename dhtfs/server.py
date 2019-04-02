import asyncio
import logging
import os
import random
import signal
from contextlib import suppress
from multiprocessing import Pipe

import appdirs

from .dht import DHT
from .rpc import RPC


class Server:
    def __init__(self, options={}):
        self._options = self._fill_options(options)
        self._conn1, self._conn2 = Pipe()
        self._dht = DHT(host=options.host, port=options.dht_port,
                        pipe=self._conn1,
                        bootstrap_peers=options.bootstrap)
        self._rpc = RPC(host=options.host, port=options.rpc_port,
                        pipe=self._conn2,
                        database=options.database)
        self._running = False

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

    def _fill_options(self, options):
        '''Fill unspecified options with default values.'''
        if options.database is None:
            # This becomes some like /home/user/.cache/dhtfs/peer-db
            options.database = os.path.join(
                appdirs.user_cache_dir(),
                'dhtfs', 'peer-db')
        if options.host is None:
            options.host = '0.0.0.0'
        if options.dht_port is None:
            options.dht_port = self._random_port()
        if options.rpc_port is None:
            # RPC port is by default DHT port+1 to make it easier to find
            options.rpc_port = options.dht_port + 1

        logging.debug(f'Database: {options.database}')
        logging.debug(f'Host: {options.host}')
        logging.debug(f'DHT port: {options.dht_port}')
        logging.debug(f'RPC port: {options.rpc_port}')
        return options

    @staticmethod
    def _random_port():
        '''Pick a random port in a reasonable range.'''
        return random.randint(10000, 65000)
