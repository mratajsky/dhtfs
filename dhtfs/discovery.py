import asyncio
import logging
import socket
from zeroconf import ServiceInfo, Zeroconf

from .utils import get_ip_address

class DiscoveryServerService:
    _SERVICE_TYPE = '_http._tcp.local.'
    _SERVICE_NAME = 'sound-system-server._http._tcp.local.'

    def __init__(self, host, port):
        self._host = host
        self._port = port
        self._zeroconf = None
        self._info = None
        self._registered = False

    def start(self):
        if not self._registered:
            self._zeroconf = Zeroconf()
            self._start_service()

    def stop(self):
        if self._registered:
            self._zeroconf.unregister_all_services()
            self._zeroconf = None
            self._registered = False

    def _start_service(self):
        self._info = ServiceInfo(self._SERVICE_TYPE,
                                 self._SERVICE_NAME,
                                 socket.inet_aton(self._host),
                                 self._port,
                                 0, 0, {})
        logging.debug('Registered zeroconf service %s host %s:%d',
                      self._SERVICE_NAME,
                      self._host,
                      self._port)
        self._zeroconf.register_service(self._info)
        self._registered = True
