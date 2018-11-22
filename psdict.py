
import time
import threading

from twisted.internet import reactor
if reactor.running:
    from crochet import no_setup
    no_setup()
else:
    from crochet import setup
    setup()
from crochet import run_in_reactor
from twisted.internet.endpoints import TCP4ClientEndpoint
from twisted.internet.endpoints import connectProtocol

from client import PubSubDictClientProtocol


class NotConnectedError(Exception):
    pass


def _raise_if_disconnected(func):
    def wrapped(self, *args, **kwargs):
        if not self.connected:
            raise NotConnectedError()
        return func(self, *args, **kwargs)
    return wrapped


class PubSubDict(dict):

    def __init__(self, server_addr='localhost', server_port=8123, *args, **kwargs):
        super(PubSubDict, self).__init__(*args, **kwargs)
        self._client = PubSubDictClientProtocol()
        self._server_addr = server_addr
        self._server_port = server_port
        self._snapshot_cb = None
        self._callbacks = {}

    def __setitem__(self, key, value):
        super(PubSubDict, self).__setitem__(key, value)
        self.publish(**{key: value})

    def __repr__(self):
        return 'PubSubDict @ {}: {}'.format(
            hex(id(self)),
            super(PubSubDict, self).__repr__()
        )

    @property
    def connected(self):
        return self._client.connected

    def connect(self, block=True):
        point = TCP4ClientEndpoint(
            reactor, self._server_addr, self._server_port
        )
        connectProtocol(point, self._client)
        if not block:
            return
        while not self.connected:
            time.sleep(0.5)

    @run_in_reactor
    @_raise_if_disconnected
    def disconnect(self, block=False):
        self._client.transport.loseConnection()
        if not block:
            return
        while self.connected:
            time.sleep(0.5)

    @run_in_reactor
    @_raise_if_disconnected
    def snapshot(self, *keys, callback=None):
        if callback:
            self._snapshot_cb = callback
        self._client.snapshot_request(keys, self._on_snapshot)

    @run_in_reactor
    @_raise_if_disconnected
    def subscribe(self, *keys, callback=None):
        if callback:
            self._callbacks.update({key: callback for key in keys})
        self._client.subscribe({key: self._on_publish for key in keys})

    @run_in_reactor
    @_raise_if_disconnected
    def unsubscribe(self, *keys):
        for key in keys:
            self._callbacks.pop(key, None)
        self._client.unsubscribe(*keys)

    @run_in_reactor
    @_raise_if_disconnected
    def publish(self, **kvs):
        self._client.publish(kvs)

    def update(self, **kwargs):
        super(PubSubDict, self).update(**kwargs)
        self.publish(**kwargs)

    def _on_snapshot(self, kvs):
        kvs = {k: v for k, v in kvs.items() if v is not None}
        super(PubSubDict, self).update(**kvs)
        if self._snapshot_cb:
            self._snapshot_cb(kvs)
            self._snapshot_cb = None

    def _on_publish(self, kvs):
        self.update(**kvs)
        for key, value in kvs.items():
            if key in self._callbacks:
                self._callbacks[key](value)
