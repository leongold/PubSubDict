
from twisted.internet.protocol import ClientFactory
from twisted.internet.protocol import Protocol

import messages
import protocol


class ClientProtocol(protocol.PubSubDictProtocol):

    def __init__(self):
        super(ClientProtocol, self).__init__()
        self._publish_callbacks = {}
        self._snapshot_callback = None
        self._connected = False

    def _on_message(self, msg):
        TYPE_MAP = {
            messages.MsgTypes.Snapshot: self._on_snapshot,
            messages.MsgTypes.Publish: self._on_publish,
        }
        msg_type, data = messages.unpack(msg)
        TYPE_MAP[msg_type](data)

    def _on_snapshot(self, kvs):
        if self._snapshot_callback:
            self._snapshot_callback(kvs)

    def _on_publish(self, kvs):
        for key in kvs.keys():
            callback = self._publish_callbacks.get(key)
            callback(kvs)

    def subscribe(self, keys_to_callbacks):
        for key, callback in keys_to_callbacks.items():
            self._publish_callbacks[key] = callback

        packed = messages.pack_subscribe(list(keys_to_callbacks.keys()))
        self.transport.write(packed)

    def unsubscribe(self, *keys):
        for key in keys:
            self._publish_callbacks.pop(key, None)

        packed = messages.pack_unsubscribe(keys)
        self.transport.write(packed)

    def publish(self, kvs):
        packed = messages.pack_publish(kvs)
        self.transport.write(packed)

    def snapshot_request(self, keys, callback=None):
        self._snapshot_callback = callback
        packed = messages.pack_snapshot_request(keys)
        self.transport.write(packed)


class ClientFactory(ClientFactory):

    def buildProtocol(self, addr):
        return ClientProtocol()
