#!/usr/bin/python3
import json
import struct

from twisted.internet import reactor
from twisted.internet.protocol import Factory

import protocol
import messages


class PubSubDictServerProtocol(protocol.PubSubDictProtocol):

    def __init__(self, data, sub_map):
        super(PubSubDictServerProtocol, self).__init__()
        self._data = data
        self._sub_map = sub_map # key -> protocol

    def connectionLost(self, reason):
        super(PubSubDictServerProtocol, self).connectionLost(reason)
        for clients in self._sub_map.values():
            if self in clients:
                clients.remove(self)

    def _on_subscribe(self, keys):
        for key in keys:
            if key not in self._sub_map:
                self._sub_map[key] = set()
            self._sub_map[key].add(self)

    def _on_unsubscribe(self, keys):
        for key in keys:
            if key not in self._sub_map:
                continue
            try:
                self._sub_map[key].remove(self)
            except KeyError:
                continue
            if not self._sub_map[key]:
                self._data.pop(key, None)

    def _on_snapshot_request(self, keys):
        kvs = {key: self._data.get(key) for key in keys}
        packed = messages.pack(messages.MsgTypes.Snapshot, kvs)
        self.transport.write(packed)

    def _on_publish(self, kvs):
        for k, v in kvs.items():
            clients = self._sub_map.get(k)
            if not clients:
                continue

            if k not in self._data:
                self._data[k] = v
            # TODO: nested values
            packed = messages.pack(messages.MsgTypes.Publish, {k: v})

            for client in clients:
                if client is self:
                    continue
                client.transport.write(packed)

    def _on_message(self, msg):
        TYPE_MAP = {
            messages.MsgTypes.Subscribe: self._on_subscribe,
            messages.MsgTypes.SnapshotRequest: self._on_snapshot_request,
            messages.MsgTypes.Unsubscribe: self._on_unsubscribe,
            messages.MsgTypes.Publish: self._on_publish,
        }
        msg_type, data = messages.unpack(msg)
        TYPE_MAP[msg_type](data)


class PubSubDictServerFactory(Factory):

    def __init__(self):
        self._data = {}
        self._sub_map = {}

    def buildProtocol(self, addr):
        return PubSubDictServerProtocol(self._data, self._sub_map)


if __name__ == '__main__':
    reactor.listenTCP(8123, PubSubDictServerFactory())
    reactor.run()
