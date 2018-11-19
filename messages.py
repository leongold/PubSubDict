
import struct
import json


INT_SIZE = 4


class MsgTypes(object):

    Subscribe = 's'
    Snapshot = 'S'
    SnapshotRequest = 'z'
    Unsubscribe = 'u'
    Publish = 'p'


# ----------------------------------------------------------------------------#


def unpack(msg):
    msg_type = chr(msg[0])
    return (msg_type, json.loads(msg[1:]))

def pack_publish(kvs):
    return pack(MsgTypes.Publish, kvs)


def pack_subscribe(keys):
    return pack(MsgTypes.Subscribe, keys)


def pack_snapshot(kvs):
    return pack(MsgTypes.Snapshot, kvs)


def pack_snapshot_request(keys):
    return pack(MsgTypes.SnapshotRequest, keys)


def pack_unsubscribe(keys):
    return pack(MsgTypes.Unsubscribe, keys)


def pack(msg_type, data):
    s = json.dumps(data)
    payload = bytes(msg_type, 'ascii') + bytes(s, 'utf-8')
    return (len(payload)).to_bytes(INT_SIZE, 'big') + payload
