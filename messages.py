
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
    TYPE_MAP = {
        MsgTypes.Subscribe: _unpack_keys,
        MsgTypes.Snapshot: _unpack_kvs,
        MsgTypes.SnapshotRequest: _unpack_keys,
        MsgTypes.Unsubscribe: _unpack_keys,
        MsgTypes.Publish: _unpack_kvs,
    }

    msg_type = chr(msg[0])
    return (msg_type, TYPE_MAP[msg_type](msg))


def _unpack_kvs(msg):
    return _unpack(msg, True)


def _unpack_keys(msg):
    return _unpack(msg, False)


def _unpack(msg, has_values):
    pointer = 1
    keys_count = struct.unpack('!I', msg[pointer:pointer+INT_SIZE])[0]
    pointer += INT_SIZE
    result = {} if has_values else []
    for _ in range(keys_count):
        key_size = struct.unpack('!I', msg[pointer:pointer+INT_SIZE])[0]
        pointer += INT_SIZE
        key = struct.unpack(
            '!{}s'.format(key_size), msg[pointer:pointer+key_size]
        )[0]
        key = json.loads(key)
        pointer += key_size
        if not has_values:
            result.append(key)
            continue
        value_size = struct.unpack('!I', msg[pointer:pointer+INT_SIZE])[0]
        pointer += INT_SIZE
        value = struct.unpack(
            '!{}s'.format(value_size), msg[pointer:pointer+value_size]
        )[0]
        value = json.loads(value)
        pointer += value_size
        result[key] = value
    return result


# ----------------------------------------------------------------------------#


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
    items_count = len(data)
    result = bytes(msg_type, 'ascii')
    result += struct.pack('!I', items_count)
    if isinstance(data, dict):
        result += _pack_kvs(data)
    else:
        result += _pack_keys(data)
    return struct.pack('!I', len(result)) + result


def _pack_kvs(kvs):
    result = b''
    for k, v in kvs.items():
        k, v = bytes(json.dumps(k), 'utf-8'), bytes(json.dumps(v), 'utf-8')
        k_size, v_size = len(k), len(v)
        result += struct.pack(
            '!I{}sI{}s'.format(k_size, v_size),
            k_size, k, v_size, v
        )
    return result


def _pack_keys(keys):
    result = b''
    for k in keys:
        k = bytes(json.dumps(k), 'utf-8')
        result += struct.pack('!I{}s'.format(len(k)), len(k), k)
    return result
