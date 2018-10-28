
from twisted.internet.protocol import Protocol

import messages


class PubSubDictProtocol(Protocol):

    def __init__(self):
        self._buffer = b''
        self._msg_size = None

    def _on_message(self, msg):
        pass

    def dataReceived(self, data):
        if not self._buffer:
            msg_size = int.from_bytes(
                data[:messages.INT_SIZE],
                byteorder='big',
                signed=False
            )
            data = data[messages.INT_SIZE:]
            if len(data) == msg_size:
                self._on_message(data)
            elif len(data) > msg_size:
                excess = data[msg_size:]
                data = data[:msg_size]
                self._on_message(data)
                self.dataReceived(excess)
            else:
                self._buffer = data
                self._msg_size = msg_size
            return

        self._buffer += data
        if len(self._buffer) == self._msg_size:
            self._on_message(self._buffer)
            self._buffer = b''
            self._msg_size = None
        elif len(self._buffer) > self._msg_size:
            excess = self._buffer[self._msg_size:]
            data = self._buffer[:self._msg_size]
            self._on_message(data)
            self._buffer = b''
            self._msg_size = None
