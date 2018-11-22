
import os
import time
import unittest
import subprocess
from contextlib import contextmanager

from server import PubSubDictServerFactory
from psdict import PubSubDict


class DistDictTestCase(unittest.TestCase):

    def setUp(self):
        self.port = subprocess.Popen(
            [os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                'server.py'
             )
            ], stdout=subprocess.PIPE
        )
        time.sleep(1)
        self.ps_dict_1 = PubSubDict('127.0.0.1')
        self.ps_dict_1.connect(block=True)
        self.ps_dict_2 = PubSubDict('127.0.0.1')
        self.ps_dict_2.connect(block=True)

    def tearDown(self):
        self.ps_dict_1.disconnect()
        self.ps_dict_2.disconnect()
        self.port.terminate()
        self.port.wait()
        self.port.kill()

    def test_nested_value(self):
        self.ps_dict_1.subscribe('key')
        self.ps_dict_2['key'] = {'nested_key': 'nested_value'}
        self._assert_key_value(
            self.ps_dict_1, 'key', {'nested_key': 'nested_value'}
        )

    def test_subscribe_publish_correct_kv_succeeds(self):
        self.ps_dict_1.subscribe('key')
        self.ps_dict_2['key'] = 'value'
        self._assert_key_value(self.ps_dict_1, 'key', 'value')

    def test_subscribe_publish_incorrect_value_fails(self):
        self.ps_dict_1.subscribe('key')
        self.ps_dict_2['key'] = 'value'
        self._assert_key_value(
            self.ps_dict_1, 'key', 'another_value', equal=False
        )

    def test_snapshot_while_subscribed(self):
        self.ps_dict_1.subscribe('key')
        self.ps_dict_2['key'] = 'value'
        self._assert_key_value(self.ps_dict_1, 'key', 'value')
        self.ps_dict_1.pop('key')
        self.ps_dict_1.snapshot('key')
        self._assert_key_value(self.ps_dict_1, 'key', 'value')

    def test_snapshot_no_subscribers(self):
        self.ps_dict_1.subscribe('key')
        self.ps_dict_2['key'] = 'value'
        self._assert_key_value(self.ps_dict_1, 'key', 'value')
        self.ps_dict_1.pop('key')
        self.ps_dict_1.unsubscribe('key')
        self.ps_dict_1.snapshot('key')
        self.assertFalse(self._is_key_in_dict(self.ps_dict_1, 'key'))

    def test_single_producer_multiple_consumer(self):
        with self._psdict_context(num=10) as dicts:
            for d in dicts:
                d.subscribe('key')
            self.ps_dict_1['key'] = 'value'
            for d in dicts:
                self._assert_key_value(d, 'key', 'value')

    def _is_key_in_dict(self, psdict, key):
        retries = 0
        while key not in psdict:
            time.sleep(0.2)
            retries += 1
            if retries == 10:
                return False
        return True

    def _assert_key_value(self, psdict, key, value, equal=True):
        if (not self._is_key_in_dict(psdict, key)):
            self.fail('missing key')
        if equal:
            self.assertEqual(psdict[key], value)
        else:
            self.assertNotEqual(psdict[key], value)

    @contextmanager
    def _psdict_context(self, num=1):
        dicts = tuple([PubSubDict('127.0.0.1') for _ in range(num)])
        for d in dicts:
            d.connect(block=True)
        try:
            yield dicts
        finally:
            for d in dicts:
                d.disconnect()
