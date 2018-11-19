```>>> from psdict import PubSubDict
>>> d1 = PubSubDict('127.0.0.1')
>>> d2 = PubSubDict('127.0.0.1')
>>> d1.connect()
>>> d2.connect()
>>> d1.subscribe('key')
>>> d2['key'] = 1
>>> d1
PubSubDict @ 0x7f1cfc61e360: {'key': 1}
>>> d2
PubSubDict @ 0x7f1cfc61e3b8: {'key': 1}
>>>
```
