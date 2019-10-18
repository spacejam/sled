#!/usr/bin/env python3
from sled import Conf

c = Conf()
c.path(b"ALICE.data")
t = c.tree()
t.set(b"k1", b"v1")
t.close()
