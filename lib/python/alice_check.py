#!/usr/bin/env python3
import os
import sys

from sled import Conf

crashed_state_directory = sys.argv[1]
os.chdir(crashed_state_directory)

dirlist = os.listdir('.')

assert("ALICE.data" in dirlist)

c = Conf()
c.path(b"ALICE.data")

t = c.tree()

assert(t.get(b"k1") == b"v1")
