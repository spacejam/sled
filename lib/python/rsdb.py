#!/usr/bin/env python

from ctypes import *
import os

sled = CDLL("./libsled.so")

sled.sled_create_config.argtypes = ()
sled.sled_create_config.restype = c_void_p

sled.sled_config_set_path.argtypes = (c_void_p, c_char_p)

sled.sled_free_config.argtypes = (c_void_p,)

sled.sled_open_tree.argtypes = (c_void_p,)
sled.sled_open_tree.restype = c_void_p

sled.sled_free_tree.argtypes = (c_void_p,)

sled.sled_get.argtypes = (c_void_p, c_char_p, c_size_t, POINTER(c_size_t))
sled.sled_get.restype = c_char_p

sled.sled_scan.argtypes = (c_void_p, c_char_p, c_size_t, POINTER(c_size_t))
sled.sled_scan.restype = c_void_p

sled.sled_set.argtypes = (c_void_p, c_char_p, c_size_t, c_char_p, c_size_t)
sled.sled_set.restype = None

sled.sled_del.argtypes = (c_void_p, c_char_p, c_size_t)
sled.sled_del.restype = None

sled.sled_cas.argtypes = (c_void_p,
                          c_char_p, c_size_t,  # key
                          c_char_p, c_size_t,  # old
                          c_char_p, c_size_t,  # new
                          POINTER(c_char_p), POINTER(c_size_t),  # actual ret
                          )
sled.sled_cas.restype = c_ubyte


class Conf:
    def __init__(self):
        self.ptr = c_void_p(sled.sled_create_config())

    def tree(self):
        tree_ptr = sled.sled_open_tree(self.ptr)
        return Tree(c_void_p(tree_ptr))

    def path(self, path):
        sled.sled_config_set_path(self.ptr, path)

    def __del__(self):
        sled.sled_free_config(self.ptr)


class TreeIterator:
    def __init__(self, ptr):
        self.ptr = ptr

    def __del__(self):
        sled.sled_free_iter(self.ptr)


class Tree:
    def __init__(self, ptr):
        self.ptr = ptr

    def __del__(self):
        if self.ptr:
            sled.sled_free_tree(self.ptr)

    def close(self):
        self.__del__()
        self.ptr = None

    def set(self, key, val):
        sled.sled_set(self.ptr, key, len(key), val, len(val))

    def get(self, key):
        vallen = c_size_t(0)
        ptr = sled.sled_get(self.ptr, key, len(key), byref(vallen))
        return ptr[:vallen.value]

    def delete(self, key):
        sled.sled_del(self.ptr, key, len(key))

    def cas(self, key, old, new):
        actual_vallen = c_size_t(0)
        actual_val = c_char_p(0)

        if old is None:
            old = b""

        if new is None:
            new = b""

        success = sled.sled_cas(self.ptr, key,
                                len(key),
                                old, len(old),
                                new, len(new),
                                byref(actual_val), byref(actual_vallen))

        if actual_vallen.value == 0:
            return (None, success == 1)
        else:
            return (actual_val.value[:actual_vallen.value], success == 1)

    def scan(self, key):
        return sled.sled_scan(self.ptr, key, len(key))
