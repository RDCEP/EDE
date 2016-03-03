#!/usr/bin/env python
# -*- coding: utf-8 -*-
from flask import request


def make_cache_key(*args, **kwargs):
    path = request.path
    args = str(hash(frozenset(request.args.items())))
    # print 'cache_key:', (path+args)
    return (path + args).encode('utf-8')