#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
from ede.credentials import DB_HOST, DB_NAME, DB_PASS, \
    DB_PORT, DB_USER, SECRET_KEY


_basedir = os.path.abspath(os.path.dirname(__file__))

DEBUG = True
ASSETS_DEBUG = True

CACHE_TYPE = 'redis'
CACHE_KEY_PREFIX = 'ede'

ADMINS = frozenset(['matteson@obstructures.org'])
SECRET_KEY = SECRET_KEY

SQLALCHEMY_DATABASE_URI = 'postgresql://{}:{}@{}:{}/{}'.format(
    DB_USER, DB_PASS, DB_HOST, DB_PORT, DB_NAME)
SQLALCHEMY_MIGRATE_REPO = os.path.join(_basedir, 'db_repository')
DATABASE_CONNECT_OPTIONS = {}

THREADS_PER_PAGE = 8

CACHE_CONFIG = {
    'CACHE_TYPE': 'redis',
}