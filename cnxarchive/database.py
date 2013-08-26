# -*- coding: utf-8 -*-
# ###
# Copyright (c) 2013, Rice University
# This software is subject to the provisions of the GNU Affero General
# Public License version 3 (AGPLv3).
# See LICENCE.txt for details.
# ###
"""Database models and utilities"""
import os
import contextlib
import threading

import psycopg2
from psycopg2.pool import ThreadedConnectionPool

from .app import get_settings


CONNECTION_SETTINGS_KEY = 'db-connection-string'
CONNECTION_MIN_CONNECTIONS_DEFAULT = 1
CONNECTION_MAX_CONNECTIONS_DEFAULT = 1
CONNECTION_MIN_CONNECTIONS_SETTINGS_KEY = 'db-connection-min-connections'
CONNECTION_MAX_CONNECTIONS_SETTINGS_KEY = 'db-connection-max-connections'

here = os.path.abspath(os.path.dirname(__file__))
SQL_DIRECTORY = os.path.join(here, 'sql')
DB_SCHEMA_DIRECTORY = os.path.join(SQL_DIRECTORY, 'schema')
DB_SCHEMA_FILES = (
    os.path.join(DB_SCHEMA_DIRECTORY, 'main.sql'),
    os.path.join(DB_SCHEMA_DIRECTORY, 'trees.sql'),
    )


def _read_sql_file(name):
    path = os.path.join(SQL_DIRECTORY, '{}.sql'.format(name))
    with open(path, 'r') as fp:
        return fp.read()
SQL = {
    'get-module': _read_sql_file('get-module'),
    'get-module-metadata': _read_sql_file('get-module-metadata'),
    'get-resource': _read_sql_file('get-resource'),
    'get-resource-by-filename': _read_sql_file('get-resource-by-filename'),
    }


class BaseConnectionPool(ThreadedConnectionPool):
    """An application settings aware connection pool.
    This mostly means that the object doesn't initialize it's settings
    on creation. Instead it looks them up via the application.
    """

    def __init__(self):
        """Initialize the threading lock."""
        self.closed = False

        # These are settings looked up on use.
        ##self.minconn = minconn
        ##self.maxconn = maxconn
        ##self._args = args
        self._kwargs = {}

        self._pool = []
        self._used = {}
        self._rused = {} # id(conn) -> key map
        self._keys = 0

        self._lock = threading.Lock()

    @property
    def _settings(self):
        raise NotImplemented

    @property
    def minconn(self):
        return self._settings.get(CONNECTION_MIN_CONNECTIONS_SETTINGS_KEY,
                                  CONNECTION_MIN_CONNECTIONS_DEFAULT)

    @property
    def maxconn(self):
        return self._settings.get(CONNECTION_MAX_CONNECTIONS_SETTINGS_KEY,
                                  CONNECTION_MAX_CONNECTIONS_DEFAULT)

    @property
    def _args(self):
        """Used by the AbstractConnectionPool during connection creation."""
        return (self._settings[CONNECTION_SETTINGS_KEY],)


class DatabaseConnectionPool(BaseConnectionPool):

    @property
    def _settings(self):
        return get_settings()

DBConnection = DatabaseConnectionPool()


def initdb(settings):
    """Initialize the database from the given settings.
    If settings is None, the settings will be looked up via pyramid.
    """
    with psycopg2.connect(settings[CONNECTION_SETTINGS_KEY]) as db_connection:
        with db_connection.cursor() as cursor:
            for schema_filepath in DB_SCHEMA_FILES:
                with open(schema_filepath, 'r') as f:
                    cursor.execute(f.read())
            sql_constants = [os.path.join(DB_SCHEMA_DIRECTORY, filename)
                             for filename in os.listdir(DB_SCHEMA_DIRECTORY)
                             if filename.startswith('constant-')]
            for filepath in sql_constants:
                with open(filepath, 'r') as f:
                    cursor.execute(f.read())
