# -*- coding: utf-8 -*-
# ###
# Copyright (c) 2013, Rice University
# This software is subject to the provisions of the GNU Affero General
# Public License version 3 (AGPLv3).
# See LICENCE.txt for details.
# ###
import os
import json
import random
import psycopg2

from . import httpexceptions
from .app import get_settings
from .utils import split_ident_hash
from .database import SQL, DBConnection


def get_content(environ, start_response):
    """Retrieve a piece of content using the ident-hash (uuid@version)."""
    settings = get_settings()
    ident_hash = environ['wsgiorg.routing_args']['ident_hash']
    id, version = split_ident_hash(ident_hash)
    db_connection_key = random.getrandbits(256)
    db_connection = DBConnection.getconn(db_connection_key)

    # Do the module lookup
    with db_connection.cursor() as cursor:
        args = dict(id=id, version=version)
        # FIXME We are doing two queries here that can hopefully be
        #       condensed into one.
        cursor.execute(SQL['get-module-metadata'], args)
        try:
            result = cursor.fetchone()[0]
        except (TypeError, IndexError,):  # None returned
            raise httpexceptions.HTTPNotFound()
        if result['type'] == 'Collection':
            # Grab the collection tree.
            result['tree'] = None  # TODO
        else:
            # Grab the html content.
            args = dict(id=id, filename='index.html')
            cursor.execute(SQL['get-resource-by-filename'], args)
            try:
                content = cursor.fetchone()[0]
            except (TypeError, IndexError,):  # None returned
                raise httpexceptions.HTTPNotFound()
            result['content'] = content[:]
    DBConnection.putconn(db_connection, db_connection_key)

    result = json.dumps(result)
    status = "200 OK"
    headers = [('Content-type', 'application/json',)]
    start_response(status, headers)
    return [result]


def get_resource(environ, start_response):
    """Retrieve a file's data."""
    settings = get_settings()
    id = environ['wsgiorg.routing_args']['id']
    db_connection_key = random.getrandbits(256)
    db_connection = DBConnection.getconn(db_connection_key)

    # Do the module lookup
    with db_connection.cursor() as cursor:
        args = dict(id=id)
        cursor.execute(SQL['get-resource'], args)
        try:
            filename, mimetype, file = cursor.fetchone()
        except TypeError:  # None returned
            raise httpexceptions.HTTPNotFound()
    DBConnection.putconn(db_connection, db_connection_key)

    status = "200 OK"
    headers = [('Content-type', mimetype,),
               ('Content-disposition',
                "attached; filename={}".format(filename),),
               ]
    start_response(status, headers)
    return [file]


TYPE_INFO = {
    # <type-name>: (<file-extension>, <mimetype>,),
    'pdf': ('pdf', 'application/pdf',),
    'epub': ('epub', 'application/epub+zip',),
    }

def get_export(environ, start_response):
    """Retrieve an export file."""
    exports_dir = get_settings()['exports-directory']
    args = environ['wsgiorg.routing_args']
    ident_hash, type = args['ident_hash'], args['type']
    id, version = split_ident_hash(ident_hash)


    file_extension, mimetype = TYPE_INFO[type]
    filename = '{}-{}.{}'.format(id, version, file_extension)

    status = "200 OK"
    headers = [('Content-type', mimetype,),
               ('Content-disposition',
                'attached; filename={}'.format(filename),),
               ]
    start_response(status, headers)
    with open(os.path.join(exports_dir, filename), 'r') as file:
        return [file.read()]
