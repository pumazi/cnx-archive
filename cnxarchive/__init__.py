# -*- coding: utf-8 -*-
# ###
# Copyright (c) 2013, Rice University
# This software is subject to the provisions of the GNU Affero General
# Public License version 3 (AGPLv3).
# See LICENCE.txt for details.
# ###
"""Document and collection archive web application."""

from .app import Application


def main(global_config, **settings):
    """Main WSGI application factory."""
    app = Application(settings)
    app.add_route('/contents/{ident_hash}', 'cnxarchive.views:get_content')
    app.add_route('/resources/{id}', 'cnxarchive.views:get_resource')
    if app.is_in_dev_mode:
        if not settings.get('exports-directory', None):
            raise ValueError("Missing exports-directory configuration setting.")
        app.add_route('/exports/{ident_hash}/{type}', 'cnxarchive.views:get_export')
    return app
