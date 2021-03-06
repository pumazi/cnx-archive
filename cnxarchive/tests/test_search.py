# -*- coding: utf-8 -*-
# ###
# Copyright (c) 2013, Rice University
# This software is subject to the provisions of the GNU Affero General
# Public License version 3 (AGPLv3).
# See LICENCE.txt for details.
# ###
import os
import json
import unittest
import uuid

import psycopg2

from . import *
from ..search import DEFAULT_QUERY_TYPE


with open(os.path.join(TEST_DATA_DIRECTORY, 'raw-search-rows.json'), 'r') as fb:
    # Search results for a search on 'physics'.
    RAW_QUERY_RECORDS = json.load(fb)


class SearchModelTestCase(unittest.TestCase):
    fixture = postgresql_fixture

    @classmethod
    def setUpClass(cls):
        from ..utils import parse_app_settings
        cls.settings = parse_app_settings(TESTING_CONFIG)
        from ..database import CONNECTION_SETTINGS_KEY
        cls.db_connection_string = cls.settings[CONNECTION_SETTINGS_KEY]
        cls._db_connection = psycopg2.connect(cls.db_connection_string)

    @classmethod
    def tearDownClass(cls):
        cls._db_connection.close()

    def setUp(self):
        from .. import _set_settings
        _set_settings(self.settings)
        self.fixture.setUp()
        # Load the database with example legacy data.
        with self._db_connection.cursor() as cursor:
            with open(TESTING_DATA_SQL_FILE, 'rb') as fb:
                cursor.execute(fb.read())
            with open(TESTING_CNXUSER_DATA_SQL_FILE, 'r') as fb:
                cursor.execute(fb.read())
        self._db_connection.commit()

    def tearDown(self):
        from .. import _set_settings
        _set_settings(None)
        self.fixture.tearDown()

    def make_queryrecord(self, *args, **kwargs):
        from ..search import QueryRecord
        return QueryRecord(*args, **kwargs)

    def make_queryresults(self, *args, **kwargs):
        from ..search import QueryResults
        return QueryResults(*args, **kwargs)

    def test_summary_highlighting(self):
        # Confirm the record highlights on found terms in the abstract/summary.
        record = self.make_queryrecord(**RAW_QUERY_RECORDS[0][0])

        expected = """algebra-based, two-semester college <b>physics</b> book is grounded with real-world examples, illustrations, and explanations to help students grasp key, fundamental <b>physics</b> concepts. This online, fully editable and customizable title includes learning objectives, concept questions, links to labs and simulations, and ample practice opportunities to solve traditional <b>physics</b> application problems."""
        self.assertEqual(record.highlighted_abstract, expected)

    def test_fulltext_highlighting(self):
        # Confirm the record highlights on found terms in the fulltext.
        record = self.make_queryrecord(**RAW_QUERY_RECORDS[0][0])

        expected = None
        # XXX Something wrong with the data, but otherwise this works as
        #     expected.
        self.assertEqual(record.highlighted_fulltext, expected)

    def test_result_counts(self):
        # Set the test to return top 5 keywords
        from .. import search
        old_max_values_for_keywords = search.MAX_VALUES_FOR_KEYWORDS
        def reset_max_values_for_keywords():
            search.MAX_VALUES_FOR_KEYWORDS = old_max_values_for_keywords
        self.addCleanup(reset_max_values_for_keywords)
        search.MAX_VALUES_FOR_KEYWORDS = 5

        # Verify the counts on the results object.
        query = [('text', 'physics')]
        results = self.make_queryresults(RAW_QUERY_RECORDS, query)

        self.assertEqual(len(results), 15)
        # Check the type counts.
        from ..utils import MODULE_MIMETYPE, COLLECTION_MIMETYPE
        types = results.counts['type']
        self.assertEqual(types, [
            (COLLECTION_MIMETYPE, 1,),
            (MODULE_MIMETYPE, 14,),
            ])
        # Check the author counts
        osc_physics = {u'email': u'info@openstaxcollege.org',
                       u'firstname': u'College',
                       u'fullname': u'OSC Physics Maintainer',
                       u'id': u'1df3bab1-1dc7-4017-9b3a-960a87e706b1',
                       u'othername': None,
                       u'suffix': None,
                       u'surname': u'Physics',
                       u'title': None,
                       u'website': None}
        open_stax_college = {u'website': None,
                             u'surname': None,
                             u'suffix': None,
                             u'firstname': u'OpenStax College',
                             u'title': None,
                             u'othername': None,
                             u'id': u'e5a07af6-09b9-4b74-aa7a-b7510bee90b8',
                             u'fullname': u'OpenStax College',
                             u'email': u'info@openstaxcollege.org'}

        expected = [(open_stax_college['id'], 15,), (osc_physics['id'], 1,)]
        self.assertEqual(results.counts['authorID'], expected)

        # Check counts for publication year.
        pub_years = list(results.counts['pubYear'])
        self.assertEqual(pub_years, [(u'2013', 12), (u'2012', 1), (u'2011', 2)])

        # Check the subject counts.
        subjects = dict(results.counts['subject'])
        self.assertEqual(subjects,
                         {u'Mathematics and Statistics': 8,
                          u'Science and Technology': 7,
                          })

        # Check the keyword counts.
        keywords = results.counts['keyword']
        self.assertEqual(len(keywords), 5)
        self.assertEqual(keywords, [(u'force', 3),
                                    (u'friction', 4),
                                    (u'Modern physics', 2),
                                    (u'Quantum mechanics', 2),
                                    (u'Scientific method', 2),
                                   ])

    def test_result_counts_with_author_limit(self):
        # Set the test to return top 1 author
        from .. import search
        old_max_values_for_authors = search.MAX_VALUES_FOR_AUTHORS
        self.addCleanup(setattr, search, 'MAX_VALUES_FOR_AUTHORS',
                        old_max_values_for_authors)
        search.MAX_VALUES_FOR_AUTHORS = 1

        query = [('text', 'physics')]
        results = self.make_queryresults(RAW_QUERY_RECORDS, query)

        open_stax_college = {u'website': None,
                             u'surname': None,
                             u'suffix': None,
                             u'firstname': u'OpenStax College',
                             u'title': None,
                             u'othername': None,
                             u'id': u'e5a07af6-09b9-4b74-aa7a-b7510bee90b8',
                             u'fullname': u'OpenStax College',
                             u'email': u'info@openstaxcollege.org'}

        # Check there is only one author returned
        authors = results.counts['authorID']
        self.assertEqual(authors, [(open_stax_college['id'], 15,)])

    def test_auxiliary_authors(self):
        # Check that the query results object contains a list of all the
        #   authors that appear in the results.
        from .. import search
        query = [('text', 'physics')]
        results = self.make_queryresults(RAW_QUERY_RECORDS, query)

        # Simple quantity check before quality.
        authors = results.auxiliary['authors']
        self.assertEqual(len(authors), 2)
        # Check the contents after sorting the results.
        authors = sorted(authors, key=lambda x: x['id'])
        expected = [{u'website': None, u'surname': u'Physics', u'suffix': None, u'firstname': u'College', u'title': None, u'othername': None, u'fullname': u'OSC Physics Maintainer', u'email': u'info@openstaxcollege.org', u'id': u'1df3bab1-1dc7-4017-9b3a-960a87e706b1'}, {u'website': None, u'surname': None, u'suffix': None, u'firstname': u'OpenStax College', u'title': None, u'othername': None, u'fullname': u'OpenStax College', u'email': u'info@openstaxcollege.org', u'id': u'e5a07af6-09b9-4b74-aa7a-b7510bee90b8'}]
        self.assertEqual(authors, expected)


class SearchTestCase(unittest.TestCase):
    fixture = postgresql_fixture

    @classmethod
    def setUpClass(cls):
        from ..utils import parse_app_settings
        cls.settings = parse_app_settings(TESTING_CONFIG)
        from ..database import CONNECTION_SETTINGS_KEY
        cls.db_connection_string = cls.settings[CONNECTION_SETTINGS_KEY]
        cls._db_connection = psycopg2.connect(cls.db_connection_string)

    @classmethod
    def tearDownClass(cls):
        cls._db_connection.close()

    def setUp(self):
        from .. import _set_settings
        _set_settings(self.settings)
        self.fixture.setUp()
        # Load the database with example legacy data.
        with self._db_connection.cursor() as cursor:
            with open(TESTING_DATA_SQL_FILE, 'rb') as fb:
                cursor.execute(fb.read())
            with open(TESTING_CNXUSER_DATA_SQL_FILE, 'r') as fb:
                cursor.execute(fb.read())
        self._db_connection.commit()

    def tearDown(self):
        from .. import _set_settings
        _set_settings(None)
        self.fixture.tearDown()

    def call_target(self, query_params, query_type=DEFAULT_QUERY_TYPE):
        # Single point of import failure.
        from ..search import search, Query
        self.query = Query(query_params)
        self.addCleanup(delattr, self, 'query')
        return search(self.query, query_type=query_type)

    def test_title_search(self):
        # Simple case to test for results of a basic title search.
        query_params = [('title', 'Physics')]
        results = self.call_target(query_params)

        self.assertEqual(len(results), 5)

    def test_abstract_search(self):
        # Test for result on an abstract search.
        query_params = [('abstract', 'algebra')]
        results = self.call_target(query_params)

        self.assertEqual(len(results), 2)
        self.assertEqual(results[0].fields['abstract'], set(['algebra']))

    def test_author_search(self):
        # Test the results of an author search.
        user_id = str(uuid.uuid4())
        query_params = [('author', 'Jill')]

        with psycopg2.connect(self.db_connection_string) as db_connection:
            with db_connection.cursor() as cursor:
                # Create a new user.
                cursor.execute(
                    "INSERT INTO users "
                    "(id, firstname, surname, fullname, email) "
                    "VALUES (%s, %s, %s, %s, %s);",
                    (user_id, 'Jill', 'Miller', 'Jill M.',
                     'jmiller@example.com',))
                # Update two modules in include this user as an author.
                cursor.execute(
                    "UPDATE latest_modules SET (authors) = (%s) "
                    "WHERE module_ident = %s OR module_ident = %s;",
                    ([user_id], 2, 3,))
            db_connection.commit()

        results = self.call_target(query_params)
        self.assertEqual(len(results), 2)

    def test_editor_search(self):
        # Test the results of an editor search.
        user_id = str(uuid.uuid4())
        query_params = [('editor', 'jmiller@example.com')]

        with psycopg2.connect(self.db_connection_string) as db_connection:
            with db_connection.cursor() as cursor:
                # Create a new user.
                cursor.execute(
                    "INSERT INTO users "
                    "(id, firstname, surname, fullname, email) "
                    "VALUES (%s, %s, %s, %s, %s);",
                    (user_id, 'Jill', 'Miller', 'Jill M.',
                     'jmiller@example.com',))
                # Update two modules in include this user as an editor.
                role_id = 5
                cursor.execute(
                    "INSERT INTO moduleoptionalroles"
                    "(personids, module_ident, roleid) VALUES (%s, %s, %s);",
                    ([user_id], 2, role_id))
                cursor.execute(
                    "INSERT INTO moduleoptionalroles"
                    "(personids, module_ident, roleid) VALUES (%s, %s, %s);",
                    ([user_id], 3, role_id))
            db_connection.commit()

        results = self.call_target(query_params)
        self.assertEqual(len(results), 2)

    def test_licensor_search(self):
        # Test the results of a licensor search.
        user_id = str(uuid.uuid4())
        query_params = [('licensor', 'jmiller')]

        with psycopg2.connect(self.db_connection_string) as db_connection:
            with db_connection.cursor() as cursor:
                # Create a new user.
                cursor.execute(
                    "INSERT INTO users "
                    "(id, firstname, surname, fullname, email) "
                    "VALUES (%s, %s, %s, %s, %s);",
                    (user_id, 'Jill', 'Miller', 'Jill M.',
                     'jmiller@example.com',))
                # Update two modules in include this user as a licensor.
                role_id = 2
                cursor.execute(
                    "INSERT INTO moduleoptionalroles"
                    "(personids, module_ident, roleid) VALUES (%s, %s, %s);",
                    ([user_id], 2, role_id))
                cursor.execute(
                    "INSERT INTO moduleoptionalroles"
                    "(personids, module_ident, roleid) VALUES (%s, %s, %s);",
                    ([user_id], 3, role_id))
            db_connection.commit()

        results = self.call_target(query_params)
        self.assertEqual(len(results), 2)

    def test_maintainer_search(self):
        # Test the results of a maintainer search.
        user_id = str(uuid.uuid4())
        query_params = [('maintainer', 'Miller')]

        with psycopg2.connect(self.db_connection_string) as db_connection:
            with db_connection.cursor() as cursor:
                # Create a new user.
                cursor.execute(
                    "INSERT INTO users "
                    "(id, firstname, surname, fullname, email) "
                    "VALUES (%s, %s, %s, %s, %s);",
                    (user_id, 'Jill', 'Miller', 'Jill M.',
                     'jmiller@example.com',))
                # Update two modules in include this user as a maintainer.
                cursor.execute(
                    "UPDATE latest_modules SET (maintainers) = (%s) "
                    "WHERE module_ident = %s OR module_ident = %s;",
                    ([user_id], 2, 3,))
            db_connection.commit()

        results = self.call_target(query_params)
        self.assertEqual(len(results), 2)

    def test_translator_search(self):
        # Test the results of a translator search.
        user_id = str(uuid.uuid4())
        query_params = [('translator', 'jmiller')]

        with psycopg2.connect(self.db_connection_string) as db_connection:
            with db_connection.cursor() as cursor:
                # Create a new user.
                cursor.execute(
                    "INSERT INTO users "
                    "(id, firstname, surname, fullname, email) "
                    "VALUES (%s, %s, %s, %s, %s);",
                    (user_id, 'Jill', 'Miller', 'Jill M.',
                     'jmiller@example.com',))
                # Update two modules in include this user as a translator.
                role_id = 4
                cursor.execute(
                    "INSERT INTO moduleoptionalroles"
                    "(personids, module_ident, roleid) VALUES (%s, %s, %s);",
                    ([user_id], 2, role_id))
                cursor.execute(
                    "INSERT INTO moduleoptionalroles"
                    "(personids, module_ident, roleid) VALUES (%s, %s, %s);",
                    ([user_id], 3, role_id))
            db_connection.commit()

        results = self.call_target(query_params)
        self.assertEqual(len(results), 2)

    def test_parentauthor_search(self):
        # Test the results of a parent author search.
        user_id = str(uuid.uuid4())
        # FIXME parentauthor is only searchable by user id, not by name
        #       like the other user based columns. Inconsistent behavior...
        query_params = [('parentauthor', user_id)]

        with psycopg2.connect(self.db_connection_string) as db_connection:
            with db_connection.cursor() as cursor:
                # Create a new user.
                cursor.execute(
                    "INSERT INTO users "
                    "(id, firstname, surname, fullname, email) "
                    "VALUES (%s, %s, %s, %s, %s);",
                    (user_id, 'Jill', 'Miller', 'Jill M.',
                     'jmiller@example.com',))
                # Update two modules in include this user as a parent author.
                cursor.execute(
                    "UPDATE latest_modules SET (parentauthors) = (%s) "
                    "WHERE module_ident = %s OR module_ident = %s;",
                    ([user_id], 2, 3,))
            db_connection.commit()

        results = self.call_target(query_params)
        self.assertEqual(len(results), 2)

    def test_fulltext_search(self):
        # Test the results of a search on fulltext.
        query_params = [('fulltext', 'uncertainty'), ('fulltext', 'rotation')]

        results = self.call_target(query_params)
        self.assertEqual(len(results), 1)
        # Ensure the record with both values is the only result.
        self.assertEqual(results[0]['id'],
                         'ae3e18de-638d-4738-b804-dc69cd4db3a3')

    def test_type_filter_on_books(self):
        # Test for type filtering that will find books only.
        query_params = [('text', 'physics'), ('type', 'book')]

        results = self.call_target(query_params)
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]['id'],
                         'e79ffde3-7fb4-4af3-9ec8-df648b391597')

    def test_type_filter_on_pages(self):
        # Test for type filtering that will find books only.
        query_params = [('text', 'physics'), ('type', 'page')]

        results = self.call_target(query_params)
        result_ids = [r['id'] for r in results]
        self.assertEqual(len(results), 14)
        # Check that the collection/book is not in the results.
        self.assertNotIn('e79ffde3-7fb4-4af3-9ec8-df648b391597',
                         result_ids)

    def test_type_filter_case_insensitive(self):
        query_params = [('text', 'physics'), ('type', 'Book')]

        results = self.call_target(query_params)
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]['id'],
                         'e79ffde3-7fb4-4af3-9ec8-df648b391597')

    def test_type_filter_on_unknown(self):
        # Test for type filtering on an unknown type.
        query_params = [('text', 'physics'), ('type', 'image')]

        results = self.call_target(query_params)
        # Check for the removal of the filter
        self.assertEqual(self.query.filters, [])

    @db_connect
    def _pubYear_setup(self, cursor):
        # Modify some modules to give them different year of publication
        pub_year_mods = {
                '2010': ['e79ffde3-7fb4-4af3-9ec8-df648b391597',
                         '209deb1f-1a46-4369-9e0d-18674cf58a3e'],
                '2012': ['f3c9ab70-a916-4d8c-9256-42953287b4e9'],
                }

        for year, ids in pub_year_mods.iteritems():
            cursor.execute(
                    "UPDATE latest_modules "
                    "SET revised = '{}-07-31 12:00:00.000000-07'"
                    "WHERE uuid IN %s RETURNING module_ident".format(year),
                    [tuple(ids)])

    def test_pubYear_limit(self):
        self._pubYear_setup()

        # Test for limit only results with pubYear 2013
        query_params = [('pubYear', '2013')]

        results = self.call_target(query_params)
        result_ids = [r['id'] for r in results]
        self.assertEqual(len(results), 13)
        self.assertNotIn('e79ffde3-7fb4-4af3-9ec8-df648b391597', result_ids)
        self.assertNotIn('209deb1f-1a46-4369-9e0d-18674cf58a3e', result_ids)
        self.assertNotIn('f3c9ab70-a916-4d8c-9256-42953287b4e9', result_ids)

    def test_pubYear_filter(self):
        self._pubYear_setup()

        # Test for filtering results with pubYear 2013
        query_params = [('text', 'physics'), ('pubYear', '2013')]

        results = self.call_target(query_params)
        result_ids = [r['id'] for r in results]
        self.assertEqual(len(results), 13)
        self.assertNotIn('e79ffde3-7fb4-4af3-9ec8-df648b391597', result_ids)
        self.assertNotIn('209deb1f-1a46-4369-9e0d-18674cf58a3e', result_ids)
        self.assertNotIn('f3c9ab70-a916-4d8c-9256-42953287b4e9', result_ids)

    def test_pubYear_filter_no_results(self):
        self._pubYear_setup()

        # Test for filtering results with pubYear 2011
        query_params = [('text', 'physics'), ('pubYear', '2011')]

        results = self.call_target(query_params)
        result_ids = [r['id'] for r in results]
        self.assertEqual(len(results), 0)

    def test_pubYear_without_term(self):
        self._pubYear_setup()
        query_params = [('pubYear', '2010')]

        results = self.call_target(query_params)
        result_ids = [r['id'] for r in results]
        self.assertEqual(len(results), 2)
        self.assertEqual(result_ids, ['e79ffde3-7fb4-4af3-9ec8-df648b391597',
                                      '209deb1f-1a46-4369-9e0d-18674cf58a3e'])

    def test_type_without_term(self):
        query_params = [('type', 'book')]

        results = self.call_target(query_params)
        result_ids = [r['id'] for r in results]
        self.assertEqual(len(results), 2)
        self.assertEqual(result_ids, ['e79ffde3-7fb4-4af3-9ec8-df648b391597',
                                      'a733d0d2-de9b-43f9-8aa9-f0895036899e'])

    def test_authorId_filter(self):
        # Filter results by author "OSC Physics Maintainer"
        query_params = [('text', 'physics'),
                        ('authorID', '1df3bab1-1dc7-4017-9b3a-960a87e706b1')]

        results = self.call_target(query_params)
        result_ids = [r['id'] for r in results]
        self.assertEqual(len(results), 1)
        self.assertEqual(result_ids, ['209deb1f-1a46-4369-9e0d-18674cf58a3e'])

    @db_connect
    def _language_setup(self, cursor):
        # Modify some modules to give them different languages
        language_mods = {
                'fr': ['209deb1f-1a46-4369-9e0d-18674cf58a3e'],
                }

        for language, ids in language_mods.iteritems():
            cursor.execute(
                    "UPDATE latest_modules "
                    "SET language = %s"
                    "WHERE uuid IN %s RETURNING module_ident",
                    [language, tuple(ids)])

    def test_language_without_term(self):
        self._language_setup()
        query_params = [('language', 'fr')]

        results = self.call_target(query_params)
        result_ids = [r['id'] for r in results]
        self.assertEqual(len(results), 1)
        self.assertEqual(result_ids, ['209deb1f-1a46-4369-9e0d-18674cf58a3e'])

    def test_term_and_subject(self):
        query_params = [('text', 'physics'),
                        ('subject', 'Science and Technology')]

        results = self.call_target(query_params)
        result_weights = [(r['id'],r['weight']) for r in results]
        self.assertEqual(len(results), 7)
        self.assertEqual(result_weights, [(u'e79ffde3-7fb4-4af3-9ec8-df648b391597',221),
                                      (u'ea271306-f7f2-46ac-b2ec-1d80ff186a59',21),
                                      (u'56f1c5c1-4014-450d-a477-2121e276beca',21),
                                      (u'f6024d8a-1868-44c7-ab65-45419ef54881',20),
                                      (u'c0a76659-c311-405f-9a99-15c71af39325',20),
                                      (u'26346a42-84b9-48ad-9f6a-62303c16ad41',20),
                                      (u'24a2ed13-22a6-47d6-97a3-c8aa8d54ac6d',20),
                                     ])

    def test_subject_and_subject(self):
        query_params = [('subject', 'Science and Technology'),
                        ('subject', 'Mathematics and Statistics')]

        results = self.call_target(query_params)
        result_ids = [r['id'] for r in results]
        self.assertEqual(len(results), 1)
        self.assertEqual(result_ids, ['e79ffde3-7fb4-4af3-9ec8-df648b391597'])

    def test_subject_authorID_term(self):
        query_params = [('text', 'physics'),
                        ('subject', 'Mathematics and Statistics'),
                        # "OSC Physics Maintainer"
                        ('authorID', '1df3bab1-1dc7-4017-9b3a-960a87e706b1')]

        results = self.call_target(query_params)
        result_ids = [r['id'] for r in results]
        self.assertEqual(len(result_ids), 1)
        self.assertEqual(result_ids, ['209deb1f-1a46-4369-9e0d-18674cf58a3e'])

    def test_sort_filter_on_pubdate(self):
        # Test the sorting of results by publication date.
        query_params = [('text', 'physics'), ('sort', 'pubDate')]
        _same_date = '2113-01-01 00:00:00 America/New_York'
        expectations = [('d395b566-5fe3-4428-bcb2-19016e3aa3ce',
                         _same_date,),  # this one has a higher weight.
                        ('c8bdbabc-62b1-4a5f-b291-982ab25756d7',
                         _same_date,),
                        ('5152cea8-829a-4aaf-bcc5-c58a416ecb66',
                         '2112-01-01 00:00:00 America/New_York',),
                        ]

        with psycopg2.connect(self.db_connection_string) as db_connection:
            with db_connection.cursor() as cursor:
                # Update two modules in include a creation date.
                for id, date in expectations:
                    cursor.execute(
                        "UPDATE latest_modules SET (revised) = (%s) "
                        "WHERE uuid = %s::uuid;", (date, id))
            db_connection.commit()

        results = self.call_target(query_params)
        self.assertEqual(len(results), 16)
        for i, (id, date) in enumerate(expectations):
            self.assertEqual(results[i]['id'], id)

    def test_sort_filter_on_popularity(self):
        # Test the sorting of results by popularity (hit statistics).
        query_params = [('text', 'physics'), ('sort', 'popularity')]
        # The top three items we are looking have their normal sort
        #   index in a comment to the left, just to show where it came from.
        expectations = [
            # ident: uuid
            (8, u'24a2ed13-22a6-47d6-97a3-c8aa8d54ac6d',),  # 10
            (7, u'5838b105-41cd-4c3d-a957-3ac004a48af3',),  # 4
            (9, u'ea271306-f7f2-46ac-b2ec-1d80ff186a59',),  # 5
            # No hits applied from here on, normal ordering expected.
            (1, u'e79ffde3-7fb4-4af3-9ec8-df648b391597',),  # 1
            ]
        hits_to_apply = {8: 25, 7: 15, 9: 5, 1: 0}

        from datetime import datetime, timedelta
        with psycopg2.connect(self.db_connection_string) as db_connection:
            with db_connection.cursor() as cursor:
                 end = datetime.today()
                 start = end - timedelta(1)
                 for ident, hits in hits_to_apply.items():
                     cursor.execute("INSERT INTO document_hits "
                                    "VALUES (%s, %s, %s, %s);",
                                    (ident, start, end, hits,))
                     cursor.execute("SELECT update_hit_ranks();")

        results = self.call_target(query_params)
        for i, (ident, id) in enumerate(expectations):
            self.assertEqual(results[i]['id'], id)

    def test_anding(self):
        # Test that the results intersect with one another rather than
        #   search the terms independently. This uses the AND operator.
        # The query for this would look like "physics [AND] force".
        query_params = [('text', 'physics'), ('text', 'force'),
                        ('keyword', 'stress'),
                        ]
        expectations = ['24a2ed13-22a6-47d6-97a3-c8aa8d54ac6d',
                         '56f1c5c1-4014-450d-a477-2121e276beca',
                         ]
        matched_on = [{u'force': set([u'fulltext', u'keyword']),
                       u'physics': set([u'maintainer']),
                       u'stress': set([u'keyword'])},
                      {u'force': set([u'fulltext', u'keyword']),
                       u'physics': set([u'fulltext', u'maintainer']),
                       u'stress': set([u'keyword'])},
                      ]

        results = self.call_target(query_params, query_type='AND')
        # Basically, everything matches the first search term,
        #   about eleven match the first two terms,
        #   and when the third is through in we condense this to two.
        self.assertEqual(len(results), 2)
        for i, id in enumerate(expectations):
            self.assertEqual(results[i]['id'], id)
        # This just verifies that all three terms matched on each result.
        self.assertEqual([r.matched for r in results], matched_on)

    def test_weak_anding(self):
        # Test that the results intersect with one another rather than
        #   search the terms independently. This uses the weakAND operator.
        # The query for this would look like "physics [weakAND] force".
        # This will drop any term-set that doesn't match anything
        #   and use the remaining terms to do a traditional AND against.
        query_params = [('text', 'physics'), ('text', 'force'),
                        ('keyword', 'contentment'),
                        ]
        expectations = ['e79ffde3-7fb4-4af3-9ec8-df648b391597',
                        'f3c9ab70-a916-4d8c-9256-42953287b4e9',
                        'd395b566-5fe3-4428-bcb2-19016e3aa3ce',
                        ]
        matched_on_keys = [[u'force', u'physics'],
                           [u'force', u'physics'],
                           [u'force', u'physics'],
                           [u'physics', u'force'],
                           [u'force', u'physics'],
                           [u'physics', u'force'],
                           [u'force', u'physics'],
                           [u'force', u'physics'],
                           [u'force', u'physics'],
                           [u'physics', u'force'],
                           [u'physics', u'force'],
                           ]

        results = self.call_target(query_params, query_type='weakAND')
        # Basically, everything matches the first search term,
        #   about eleven match the first two terms,
        #   and when the third is through in we condense this to two.
        self.assertEqual(len(results), 11)
        for i, id in enumerate(expectations):
            self.assertEqual(results[i]['id'], id)
        # This just verifies that only two of the three terms
        #   matched on each result.
        self.assertEqual([r.matched.keys() for r in results],
                         matched_on_keys)
