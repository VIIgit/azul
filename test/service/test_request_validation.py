import json
import os
import sys

from tempfile import TemporaryDirectory
from unittest import mock

import requests

import azul.changelog
from azul.logging import configure_test_logging
from azul.plugin import Plugin
from service import WebServiceTestCase


def setUpModule():
    configure_test_logging()


class FacetNameValidationTest(WebServiceTestCase):

    facet_message = {'Code': 'BadRequestError',
                     'Message': 'BadRequestError: Invalid parameter `bad-facet`'}

    def test_version(self):
        commit = 'a9eb85ea214a6cfa6882f4be041d5cce7bee3e45'
        with TemporaryDirectory() as tmpdir:
            azul.changelog.write_changes(tmpdir)
            with mock.patch('sys.path', new=sys.path + [tmpdir]):
                for dirty in True, False:
                    with self.subTest(is_repo_dirty=dirty):
                        with mock.patch.dict(os.environ, azul_git_commit=commit, azul_git_dirty=str(dirty)):
                            url = self.base_url + "/version"
                            response = requests.get(url)
                            response.raise_for_status()
                            expected_json = {
                                'commit': commit,
                                'dirty': dirty
                            }
                            self.assertEqual(response.json()['git'], expected_json)

    def test_bad_single_filter_facet_of_sample(self):
        url = self.base_url + '/repository/samples'
        params = {
            'size': 1,
            'filters': json.dumps({'bad-facet': {'is': ['fake-val']}}),
        }
        response = requests.get(url, params=params)
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.facet_message, response.json())

    def test_bad_multiple_filter_facet_of_sample(self):
        url = self.base_url + '/repository/samples'
        params = {
            'size': 1,
            'filters': json.dumps({'bad-facet': {'is': ['fake-val']}, 'bad-facet2': {'is': ['fake-val2']}}),
        }
        response = requests.get(url, params=params)
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.facet_message, response.json())

    def test_mixed_multiple_filter_facet_of_sample(self):
        url = self.base_url + '/repository/samples'
        params = {
            'size': 1,
            'filters': json.dumps({'organPart': {'is': ['fake-val']}, 'bad-facet': {'is': ['fake-val']}}),
        }
        response = requests.get(url, params=params)
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.facet_message, response.json())

    def test_bad_sort_facet_of_sample(self):
        url = self.base_url + '/repository/samples'
        params = {
            'size': 1,
            'filters': json.dumps({}),
            'sort': 'bad-facet',
            'order': 'asc',
        }
        response = requests.get(url, params=params)
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.facet_message, response.json())

    def test_bad_sort_facet_and_filter_facet_of_sample(self):
        url = self.base_url + '/repository/samples'
        params = {
            'size': 15,
            'filters': json.dumps({'bad-facet': {'is': ['fake-val']}}),
            'sort': 'bad-facet',
            'order': 'asc',
        }
        response = requests.get(url, params=params)
        self.assertEqual(400, response.status_code, response.json())
        self.assertTrue(response.json() in [self.facet_message, self.facet_message])

    def test_valid_sort_facet_but_bad_filter_facet_of_sample(self):
        url = self.base_url + '/repository/samples'
        params = {
            'size': 15,
            'filters': json.dumps({'bad-facet': {'is': ['fake-val']}}),
            'sort': 'organPart',
            'order': 'asc',
        }
        response = requests.get(url, params=params)
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.facet_message, response.json())

    def test_bad_sort_facet_but_valid_filter_facet_of_sample(self):
        url = self.base_url + '/repository/samples'
        params = {
            'size': 15,
            'filters': json.dumps({'organPart': {'is': ['fake-val2']}}),
            'sort': 'bad-facet',
            'order': 'asc',
        }
        response = requests.get(url, params=params)
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.facet_message, response.json())

    def test_bad_single_filter_facet_of_file(self):
        url = self.base_url + '/repository/files'
        params = {
            'size': 1,
            'filters': json.dumps({'bad-facet': {'is': ['fake-val2']}}),
        }
        response = requests.get(url, params=params)
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.facet_message, response.json())

    def test_bad_multiple_filter_facet_of_file(self):
        url = self.base_url + '/repository/files'
        params = {
            'size': 1,
            'filters': json.dumps({'bad-facet': {'is': ['fake-val']}, 'bad-facet2': {'is': ['fake-val2']}}),
        }
        response = requests.get(url, params=params)
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.facet_message, response.json())

    def test_mixed_multiple_filter_facet_of_file(self):
        url = self.base_url + '/repository/files'
        params = {
            'size': 1,
            'filters': json.dumps({'organPart': {'is': ['fake-val']}, 'bad-facet': {'is': ['fake-val']}}),
        }
        response = requests.get(url, params=params)
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.facet_message, response.json())

    def test_bad_sort_facet_of_file(self):
        url = self.base_url + '/repository/files'
        params = {
            'size': 15,
            'sort': 'bad-facet',
            'order': 'asc',
            'filters': json.dumps({}),
        }
        response = requests.get(url, params=params)
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.facet_message, response.json())

    def test_bad_sort_facet_and_filter_facet_of_file(self):
        url = self.base_url + '/repository/files'
        params = {
            'size': 15,
            'filters': json.dumps({'bad-facet': {'is': ['fake-val2']}}),
        }
        response = requests.get(url, params=params)
        self.assertEqual(400, response.status_code, response.json())
        self.assertTrue(response.json() in [self.facet_message, self.facet_message])

    def test_bad_sort_facet_but_valid_filter_facet_of_file(self):
        url = self.base_url + '/repository/files'
        params = {
            'size': 15,
            'sort': 'bad-facet',
            'order': 'asc',
            'filters': json.dumps({'organ': {'is': ['fake-val2']}}),
        }
        response = requests.get(url, params=params)
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.facet_message, response.json())

    def test_valid_sort_facet_but_bad_filter_facet_of_file(self):

        url = self.base_url + '/repository/files'
        params = {
            'size': 15,
            'sort': 'organPart',
            'order': 'asc',
            'filters': json.dumps({'bad-facet': {'is': ['fake-val2']}}),
        }
        response = requests.get(url, params=params)
        self.assertEqual(400, response.status_code, response.json())
        self.assertEqual(self.facet_message, response.json())

    def test_single_entity_error_responses(self):
        entity_types = ['files', 'projects']
        for uuid, expected_error_code in [('2b7959bb-acd1-4aa3-9557-345f9b3c6327', 404),
                                          ('-0c5ac7c0-817e-40d4-b1b1-34c3d5cfecdb-', 400),
                                          ('FOO', 400)]:
            for entity_type in entity_types:
                with self.subTest(entity_name=entity_type, error_code=expected_error_code, uuid=uuid):
                    url = self.base_url + f'/repository/{entity_type}/{uuid}'
                    response = requests.get(url)
                    self.assertEqual(expected_error_code, response.status_code)

    def test_file_order(self):
        url = self.base_url + '/repository/files/order'
        response = requests.get(url)
        self.assertEqual(200, response.status_code, response.json())
        actual_field_order = response.json()['order']
        expected_field_order = Plugin.load().service_config().order_config
        self.assertEqual(expected_field_order, actual_field_order)

    def test_bad_query_params(self):
        entity_types = ['files', 'bundles', 'samples']
        for entity_type in entity_types:
            url = self.base_url + f'/repository/{entity_type}'
            with self.subTest(test='extra parameter', entity_type=entity_type):
                params = {
                    'some_nonexistent_filter': 1,
                }
                response = requests.get(url, params=params)
                self.assertEqual(400, response.status_code, response.json())
                self.assertEqual('BadRequestError', response.json()['Code'])
            with self.subTest(test='malformed parameter', entity_type=entity_type):
                params = {
                    'size': 'foo',
                }
                response = requests.get(url, params=params)
                self.assertEqual(400, response.status_code, response.json())
                self.assertEqual('BadRequestError', response.json()['Code'])
