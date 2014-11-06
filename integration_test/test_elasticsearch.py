from random import randint
from unittest import TestCase

from elasticsearch.client import Elasticsearch
from elasticsearch.client.indices import IndicesClient
from elasticsearch.exceptions import TransportError

from eslib.elasticsearch import create_index, is_es_response_ok, rename_index_alias, rotate_indices


def random_index_name():
    return 'test-index-%d' % randint(1, 1000)


def random_alias_name():
    return 'test-alias-%d' % randint(1, 1000)


class TestElasticsearch(TestCase):
    """
    Integration test for eslib.elasticsearch.

    Requires a local Elasticsearch instance.
    """
    def setUp(self):
        super(TestElasticsearch, self).setUp()

        # TODO allow configuration of non-local instance
        self.es = Elasticsearch()

    def test_create_index(self):
        index_name = random_index_name()

        ic = IndicesClient(self.es)

        if ic.exists(index_name):
            # shouldn't happen unless indices are never cleaned up for some reason
            self.fail("Index %s already exists, failing test.")
            
        create_index(self.es, index_name)
        
        self.assertTrue(ic.exists(index_name))

        self.assertRaises(TransportError, create_index, self.es, index_name)

        ic.delete(index_name)

    def test_is_es_response_ok(self):
        self.assertTrue(is_es_response_ok({'acknowledged': True}))
        self.assertFalse(is_es_response_ok({'acknowledged': False}))
        self.assertFalse(is_es_response_ok({'junk': True}))

    def test_rename_index_alias(self):
        cur_index = random_index_name()
        new_index = random_index_name()
        alias = random_alias_name()

        ic = IndicesClient(self.es)
        ic.create(cur_index)
        ic.create(new_index)
        ic.put_alias(alias, index=cur_index)

        rename_index_alias(self.es, alias, cur_index, new_index)

        resp = ic.get_alias(alias)
        self.assertEqual(resp, {new_index: {'aliases': {alias: {}}}})

        ic.delete_alias(new_index, alias)
        ic.delete(new_index)
        ic.delete(cur_index)

    def test_rotate_indices(self):
        cur_index = random_index_name()
        new_index = random_index_name()
        alias = random_alias_name()

        ic = IndicesClient(self.es)
        ic.create(cur_index)
        ic.put_alias(alias, index=cur_index)

        rotate_indices(self.es, new_index, cur_index, alias)
        resp = ic.get_alias(alias)
        self.assertEqual(resp, {new_index: {'aliases': {alias: {}}}})

        ic.delete_alias(new_index, alias)
        ic.delete(new_index)
        ic.delete(cur_index)