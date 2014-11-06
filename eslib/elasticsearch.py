from __future__ import absolute_import

import logging

import curator
from elasticsearch import TransportError
from elasticsearch.client import IndicesClient


def is_es_response_ok(resp):
    """
    Check if Elasticsearch response data corresponds to successful completion of the request.

    :param resp: dict
    :return: True if the request was successful.
    """
    if resp.has_key('acknowledged'):
        return resp['acknowledged']

    return False


class ESRequestFailedException(BaseException):
    """
    Exception that signals an error returned from a Elasticsearch query or request.
    """
    pass


def create_index(es_client, new_index):
    """
    Create a new index.

    Takes only an index name, the remaining configuration is assumed to be based on an index template.

    :param es_client: elasticsearch.client.Elasticsearch Elasticsearch client instance (from elasticsearch package).
    :param new_index: str Name of the new index.
    :raises eslib.elasticsearch.ESRequestFailedException
    :return:
    """
    # Note this method might be redundant but may be useful until we know all the exceptions and return values that
    # might result from upstream libraries.
    ic = IndicesClient(es_client)

    logging.getLogger(__name__).info('Creating index %s ...' % new_index)

    try:
        resp = ic.create(new_index)
    # Reraise exceptions for now
    except TransportError, e:
        raise e

    if not is_es_response_ok(resp):
        logging.getLogger(__name__).error('Could not create index %s' % new_index)
        raise ESRequestFailedException


def rename_index_alias(es_client, alias, current_index, new_index):
    """
    Change the index an index alias is pointing to.

    :param es_client: elasticsearch.client.Elasticsearch Elasticsearch client instance (from elasticsearch package).
    :param alias: str Name of the alias
    :param new_index: str Name of new index to change alias to.
    :param current_index: str Name of the index the alias is currently pointing to.
    :raises eslib.elasticsearch.ESRequestFailedException
    :return:
    """
    ic = IndicesClient(es_client)

    # point alias to new index. Operation is atomic pr. Elasticsearch documentation
    # http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/indices-aliases.html
    resp = ic.update_aliases(body={"actions": [{"remove": {"index": current_index, "alias": alias}},
                                               {"add": {"index": new_index, "alias": alias}}]})
    if not is_es_response_ok(resp):
        logging.getLogger(__name__).error('Could not create index %s' % new_index)
        raise ESRequestFailedException


def rotate_indices(es_client, new_index, current_index, alias):
    """

    :param es_client: elasticsearch.client.Elasticsearch Elasticsearch client instance (from elasticsearch package).
    :param new_index: str Name of the index to rotate to.
    :param current_index: str Name of the index to rotate from.
    :param alias: str Alias indicating current index in the rotation.
    :raises eslib.elasticsearch.ESRequestFailedException
    :return:
    """
    logging.getLogger(__name__).info('Creating index %s ...' % new_index)
    create_index(es_client, new_index)

    logging.getLogger(__name__).info('Moving alias %s from index %s to index %s ...' %
                                     (alias, current_index, new_index))
    rename_index_alias(es_client, alias, current_index, new_index)

    logging.getLogger(__name__).info('Optimizing index %s ...' % current_index)
    curator.optimize_index(es_client, current_index)
