import argparse
import logging

from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan, bulk


"""
Quick script for copying all documents in an index to another. Adjust to fit.
"""

def bulk_create(es, docs):
    return bulk(es, docs)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser('Copies all documents in one index to another.')
    parser.add_argument('-s', '--server', default='localhost', help='Elasticsearch server host (default: localhost).')
    parser.add_argument('-p', '--port', default=9200, type=int, help='Elasticsearch server port (default: 9200).')
    parser.add_argument('-f', '--from-index', required=True, help='Index to rotate alias to.')
    parser.add_argument('-t', '--to-index', required=True, help='Current index. Will be closed and optimized.')
    args = parser.parse_args()

    logging.info("Connecting to Elasticsearch at %s:%d ..." % (args.server, args.port))
    es = Elasticsearch(hosts=[{'host': args.server, 'port': args.port}])

    from_index = args.from_index
    to_index = args.to_index

    docs = []

    scan_resp = scan(es, index=from_index, query={"query" : {"match_all" : {}}}, scroll='10m')

    for resp in scan_resp:
        doc_type = resp['_type']
        doc = resp['_source']

        docs.append({'_index': to_index, '_type': doc_type, '_source': doc})

        if len(docs) >= 1000:
            resp = bulk_create(es, docs)
            if resp != (1000, []):
                logging.error("Bulk create returned %s" % str(resp))

            docs = []

    if len(docs) > 0:
        resp = bulk_create(es, docs)
        if resp != (len(docs), []):
            logging.error("Bulk create returned %s" % str(resp))
