__author__ = 'mats'

import logging
from eslib.procs import ElasticsearchReader, ElasticsearchWriter, DateFields

LOG_FORMAT = ('%(name) -8s %(levelname) -10s %(funcName) -30s %(lineno) -5d: %(message)s')
#logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)

console = logging.StreamHandler()
console.setLevel(logging.TRACE)
console.setFormatter(logging.Formatter("%(firstname) -8s %(lineno) -5d %(levelname) -10s %(message)s"))

proclog = logging.getLogger("proclog")
proclog.setLevel(logging.TRACE)
proclog.addHandler(console)

doclog  = logging.getLogger("doclog")
doclog.setLevel(logging.TRACE)
doclog.addHandler(console)

count = 0
def printer(doc):
    global count
    count += 1
    if not count % 1000:
        print doc

def main():
    # file_reader = FileReader(filenames=['resources/tweet.json'])
    es_reader = ElasticsearchReader(
        hosts=['es.nets.comperio.no:9200'],
        index='raw_twitter_all',
        doctype='tweets',
        size=100
        )
    es_writer = ElasticsearchWriter(
        hosts=['es.nets.comperio.no:9200'],
        index='mats',
        doctype='tweet',
        batchsize=1000
    )
    date_fields = DateFields()

    date_fields.subscribe(es_reader)
    es_writer.add_callback(printer)
    es_writer.subscribe(date_fields)

    es_reader.start()

    # date_fields.put({'_source': {'created_at': '-120-13-142T25:61:61+30:00'}})
    try:
        es_reader.wait()
    except KeyboardInterrupt:
        es_reader.stop()
        es_writer.wait()



if __name__ == '__main__':
    main()