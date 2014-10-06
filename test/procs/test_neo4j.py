__author__ = 'mats'

from eslib.procs import Neo4jWriter
from eslib.procs import Neo4jReader


def main():
    neo = Neo4jWriter(host="nets.comperio.no", port="7474")
    edge = {
        "to": "2320059392",
        "type": "mentioned",
        "from": "23200593123"
    }
    user = {"id": "23200593123", "hero": "SUPERMAN", "level": 1, "awesome": "yes"}
    neo.start()
    # neo.put(edge, "edge")
    # neo.put(user, "user")

    reader = Neo4jReader(host="nets.comperio.no", port="7474", batchsize=2)
    ids = ["41234","541198739", "12341", "198451236"]
    reader.start()
    for uid in ids:
        reader.put(uid)

    reader.add_callback(printer, "ids")
    try:
        neo.wait()
        reader.wait()
    except KeyboardInterrupt:
        neo.stop()
        reader.stop()


def printer(doc):
    print doc

if __name__ == '__main__':
    main()
