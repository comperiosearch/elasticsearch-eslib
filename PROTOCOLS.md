# Protocols

This document describes the common protocols for document exchange between terminals (connectors and sockets).

The name of the protocol is meant as a hint, although keeping track of a common set of protocols would be good.

## esdoc

### esdoc (general)

Used by

    ElasticsearchReader.output (socket)
    ElasticsearchWriter.input (connector)
    ElasticsearchWriter.output (socket)
    CsvConverter.output (socket)

Format

    _index          str
    _type           str
    _id             str
    _version        int
    _timestamp      str
    _source         dict  # Dict of { field : value }


All fields are optional, depending on the case


## urlrequest

Used by

    WebGetter.input (connector)

Format

    url             str  #
    what            str  # Source requesting the url, e.g. "twitter_mon"
    who             str  # Who requested it, e.g. some user id from the source

## webpage

Used by

    WebGetter.output (socket)
    
Format

    _id                str   # Using the URL as ID
    _timestamp         str   # When the content was fetched
    _source            dict of ...
        domain         str
        requested_by   list  # Of of dicts of format [ what : [ who, ...] }, ... ]
        content        str
        content_type   str
        encoding       str

## csv

Used by

    CsvConverter.input (connector)

Format

```csv
"field","field,"field","..."
```
