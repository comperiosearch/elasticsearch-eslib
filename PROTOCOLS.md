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
    HtmlRemover.input (connector)
    HtmlRemover.output (soclet)
    PatternRemover.input (connector)
    PatternRemover.output (socket)

Format

    _index          str
    _type           str
    _id             str
    _version        int
    _timestamp      str
    _source         dict  # Dict of { field : value }


All fields are optional, depending on the case

### esdoc.webpage

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

## urlrequest

Used by

    WebGetter.input (connector)

Format

    url             str  #
    what            str  # Source requesting the url, e.g. "twitter_mon"
    who             str  # Who requested it, e.g. some user id from the source

## csv

Used by

    CsvConverter.input (connector)

Format

```csv
"field","field,"field","..."
```


## graph-edge
The graph-edge protocol is simply a dictionary with three mandatory keys,
that together represents an edge.

Used by
    Neo4jWriter.edge (connector)

Format

    from    str # The property-id of the source node
    type    str # The type of the edge. ("follows", "author", "mention", "quote")
    to      str # The property-id of the receiving node

Note that all fields are mandatory.

## graph-user

The graph-user protocol is a dictionary holding properties.

Used by
    
    Neo4jWriter.user (connector)
    TwitterUserGetter.user (socket)

Format
    
    id              str              
    location        str             #Optional
    description     str             #Optional
    screen_name     str             #Optional
    lang            str             #Optional
    name            str             #Optional
    created_at      date.isoformat()#Optional
    