elasticsearch-eslib
===================

Python library for document processing for elasticsearch.

*** WORK IN PROGESS ***

## INTRO

A 'processor' processes incoming data and generates output.

It can also generate its own data or fetch from external data sources and services.
Instead, or in addition to, writing output to its own 'sockets', it can also write data
to external data sources and services. In these cases it is commonly referred to as a 'generator',
and has its own execution thread.

A processor has one or more input 'connectors' that can connect to one more output 'sockets'.
Connectors and sockets (commonly called 'terminals') are registered with an optional 'protocol' tag.
If it exists, an attempted connection will check if the data protocol is the same in both connector
and socket.

A processor B is said to 'subscribe' to output from processor A if it has a connector connected a
socket on A. In this case, A has the role of 'producer' (to B) and B has the role of 'subscriber' (to A).

## USAGE

From a Python script, we can create a processing graph as in this example:

    a = ElasticsearchReader()
    b = ElasticsearchWriter()
    a.config.index = "employees"
    b.config.index = "employees_copy"
    b.subscribe(a)

and execute it with

    a.start()

In this simple example, first processor is a generator, and the entire pipeline will finish when 'a'
completes. The simple "b.subscribe(a)" is possible because there is only one connector in 'b' and only
one socket in 'a'. Otherwise, we would have to specify the connector and socket names.

By default, a processor that is stopped either explicitly from outside, or completes generating data (as
in this example), will send a stop signal to its subscribers. This is not always a desirable behaviour.
Say we had 20 readers sending data to 1 writer. We would not like the writer to stop when the first reader
completes. To avoid this, we can use

    ...
    b.keepalive = True
    a.start()
    time.sleep(10)  # The reader is still working in its own thread
    b.put(mydoc)    # Writes to the only connector ("input", of protocol "esdoc")
    a.wait()        # Wait for a to complete/stop
    b.stop()        # ... then explicitly stop b

One processor/connector can subscribe to data from many processors/sockets. One processor can have many
different named connectors, expecting data in various formats (hinted by its 'protocol' tag.) And a processor/socket
can have many processors/connectors subscribing to data it outputs.

## BEHIND THE SCENE

Technically, a processor sends document data to its sockets. The sockets send documents to its connected connectors.
A connector has a queue of incoming items, and a thread that pulls documents off the queue and sends it to
a processing method in the processor class. This method processes the data and sends the result to one or
more of its sockets, which again send to connected connectors...

A generator style processor has another thread that generates documents somehow, and sends it to its socket(s).


## MEMBERS FOR USING THE Processor (and derivates)

    Read/write:
        keepalive
    Read only:
        accepting
        stopping
        running
        suspended
        aborted
    Methods:
        __init__(name) # Constructor/init
        subscribe(producer=None, socket_name=None, connector_name=None)
        unsubscribe(producer=None, socket_name=None, connector_name=None)
        attach(subscriber, socket_name=None, connector_name=None)
        detach(subscriber, socket_name=None, connector_name=None)
        connector_info(*args)  # returns list
        socket_info(*args)     # returns list
        start()
        stop()
        abort()
        suspend()
        resume()
        wait()
        put(document, connector_name=None)
        add_callback(method, socket_name=None)
    Methods for debugging:
        DUMP
        DUMP_connectors
        DUMP_sockets
        
        