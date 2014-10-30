__author__ = 'Hans Terje Bakke'

# TODO: Verify encoding working, especially when writing to stdout

from ..Processor import Processor
import sys
from ..esdoc import tojson


class FileWriter(Processor):
    """
    Write incoming documents to specified file or standard output.
    Documents of dict type are written as json documents, per line. Other types are written directly with
    their string representation.

    Connectors:
        input      (*)       : Incoming documents to write to file as string or json objects per line.

    Config:
        filename          = None    : If not set then 'stdout' is assumed.
        append            = False   : Whether to append to existing file, rather than overwrite.
    """
    def __init__(self, **kwargs):
        super(FileWriter, self).__init__(**kwargs)
        self.create_connector(self._incoming, "input", None, "Incoming documents to write to file as string or JSON objects per line.")

        self.config.set_default(
            filename = None,
            append   = False
        )

        self._file = None

    def on_open(self):

        if self._file:
            self.log.error("on_open() attempted when _file exists -- should not be possible.")
            return

        if not self.config.filename:
            # Assuming stdout
            self._file = sys.stdout
        else:
            # May raise exception:
            self._file = open(self.config.filename, "a" if self.config.append else "w")

    def on_close(self):
        if self._file and self._file != sys.stdout:
            self._file.close()
        self._file = None

    def _incoming(self, document):
        if document:
            if type(document) is dict:
                print >> self._file, tojson(document)
            else:
                print >> self._file, document
            self._file.flush()
