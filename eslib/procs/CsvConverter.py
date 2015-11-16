__author__ = 'Hans Terje Bakke'

import csv, codecs
from ..Processor import Processor

class CsvConverter(Processor):
    """
    Convert csv input to Elasticsearch document format.
    Field names can be explicitly entered or derived from the first line of input,
    assuming that is the first line contains column names. When explicitly specified, only those columns entered
    will be used, the others will be ignored. When derived, all columns are used.

    NOTE: Fields, including column headers, must not have any spacing between delimiters and quotes.

    NOTE: Fields that are mapped to meta fields ('_id', '_index', '_type') will not be part of the '_source'.

    Connectors:
        input      (csv)     : Document in 'csv' format. First document is optionally column list.
    Sockets:
        output     (esdoc)   : Documents converted from 'csv' to 'esdoc' format.

    Config:
        index             = None     : Override '_index' meta field with this value.
        doctype           = None     : Override '_type' meta field with this value.
        columns           = None     : List of columns to pick from the CSV input. Use None for columns to ignore.
        skip_first_line   = False    : Skip first line of the input. (Typically column headers you don't want.
        delimiter         = ","      : CSV column delimiter character.

        id_field          = "_id"    : Name of field to map to meta field '_id'.
        index_field       = "_index" : Name of field to map to meta field '_index'.
        type_field        = "_type"  : Name of field to map to meta field '_type'.
    """

    def __init__(self, **kwargs):
        super(CsvConverter, self).__init__(**kwargs)
        self.create_connector(self._incoming, "input", "csv", "Document in 'csv' format. First document is optionally column list.")
        self.output = self.create_socket("output", "esdoc", "Documents converted from 'csv' to 'esdoc' format.")

        self.config.set_default(
            index           = None,
            doctype         = None,
            columns         = None,
            skip_first_line = False,
            delimiter       = ",",

            id_field        = "_id",
            index_field     = "_index",
            type_field      = "_type"
        )

        self._columns = []
        self._first_line_processed = False


    def on_open(self):
        # Sanity check:
        if self.config.skip_first_line and not self.config.columns:
            raise Exception("Nothing specified in 'columns' and 'skip_first_line' set. Unable to determine fields to include, then.")

        self._first_line_processed = False
        self._columns = self.config.columns or []

    def _incoming(self, line):
        # Check if we should skip first line or use it as column definitions (columns)
        if not self._first_line_processed:
            self._first_line_processed = True
            if self.config.skip_first_line:
                return
            if not self._columns:
                # No skipping first line ordered and no field list. Now assume first line to be column headings
                for csvrow in csv.reader([line], delimiter=self.config.delimiter):
                    self._columns = csvrow
                    return

        # Pick the only line. Since csv does not support unicode, we do this little encoding massage:
        raw_line = codecs.encode(line, "UTF-8")
        raw_csvrow = csv.reader([raw_line], delimiter=self.config.delimiter).next()
        csvrow = [codecs.decode(x, "UTF-8") for x in raw_csvrow]

        if not len(self._columns) == len(csvrow):
            self.doclog.warning("Column count does not match number of fields. Aborting. Row =\n%s" % csvrow)
            self.abort()  # NOTE: We might want to continue processing, or we might not...

        doc = {}
        id = None
        index = None
        doctype = None
        for i in range(len(self._columns)):
            if not self._columns[i]:
                continue # Skip non-specified fields
            elif self._columns[i] == self.config.id_field:
                id = csvrow[i]
            elif self._columns[i] == self.config.index_field: # Override index
                index = csvrow[i]
            elif self._columns[i] == self.config.type_field: # Override doctype
                doctype = csvrow[i]
            else:
                doc.update({self._columns[i]: csvrow[i]})

        # Convert to Elasticsearch type document
        esdoc = {"_index":self.config.index or index, "_type":self.config.doctype or doctype, "_id":id, "_source":doc}

        self.output.send(esdoc)
