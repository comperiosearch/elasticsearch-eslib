__author__ = 'Mats Julian Olsen'

from ..Processor import Processor
from .. import esdoc
from .. import time


class DateExpander(Processor):
    """
    This processor will use a date field in an esdoc as a basis for constructing
    an object with

        year
        month        (1 through 12)
        day          (1 through 31)
        hour         (0 through 23)
        minute       (0 through 59)
        second       (0 through 59)
        weekday      (1 through 7)
        week         (1 through 53)

    Connectors:
        input       (esdoc)   : Incoming.
    Sockets:
        output      (esdoc)   : Outgoing, with configured date field expanded.

    Config:
        source_field      = "created_at"   : Field which date value to expand.
        target_field      = "date_fields"  : Target field for the expanded object.
    """
    def __init__(self, **kwargs):
        super(DateExpander, self).__init__(**kwargs)
        self._input = self.create_connector(self._incoming, 'input', 'esdoc', "Incoming.")
        self._output = self.create_socket('output', 'esdoc', "Outgoing, with configured date field expanded.")

        self.config.set_default(
            source_field='created_at',
            target_field='date_fields'
        )

    def _incoming(self, doc):
        if self._output.has_output:
            self._output.send(self._process(doc))

    def _process(self, doc):
        value = esdoc.getfield(doc, "_source." + self.config.source_field)
        if value is None:
            self.doclog.warning(
                "Document '%s' is missing field or value in '%s'."
                % (doc.get("_id"), self.config.source_field))
            return doc

        date = time.utcdate(value)
        if date is None:
            self.doclog.warning(
                "Document '%s' has non-date value in field '%s'."
                % (doc.get("_id"), self.config.source_field))
            return doc

        date_dict = time.date_dict(date)
        if date_dict is None:
            # This should not be possible, therefore logging to proclog
            self.log.error("Date field extraction failed for date: %s" % date)
            return doc

        # Create a new document (if necessary) with just the minimum cloning necessary,
        # leaving references to the rest.
        return esdoc.shallowputfield(doc, '_source.' + self.config.target_field, date_dict)
