__author__ = 'Hans Terje Bakke'

from ..Processor import Processor
from .. import esdoc
from eslib.text import remove_html

class HtmlRemover(Processor):
    """
    Remove HTML tags and unescape HTML escapings.

    Connectors:
        input      (esdoc)   (default) : Incoming document in 'esdoc' dict format.
        str        (str)               : Incoming document of type 'str' or 'unicode'.
    Sockets:
        output     (esdoc)   (default) : Output of documents that arrived on 'input' connector.
        str        (str)               : Output of documents that arrived on 'str' connector.

    Config:
        source_field        = "text"   : Part of twitter dev credentials.
        target_field        = None     : Defaults to 'source_field', replacing the input field.
        field_map           = {}       : A dict of fields to use as { source : target }.
                                         If specified, this *replaces* the source_field and target_field pair!
        strip               = True     : Remove boundary spaces and double spaces, commonly left after a removal.
    """

    def __init__(self, **kwargs):
        super(HtmlRemover, self).__init__(**kwargs)

        m = self.create_connector(self._incoming_esdoc, "input", "esdoc", "Incoming 'esdoc'.", is_default=True)
        self.create_connector(self._incoming_str  , "str"  , "str"  , "Incoming document of type 'str' or 'unicode'.")
        self.output_esdoc = self.create_socket("output" , "esdoc"   , "Outgoing, cleaned, 'esdoc'.", is_default=True, mimic=m)
        self.output_str   = self.create_socket("str"    , "str"     , "Outgoing, cleaned, 'str'.")

        self.config.set_default(
            source_field    = "text",
            target_field    = None,
            field_map       = {},
            strip           = True
        )

        self._regexes = []
        self._field_map = {}

    def on_open(self):
        # Create field map
        self._field_map = self.config.field_map or {}
        if not self._field_map:
            if not self.config.source_field:
                raise ValueError("Neither field_map nor source_field is configured.")
            self._field_map[self.config.source_field] = (self.config.target_field or self.config.source_field)


    def _clean_text(self, text):
        text = remove_html(text)
        if self.config.strip:
            text = text.strip().replace("  ", " ")
        return text

    def _clean(self, doc):

        if not doc:
            return doc

        # This makes this method work also for 'str' and 'unicode' type documents; not only for the expected 'esdoc' protocol (a 'dict').
        if type(doc) in [str, unicode]:
            cleaned = self._clean_text(doc)
            return cleaned
        elif not type(doc) is dict:
            self.doclog.debug("Unsupported document type '%s'." % type(doc))
            return doc

        source = doc.get("_source")
        if not source:
            return doc  # Missing source section; don't do anything

        for source_field, target_field in self._field_map.iteritems():
            text = esdoc.getfield(source, source_field)
            if text and type(text) in [str, unicode]:
                cleaned = self._clean_text(text)
                if cleaned != text:
                    # Note: This may lead to a few strictly unnecessary shallow clonings...
                    doc = esdoc.shallowputfield(doc, "_source." + target_field, cleaned)
        return doc

    def _incoming_esdoc(self, doc):
        if self.output_esdoc.has_output:
            self.output_esdoc.send(self._clean(doc))

    def _incoming_str(self, doc):
        if self.output_str.has_output:
            self.output_str.send(self._clean(doc))
