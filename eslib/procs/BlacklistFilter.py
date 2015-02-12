__author__ = 'Hans Terje Bakke'

from ..Processor import Processor
from .. import esdoc
import re


class BlacklistFilter(Processor):
    """
    Only pass through documents that satisfy a whitelist of terms or where certain terms do not occur in a combination
    with blacklisted terms.

    Connectors:
        input      (esdoc)   (default) : Incoming document in 'esdoc' dict format.
        str        (str)               : Incoming document of type 'str' or 'unicode'.
    Sockets:
        output     (esdoc)   (default) : Documents that passed the blacklist filtering, arrived on 'input' connector.
        str        (str)               : Documents that passed the blacklist filtering, arrived on 'str' connector.
        dropped    (*)                 : Documents that did not pass the blacklist filtering.

    Config:
        field               = None     : A field to check. Merged with 'fields'.
        fields              = []       : Fields to check.
        filters             = []       : List of filters of a dict of attributes 'tokens', 'blacklist', 'whitelist',
                                         each containing a list of terms.
                                         If specified, this *replaces* the source_field and target_field pair!
        whitelist           = []       : Global whitelist; list of terms that should force a document to pass.
    """

    def __init__(self, **kwargs):
        super(BlacklistFilter, self).__init__(**kwargs)

        m = self.create_connector(self._incoming_esdoc, "input", "esdoc"  , "Incoming 'esdoc'.", is_default=True)
        self.create_connector(self._incoming_str  , "str"  , "str"    , "Incoming document of type 'str' or 'unicode'.")
        self.output_esdoc   = self.create_socket("output" , "esdoc"   , "Documents that passed the blacklist filtering, 'esdoc'.", is_default=True, mimic=m)
        self.output_str     = self.create_socket("str"    , "str"     , "Documents that passed the blacklist filtering, 'str'.")
        self.output_dropped = self.create_socket("dropped", None      , "Documents that did not pass the blacklist filtering.")

        self.config.set_default(
            field     = None,
            fields    = [],
            filters   = {},
            whitelist = []
        )

        self._fields = []
        self._filters = []  # List of tuples of three elements: (token_regex, blacklist_regex, whitelist_regex)
        self._global_whitelist_regex = None

        self.count_passed  = 0
        self.count_dropped = 0

    def _create_regex(self, terms, name):
        if not terms:
            return None
        try:
            return re.compile(r"\b(%s)\b" % "|".join(terms), re.DOTALL|re.MULTILINE|re.UNICODE|re.IGNORECASE)
        except Exception as e:
            raise Exception("Failed to create a %s regex: %s" % (name, e.message))

    def on_open(self):

        # Create field list
        self._fields = []
        if self.config.field:
            self._fields.append(self.config.field)
        if self.config.fields:
            self._fields.extend(self.config.fields)

        # Create filter regexes
        self._filters = []
        if self.config.filters:
            for filtercfg in self.config.filters:
                tokens    = filtercfg.get("tokens")
                blacklist = filtercfg.get("blacklist")
                whitelist = filtercfg.get("whitelist")
                if tokens and (blacklist or whitelist):
                    token_regex     = self._create_regex(tokens   , "token")
                    blacklist_regex = self._create_regex(blacklist, "blacklist")
                    whitelist_regex = self._create_regex(whitelist, "whitelist")
                    self._filters.append((token_regex, blacklist_regex, whitelist_regex))

        # Create global whitelist regex
        self._global_whitelist_regex = self._create_regex(self.config.whitelist, "global whitelist")

        self.count_passed  = 0
        self.count_dropped = 0

    def _check_text(self, text):
        if not text:
            return True

        blacklist_match = False
        for filter in self._filters:
            token_regex, blacklist_regex, whitelist_regex = filter
            if blacklist_match and not whitelist_regex:
                continue  # Already blacklist match and not whitelisting, so skip the rest for this filter
            if token_regex.search(text):
                # Now we have a hit in a token
                if whitelist_regex and whitelist_regex.search(text):
                    return True  # We must keep documents with a token + whitelist match
                if blacklist_regex and blacklist_regex.search(text):
                    blacklist_match = True
                    break  # Document is not blacklisted, but we must proceed in case we also get a whitelist match

        return not blacklist_match  # I.e. the check should return True if there was no blacklist match

    def _check(self, doc):

        if not doc or not self._filters:
            return True

        # This makes this method work also for 'str' and 'unicode' type documents; not only for the expected 'esdoc' protocol (a 'dict').
        if type(doc) in [str, unicode]:
            if self._global_whitelist_regex and self._global_whitelist_regex.search(doc):
                return True  # Hit in global whitelist
            return self._check_text(doc)
        elif not type(doc) is dict:
            self.doclog.debug("Unsupported document type '%s'." % type(doc))
            return True  # So this silly document will pass through unfiltered...

        source = doc.get("_source")
        if not source:
            return True  # Missing source section; don't do anything and let it pass through unfiltered...

        for field in self._fields:
            text = esdoc.getfield(source, field)
            if text and type(text) in [str, unicode]:
                if self._global_whitelist_regex and self._global_whitelist_regex.search(text):
                    return True  # Hit in global whitelist
                if not self._check_text(text):
                    return False  # A hit in the combination of terms and blacklisted terms
        return True  # The document passed

    def _incoming_esdoc(self, doc):
        if self.output_esdoc.has_output or self.output_dropped.has_output:
            if (not self._fields or not self._filters) or self._check(doc):
                self.count_passed  += 1
                self.output_esdoc.send(doc)
            else:
                self.count_dropped += 1
                self.output_dropped.send(doc)

    def _incoming_str(self, doc):
        if self.output_str.has_output or self.output_dropped.has_output:
            if (not self._filters) or self._check(doc):
                self.count_passed  += 1
                self.output_str.send(doc)
            else:
                self.count_dropped += 1
                self.output_dropped.send(doc)
