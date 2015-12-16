__author__ = 'Hans Terje Bakke'

from ..Processor import Processor
from .. import esdoc
import re, copy


class EntityExtractor(Processor):
    """
    Extract configured values and add it to the document's 'entities' section. (Configurable.)

    Entity extraction config contains a list of elements following this format:

        category       str          # Name of category.
        name           str          # Name of entity within this category
        match          list of...
            type       str          # Match type (e.g. email, exact, iprange, etc.)
            pattern    str          # Pattern to use for finding a match. (Not always applicable.)
            weight     float        # How much weight to apply to this match. Will be multiplied with matching algorithm
                                      score to determine the final entity score.
            weights    dict {str: float}  # Additional weights per language (as key) (ISO 639-1 alpha-2, ISO 639-3 alpha-3)

    The result pattern under the 'entities' section of the document:

        <category>      list of dict of... # Name of the category for the entity.
            name        str         # The name/id of this entity within this category.
            type        str         # Match type (e.g. email, exact, iprange, etc.)
            pattern     str         # Pattern that was used to find the hit, e.g. the 'iprange' pattern.
            hit         str         # String that is identified as matching the match criteria.
            span        list[2]     # (start, exclusive end) position of the hit.
            field       str         # Which field the hit was found in.
            score       float       # The quality of the hit (multiplied with the optional weight specified!)

    Connectors:
        input      (esdoc)     (default)  : Incoming document in 'esdoc' dict format.
        str        (str)                  : Incoming document of type 'str' or 'unicode'.
    Sockets:
        entities   (entities)             : Output of the extracted entities per document.
        output     (esdoc)     (default)  : Output of documents that arrived on 'input' connector.
                                            Not used if input was on 'str' connector.

    Config:
        fields              = []          : Which fields to do entity extraction on.
        target              = "entities"  : Which section ("field") to write the extracted entity information to.
        entities            = {}          : The entities to look for and how. See format above.
    """

    _regex_email      = re.compile(r"\b([a-zA-Z0-9_\-\.]+)@([a-zA-Z0-9_\-\.]+)\.([a-zA-Z]{2,5})\b", re.UNICODE|re.IGNORECASE)
    _regex_creditcard = re.compile(r"\b(\d{4}[. ]\d{4}[. ]\d{4}[. ]\d{4})\b")
    _regex_ipaddr     = re.compile(r"\b(\d{4}[.]\d{4}[.]\d{4}[.]\d{4})\b")
    _regex_exact_format = r"\b(%s)\b"
    _regex_exact_flags  = re.DOTALL|re.IGNORECASE|re.UNICODE|re.MULTILINE
    _regex_ipaddr = re.compile(r"\b" +
        r"(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\." +
        r"(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\." +
        r"(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\." +
        r"(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)" +
        r"(?:[:](\d+))?" +
        r"\b")

    def __init__(self, **kwargs):
        super(EntityExtractor, self).__init__(**kwargs)
        m = self.create_connector(self._incoming_esdoc, "input", "esdoc", "Incoming 'esdoc'.", is_default=True)
        self.create_connector(self._incoming_str, "str", "str", "Incoming document of type 'str' or 'unicode'.")
        self.output_esdoc = self.create_socket("output", "esdoc", "Outgoing 'esdoc' with identified entities.", is_default=True, mimic=m)
        self.output_entities = self.create_socket("entities", "entities", "The extracted entities only.")

        self.config.set_default(
            entities       = [],
            fields         = [],
            language_field = None,
            target         = "entities"
        )

        self._regex_exact = {}

    def on_open(self):
        # Create a dictionary of all regexes for exact matching
        for conf in self.config.entities or []:
            for match in conf.get("match") or []:
                t = match.get("type")
                if t == "exact":
                    pattern = match.get("pattern")
                    if pattern and not pattern in self._regex_exact:
                        self._regex_exact[pattern] =\
                            re.compile(
                                self._regex_exact_format % pattern.replace("*", ".*?"),  # Non greedy
                                flags=self._regex_exact_flags
                            )

    def _incoming_esdoc(self, doc):
        if self.has_output:
            lang = esdoc.getfield(doc, "_source." + self.config.language_field) if self.config.language_field else None

            extracted = []
            for field in self.config.fields:
                text = esdoc.getfield(doc, "_source." + field)
                if text is not None:
                    if not isinstance(text, basestring):
                        self.doclog.warning("Configured field '%s' of unsupported type '%s'. Doc id='%s'." % (field, type(text), doc.get("_id")))
                    ee = self._extract(field, text, lang)
                    for e in ee:
                        extracted.append(e)

            # If the 'entities' part already exists, make a deep clone of it and add to it.
            existing = esdoc.getfield(doc, "_source." + self.config.target)
            target = copy.deepcopy(existing) if existing else {}
            # Create a new document by cloning only necessary parts; otherwise use object references.
            merged_doc = esdoc.shallowputfield(doc, "_source." + self.config.target, target)

            entities = self._merge(extracted, target)
            if extracted:
                self.output_entities.send(extracted) ##entities)
            self.output_esdoc.send(merged_doc)

    def _incoming_str(self, doc):
        if self.output_str.has_output:
            extracted = []
            text = doc
            if text is not None:
                if not type(text) in [str, unicode]:
                    self.doclog.warning("Document of unsupported type '%s'. Expected 'str' or 'unicode'." % type(text))
                    ee = self._extract(None, text)
                    for e in ee:
                        extracted.extend(e)
            entities = self._merge(extracted)

            if extracted:
                self.output_entities.send(extracted) ##entities)


    def _merge(self, extracted, entities=None):
        "Convert a list of extracted entities (generator) to a merged dictionary."

        if entities is None:
            entities = {}
        for e_category, e_match in extracted:
            category = entities.get(e_category)
            if category is None:
                category = entities[e_category] = []
            category.append(e_match)

        return entities


    def _extract(self, field, text, lang=None):
        """
        Extract as per entity extraction config from 'text'. 'field' is the name of the field containing the text, or None.
        Return type is a tuple of (category, name, match), where match is a dict.
        """

        for conf in self.config.entities or []:
            category = conf.get("category")
            entity_name = conf.get("name")
            for match in conf.get("match") or []:
                name = entity_name
                t = match.get("type")
                pattern = match.get("pattern")
                weight = match.get("weight")
                if weight is None:
                    weight = 1.0
                language_weights = match.get("weights") or {}
                language_weight = 1.0

                if lang in language_weights:
                    language_weight=language_weights[lang]
                elif "*" in language_weights:
                    language_weight = language_weights["*"]
                # Skip 0-weights
                if language_weights == 0.0 or weight == 0.0:
                    continue

                if t == "exact":
                    extracted = self._extract_exact(pattern, text)
                elif t == "email":
                    extracted = self._extract_email(text)
                    name = None  # Use extracted element text instead
                elif t == "iprange":
                    extracted = self._extract_iprange(pattern, text)
                    name = None  # Use extracted element text instead
                elif t == "creditcard":
                    extracted = self._extract_creditcard(text)
                    name = None  # Use extracted element text instead
                else:
                    continue  # Unsupported type

                for txt, span, score in extracted:
                    # One item to follow...
                    yield (
                        category,
                        {
                            "name"   : name or txt,
                            "type"   : t,
                            "pattern": pattern,
                            "value"  : txt,
                            "indices": span,
                            "field"  : field,
                            "score"  : score * weight * language_weight
                        }
                    )

    def _extract_exact(self, pattern, text):
        "An extremely simple implementation for now.."

        for match in self._regex_exact[pattern].finditer(text):
            yield (
                match.group(),
                match.span(),
                1.0,  # Score
            )

        # i = text.lower().find(pattern.lower())
        # if i >= 0:
        #     i_to = i + len(pattern)
        #     yield (
        #         text[i:i_to],
        #         (i, i_to),
        #         1.0  # Match score, disregarding case mismatch
        #     )

    def _extract_email(self, text):

        for match in self._regex_email.finditer(text):
            yield (
                match.group(),
                match.span(),
                1.0,  # Score
            )

    def _extract_creditcard(self, text):

        for match in self._regex_creditcard.finditer(text):
            yield (
                match.group(),
                match.span(),
                1.0,  # Score
            )

    def _extract_iprange(self, pattern, text):
        "Very simple IP version 4 parser."
        for match in self._regex_ipaddr.finditer(text):
            yield (
                match.group(),
                match.span(),
                1.0,  # Score
            )
