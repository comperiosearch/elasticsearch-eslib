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

    The result pattern under the 'entities' section of the document:

        <category>      dict            # Name of the category for the entity.
            <name>      list of...      # The name/id of this entity within this category, containing a list of matches...
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
    def __init__(self, **kwargs):
        super(EntityExtractor, self).__init__(**kwargs)
        m = self.create_connector(self._incoming_esdoc, "input", "esdoc", "Incoming 'esdoc'.", is_default=True)
        self.create_connector(self._incoming_str, "str", "str", "Incoming document of type 'str' or 'unicode'.")
        self.output_esdoc = self.create_socket("output", "esdoc", "Outgoing 'esdoc' with identified entities.", is_default=True, mimic=m)
        self.output_entities = self.create_socket("entities", "entities", "The extracted entities only.")

        self.config.set_default(
            entities  = [],
            fields    = [],
            target    = "entities"
        )

        self._regex_email      = re.compile(r"([a-zA-Z0-9_\-\.]+)@([a-zA-Z0-9_\-\.]+)\.([a-zA-Z]{2,5})", re.UNICODE|re.IGNORECASE)
        self._regex_creditcard = re.compile(r"\b(\d{4}[.]\d{4}[.]\d{4}[.]\d{4})\b")


    def _incoming_esdoc(self, doc):
        if self.has_output:
            extracted = []
            for field in self.config.fields:
                text = esdoc.getfield(doc, "_source." + field)
                if text is not None:
                    if not type(text) in [str, unicode]:
                        self.doclog.warning("Configured field '%s' of unsupported type '%s'. Doc id='%s'." % (field, type(text), doc.get("_id")))
                    e = self._extract(field, text)
                    if e:
                        extracted.extend(e)

            # If the 'entities' part already exists, make a deep clone of it and add to it.
            existing = esdoc.getfield(doc, "_source." + self.config.target)
            target = copy.deepcopy(existing) if existing else {}
            # Create a new document by cloning only necessary parts; otherwise use object references.
            merged_doc = esdoc.shallowputfield(doc, "_source." + self.config.target, target)

            entities = self._merge(extracted, target)
            self.output_entities.send(entities)
            self.output_esdoc.send(merged_doc)

    def _incoming_str(self, doc):
        if self.output_str.has_output:
            extracted = []
            text = doc
            if text is not None:
                if not type(text) in [str, unicode]:
                    self.doclog.warning("Document of unsupported type '%s'. Expected 'str' or 'unicode'." % type(text))
                    e = self._extract(None, text)
                    if e:
                        extracted.extend(e)
            entities = self._merge(extracted)

            if extracted:
                self.output_entities.send(extracted) ##entities)


    def _merge(self, extracted, entities=None):
        "Convert a list of extracted entities (generator) to a merged dictionary."

        if entities is None:
            entities = {}
        for e_category, e_name, e_match in extracted:
            category = entities.get(e_category)
            if category is None:
                category = entities[e_category] = {}
            mlist = category.get(e_name)
            if not mlist:
                mlist = category[e_name] = []
            mlist.append(e_match)

        return entities


    def _extract(self, field, text):
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
                if t == "exact":
                    extracted = self._extract_exact(pattern, text)
                elif t == "email":
                    extracted = self._extract_email(text)
                    name = None  # Use extracted element text instead
                elif t == "iprange":
                    pass # TODO
                elif t == "creditcard":
                    extracted = self._extract_creditcard(text)
                    name = None  # Use extracted element text instead
                else:
                    continue  # Unsupported type

                for txt, span, score in extracted:
                    # One item to follow...
                    yield (
                        category,
                        name or txt,
                        {
                            "type"   : t,
                            "pattern": pattern,
                            "value"  : txt,
                            "indices": span,
                            "field"  : field,
                            "score"  : score * weight
                        }
                    )

    def _extract_exact(self, pattern, text):
        "An extremely simple implementation for now.."

        i = text.lower().find(pattern.lower())
        if i >= 0:
            i_to = i + len(pattern)
            yield (
                text[i:i_to],
                (i, i_to),
                1.0  # Match score, disregarding case mismatch
            )

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
