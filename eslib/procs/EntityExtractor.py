__author__ = 'Eivind Eidheim Elseth'

from ..Processor import Processor
from .. import esdoc


class EntityExtractor(Processor):

    def __init__(self, **kwargs):
        super(EntityExtractor, self).__init__(**kwargs)
        m = self.create_connector(self._incoming_esdoc,
                                  "input",
                                  "esdoc",
                                  "Incoming 'esdoc'.",
                                  is_default=True)
#        self.create_connector(self._incoming_str  , "str"  , "str"  , "Incoming document of type 'str' or 'unicode'.")
        self.output_esdoc = self.create_socket("output",
                                               "esdoc",
                                               "Outgoing 'esdoc' with.",
                                               is_default=True,
                                               mimic=m)
#        self.output_str   = self.create_socket("str", "str"     , "Outgoing, cleaned, 'str'.")
        self.config.set_default(
            entities=[],
            source_field="text",
            target_field="entities"
        )
        self._entities = []

    def on_open(self):
        self._entities = self.config.entities

    def _incoming_esdoc(self, doc):
        if self.output_esdoc.has_output:
            self.output_esdoc.send(self._extract(doc))

    def _extract(self, doc):
        source = doc.get("_source")
        text = esdoc.getfield(source, self.config.source_field)
        extracted = self._extract_exact(text)
        return esdoc.shallowputfield(doc,
                                     "_source." + self.config.target_field,
                                     extracted)

    def add_entity(self, entity):
        self._entities.append(entity)

    def add_entities(self, entities):
        self._entities.extend(entities)

    def _extract_exact(self, input_):
        extracted = {}
        for entity in self._entities:
            for match in entity["match"]:
                if match["type"] == "exact":
                    if match["value"] in input_:
                        if not entity["category"] in extracted:
                            extracted[entity["category"]] = []
                        item = {entity["name"]: {match["type"]: [match["value"]]}}
                        extracted[entity["category"]].append(item)

        return extracted
