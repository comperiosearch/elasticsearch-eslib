# ============================================================================
# Base class for processing Elasticsearch documents
# ============================================================================

import json
from .PipelineStage import PipelineStage


class DocumentProcessor(PipelineStage):
    "Base class for pipeline stage working on documents Elasticsearch documents."


    def __init__(self, name):
        PipelineStage.__init__(self, name)


    def doclog(self, doc, message):
        """
        :type doc: dict
        :type message: str
        """
        id      = doc.get("_id")
        index   = doc.get("_index")
        doctype = doc.get("_type")
        self._doclog.debug("/%s/%s/%s: %s" % (index, doctype, id, message))


    def write(self, doc):
        if not self.terminal:
            text = json.dumps(doc, ensure_ascii=False)
            super(DocumentProcessor, self).write(text)

  
    def convert(self, line):
        # Convert line to json. TODO: Verify Elasticsearch format.
        # NOTE: This is very simple, requiring one JSON document per line.
        # In order to use a more advanced parsing, override read() instead.
        line = line.strip()
        if not line or line.startswith("#"):
            return None
        try:
            jobj = json.loads(line)
            return jobj
        except ValueError as e:
            self.error("JSON decode error in line below: %s\n%s" % (e.args[0], line), e)
            return None 

