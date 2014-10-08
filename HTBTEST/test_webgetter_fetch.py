#!/usr/bin/env python

from eslib.procs import WebGetter
from eslib import time
import json

wg = WebGetter()

doc = wg.fetch("http://www.comperio.no", what="script", who="htb", domain_id="comperio")
doc["_source"]["content"] = len(doc["_source"]["content"])

print json.dumps(doc, default=time.json_serializer_isodate, indent=2)
