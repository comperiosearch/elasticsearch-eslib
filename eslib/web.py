# -*- coding: utf-8 -*-

"""
eslib.web
~~~~~~~~~~

Module containing operations against web servers and on web content.
"""


__all__ = ("WebGetter",)


import requests
import eslib, eslib.time
import datetime


class WebGetter(object):
    def __init__(self, max_size=-1, content_types=None):
        self.content_types = content_types or ["text/plain", "text/html", "text/xml", "application/xml"]
        self.max_size = 1024*1024 # 1 MB
        if max_size > 0: self.max_size = max_size

    def get(self, url):
        # Fetch web page
        try:
            res = requests.get(url, verify=False)
            res.raise_for_status
        except:
            msg = "URL failed: %s" % url
            raise IOError(msg)
        if not res.ok:
            msg = "URL not ok, status_code=%s for URL: %s" % (res.status_code, url)
            raise IOError(msg)

        # Verify allowed content type
        content_type = (res.headers.get("content-type") or "").split(";")[0]
        if not content_type in self.content_types:
            msg = "Skipping web page with content type '%s', URL: %s" % (content_type, url)
            raise ValueError(msg)

        # Size check with reported content size
        if self.max_size > 0:
            size = int(res.headers.get("content-length") or -1)
            if size > 0 and size > self.max_size:
                msg = "Skipping too large web page (%s), URL: %s" % (eslib.debug.byteSizeString(size, 2), url)
                raise ValueError(msg)

        # Extract vitals from web result
        id = url # res.url
        encoding = res.encoding
        content = res.text

        # Repeat size check with actual content size
        if self.max_size > 0:
            size = len(content)
            if size > self.max_size:
                msg = "Skipping too large web page (%s), URL: %s" % (eslib.debug.byteSizeString(size, 2), url)
                raise ValueError(msg)

        body = {"content": content, "content_type": content_type, "encoding": encoding}
        return body
