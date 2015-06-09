# -*- coding: utf-8 -*-

"""
eslib.web
~~~~~~~~~~

Module containing operations against web servers and on web content.
"""


__all__ = ("WebGetter", "detect_language", "remove_boilerplate")


import requests
import eslib
from collections import Counter
from textblob import TextBlob
import justext
from datetime import datetime, timedelta
from email.utils import parsedate_tz, mktime_tz

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

        # Find timestamp
        date_str = res.headers.get("date")
        if not date_str:
            timestamp = datetime.utcnow()
        else:
            t = mktime_tz(parsedate_tz(date_str))
            timestamp = datetime(1970, 1, 1) + timedelta(seconds=t)

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

        body = {"content": content, "content_type": content_type, "encoding": encoding, "date": timestamp}
        return body

#region Language detection

def detect_language(text, chunk_size=250, max_chunks=5):
    """
    Detects language of the passed text. Returns majority detection on multiple chunks in order to avoid
    misclassification on text with boilerplate text of another language in the beginning of the string.

    Uses Google Translate REST API through the TextBlob library.

    :param text: str
    :param chunk_size: int Number of characters in each detection chunk.
    :param max_chunks: int Maximum number of chunks to run detection on.
    :return: str Google Translate language code.
    """
    n_chunks = int(max(min(len(text) / chunk_size, max_chunks), 1))
    detections = []

    for c in xrange(n_chunks):
        l = c * chunk_size
        u = max((c + 1) * chunk_size, len(text))

        chunk = text[l:u]
        detections.append(TextBlob(chunk).detect_language())

    counts = Counter(detections)

    return counts.most_common(n=1)[0][0]

#endregion Language detection

# #region Boilerplate removal

# Map of correspondences between Google Translate and internal JusText
# language codes
GTRANS_JUSTEXT_LANG_MAP = {
    u'af': u'Afrikaans',
    u'sq': u'Albanian',
    u'ar': u'Arabic',
    u'az': u'Azerbaijani',
    u'eu': u'Basque',
    u'be': u'Belarusian',
    u'bg': u'Bulgarian',
    u'ca': u'Catalan',
    u'hr': u'Croatian',
    u'cz': u'Czech',
    u'da': u'Danish',
    u'nl': u'Dutch',
    u'en': u'English',
    u'eo': u'Esperanto',
    u'et': u'Estonian',
    u'fi': u'Finnish',
    u'fr': u'French',
    u'gl': u'Galician',
    u'ka': u'Georgian',
    u'de': u'German',
    u'el': u'Greek',
    u'gu': u'Gujarati',
    u'ht': u'Haitian',
    u'iw': u'Hebrew',
    u'hi': u'Hindi',
    u'hu': u'Hungarian',
    u'is': u'Icelandic',
    u'id': u'Indonesian',
    u'ga': u'Irish',
    u'it': u'Italian',
    u'kn': u'Kannada',
    u'ko': u'Korean',
    u'la': u'Latin',
    u'lv': u'Latvian',
    u'lt': u'Lithuanian',
    u'mk': u'Macedonian',
    u'ms': u'Malay',
    u'mt': u'Maltese',
    u'no': u'Norwegian_Bokmal',
    u'fa': u'Persian',
    u'pl': u'Polish',
    u'pt': u'Portuguese',
    u'ro': u'Romanian',
    u'ru': u'Russian',
    u'sr': u'Serbian',
    u'sk': u'Slovak',
    u'sl': u'Slovenian',
    u'es': u'Spanish',
    u'sw': u'Swahili',
    u'sv': u'Swedish',
    u'tl': u'Tagalog',
    u'ta': u'Tamil',
    u'te': u'Telugu',
    u'tr': u'Turkish',
    u'uk': u'Ukrainian',
    u'ur': u'Urdu',
    u'vi': u'Vietnamese',
    u'cy': u'Welsh'}

def remove_boilerplate(page_str, lang, relaxed=False):
    """
    Removes boilerplate from HTML documents.

    Uses JusText library.

    NOTE: quality dependent on correct language detection.

    :param page_str: str HTML page source.
    :param lang: str Google Translate language code.
    :param relaxed: boolean If True the span between the first and last good/near-good boilerplate match
        is returned. Short and bad segments in between are kept.
    :return: list List of non-boilerplate segments/paragraphs.
    """
    if lang not in GTRANS_JUSTEXT_LANG_MAP:
        #raise AttributeError("Can not remove boilerplate for language code lang='%s'." % lang)
        return []

    jt_lang = GTRANS_JUSTEXT_LANG_MAP[lang]

    paragraphs = justext.justext(page_str, justext.get_stoplist(jt_lang))

    if relaxed:
        good_indexes = [paragraphs.index(p) for p in paragraphs if p.class_type in ['near-good', 'good']]

        if len(good_indexes) == 0:
            return []

        return [paragraph.text for paragraph in paragraphs[min(good_indexes):max(good_indexes) + 1]]
    else:
        return [paragraph.text for paragraph in paragraphs if paragraph.class_type in ['near-good', 'good', 'short']]

#endregion Boilerplate removal
