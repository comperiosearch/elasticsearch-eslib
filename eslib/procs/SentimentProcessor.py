#!/usr/bin/python3
# -*- coding: utf-8 -*-

# Find sentiment/mood in documents


import re, json
import eslib.DocumentProcessor


class SentimentProcessor(eslib.DocumentProcessor):

    def __init__(self, name):
        eslib.DocumentProcessor.__init__(self, name)

        self._sentimentMeta = {}
        self.sentimentDescFile = None
        self.fieldList = []
        self.targetField = "sentiment"


    def configure(self, config=None):
        pass # TODO: Throw exception if mandatory attributes are not configured


    def load(self):
        # Load sentiment file. Ok to fail with exception here if file is not found
        f = open(self.sentimentDescFile)
        self._sentimentMeta = json.load(f)
        f.close()


    def process(self, doc):
        fields  = doc.get("_source")

        sentiments = []
        for fieldStr in self.fieldList:
            a = fieldStr.split("^")
            field = a[0]
            weight = 1.0
            if len(a) > 1: weight = float(a[1])
            partialSentiment = self._analyze(self._sentimentMeta, fields.get(field))
            if partialSentiment: # Skip completely neutral fields
                sentiments.append(partialSentiment * weight)
        # Calculate weighted average
        sentiment = 0.0
        if sentiments:
            sentiment = sum(sentiments) / float(len(sentiments))

        if self.DEBUG:
            id      = doc.get("_id")
            index   = doc.get("_index")
            doctype = doc.get("_type")
            self.dout("/%s/%s/%s: %5.2f" % (index, doctype, id, sentiment))

        # Add sentiment to the document
        fields.update({self.targetField : sentiment})

        return doc # This must be returned, otherwise the doc is considered to be dumped


    def _cramp(self, value):
        return min(1, max(-1, value))


    def _analyze(self, sentimentMeta, text, negationWords=["not"]):
        """Sentiment analysis in English or similar style language. The 'sentimentMeta' takes a dict of 'adjectives' and 'strenghts' which are again dictionaries of word:weight, where weight is a float in the range (-1,+1)."""

        if not text: return 0.0

        scores = []
        aa = sentimentMeta["adjectives"]
        ss = sentimentMeta["strengths"]
        words = re.findall(r"[\w']+", text)
        for i in range(len(words)):
            word = words[i].lower()
            if word in aa:
                hit = word
                # Score the adjective
                score = aa[word]
                nptr = i-1
                # Look for strenght and modify score
                if i > 0 and words[i-1] in ss:
                    hit = words[i-1] + " " + hit
                    score = self._cramp(score * (1 + ss[words[i-1]]))
                    nptr -= 1
                # Look up to two words back for negation and negate score
                if (nptr >= 0 and words[nptr] in negationWords) or (nptr > 0 and words[nptr-1] in negationWords):
                    score = -score
                scores.append(score)
                #print >>sys.strerr, "HIT: %s, %5.2f" % (hit, score)

        average = 0.0
        if scores: average = sum(scores) / float(len(scores))
        return average


# ============================================================================
# For running as a script
# ============================================================================

import sys, getopt
from eslib.prog import progname


OUT = sys.stderr


def usage(err = None, rich= False):
    if err:
        print("Argument error: %s" % err, file=OUT)

    p = progname()
    print("Usage:", file=OUT)
    print("  %s -h" % p, file=OUT)
    print("  %s -s <sentimentFile> -f <fieldList> [-v] [-t <targetField>] [<fileNames>]" % p, file=OUT)

    if rich:
        print(file=OUT)
        print("Field names are separated by commas. File names are normal command line arguments.", file=OUT)
        print("If no file name is given then stdin is used instead. Field names can be suffixed", file=OUT)
        print("with ^weight where weight is a floating point number (0,1] to denote field weight.", file=OUT)
        print(file=OUT)
        print("Sentiment description file has the format, by example:", file=OUT)
        print("{ \"strenghts\": {\"very\": 0.5, \"terribly\": 0.9},", file=OUT)
        print("  \"adjectives\": {\"good\": 0.5, \"bad\": -0.5} }", file=OUT)

    if err:
        sys.exit(-1)
    else:
        sys.exit(0)


def main():

    # Default values
    fieldListStr = None
    fieldList = []
    filenames = []
    sentimentDescFile = None
    sentimentMeta = None
    targetField = "sentiment"
    verbose = False
    debug = False

    # Parse command line input
    try:
        optlist, args = getopt.gnu_getopt(sys.argv[1:], ':s:f:t:vh', ["debug"])
    except:
        usage()
    for (o, a) in optlist:
        if   o == "-h": usage(rich=True)
        elif o == "-s": sentimentDescFile = a
        elif o == "-f": fieldListStr = a
        elif o == "-t": targetField = a
        elif o == "-v": verbose = True
        elif o == "--debug": debug = True
    filenames = args

    if not sentimentDescFile: usage("missing sentiment description file")
    
    if fieldListStr:
        fieldList = [x.strip() for x in fieldListStr.split(",")]

    # Set up and run this processor
    dp = SentimentProcessor(progname())
    dp.sentimentDescFile = sentimentDescFile
    dp.fieldList = fieldList
    dp.targetField = targetField

    dp.VERBOSE = verbose
    dp.DEBUG = debug

    dp.run(filenames)


if __name__ == "__main__": main()

