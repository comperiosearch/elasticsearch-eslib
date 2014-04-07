#!/usr/bin/python3
# -*- coding: utf-8 -*-

# Analyze actors, actions and targets in tweets


import re, json
import eslib.DocumentProcessor


class TweetAnalyzer(eslib.DocumentProcessor):

    def __init__(self, name):
        eslib.DocumentProcessor.__init__(self, name)

        self.actions_file = None
        self.actors_file  = None
        self.targets_file = None
        self.field        = "text"
        self._actors_db   = {}
        self._target_idex = {}


    def configure(self, config=None):
        # Throw exception if mandatory attributes are not configured
        if not self.actions_file:
            raise Exception("Missing actions metadata. Actions file must be specified.")


    def load(self):
        if self.actors_file:
            if self.VERBOSE: self.vout("Loading actors file: %s" % self.actors_file)
            f = open(self.actors_file)
            actors = json.load(f)
            f.close()
            for actor in actors:
                self._actors_db.update({actor["name"].lower(): actor})
        else:
            self.vout("No actor data loaded.")

        if self.targets_file:
            if self.VERBOSE: self.vout("Loading targets file: %s" % self.targets_file)
            f = open(self.targets_file)
            targets = json.load(f)
            f.close()
        else:
            self.vout("No target data loaded.")

        if self.actions_file:
            if self.VERBOSE: self.vout("Loading actions file: %s" % self.actions_file)
            f = open(self.actions_file)
            actions = json.load(f)
            f.close()
        else:
            self.vout("No actions data loaded.")

        self._target_index = {}
        self._populate_index(self._target_index, targets)
        self._action_index = {}
        self._populate_index(self._action_index, actions)

    def _populate_index(self, target_index, source_list):
        for item in source_list:
            words = item["name"].lower().split(" ")
            item.update({"words" : words})
            if words[0] in target_index:
                target_index[words[0]].append(item)
            else:
                target_index[words[0]] = [item]


    def _get_actor_weight(self, actor):
        actor = actor.lower()
        if actor in self._actors_db:
            return float(self._actors_db[actor].get("weight", 0.0))
        return 0.0

    def _get_actors(self, fields):

        author = eslib.getfield(fields, "user.screen_name")
        mentions = [eslib.getfield(mention, "screen_name") for mention in eslib.getfield(fields, "mention", [])]
        retweet = eslib.getfield(fields, "retweet.user_screen_name")
        if retweet and retweet in mentions:
            mentions.remove(retweet)

        score = 0.0
        actors = []

        weight = self._get_actor_weight(author)
        actors.append({"identity": author, "role": "author", "weight": weight})
        # EXTREMELY SIMPLE SCORING FOR NOW:
        score = weight

        if retweet:
            weight = self._get_actor_weight(retweet)
            actors.append({"identity": retweet, "role": "retweet", "weight": weight})

        for actor in mentions:
            weight = self._get_actor_weight(actor)
            actors.append({"identity": actor, "role": "mention", "weight": weight})

        # Update fields with new knowledge
        fields.update({"actors": actors})
        fields.update({"actor_score": score})

        return score


    # Devider = hackish stuff
    def _get_text_stuff(self, fields, text, word_index, node_field, score_field, divider=1.0):
        if not text: return 0.0

        score = 0.0
        targets = []
        weights = []
        # EXTREMELY SIMPLE TOKENIZATION AND TARGET MATCHING:
        text_words = re.split(r'\W', text.lower())
        for i, word in enumerate(text_words):
            if word in word_index:
                #self.dout("CHECKING WORD=[%s]" % word)
                for target in word_index[word]:
                    ok = True
                    for j, tword in enumerate(target["words"]):
                        if i+j >= len(text_words) or not text_words[i+j] == tword:
                            ok = False
                            break
                    if ok:
                        # Target found
                        weight = float(target["weight"])
                        weights.append(weight)
                        targets.append({"name": target["name"], "weight": weight, "location": i})
                        break # ..only take the first one in this naiive implementation
        
        score = self._cramp(sum(weights)/divider)
        
        # Update fields with new knowledge
        fields.update({node_field: targets})
        fields.update({score_field: score})

        return score
        

    def process(self, doc):
        fields  = doc.get("_source")
        text = eslib.getfield(fields, self.field, "")

        if self._actors_db:
            actor_score  = self._get_actors(fields)
        else:
            actor_score = 1.0

        action_score = self._get_text_stuff(fields, text, self._action_index, "actions", "action_score", divider=1.5)
        target_score = self._get_text_stuff(fields, text, self._target_index, "targets", "target_score")

        if self.DEBUG:
            id      = doc.get("_id")
            index   = doc.get("_index")
            doctype = doc.get("_type")
            self.log.debug("/%s/%s/%s: actor=%5.2f, target=%5.2f, action=%5.2f" % \
                (index, doctype, id, actor_score, target_score, action_score))

        # A simple summary score
        score = (action_score * (1 + actor_score)) / 2.0
        fields.update({"score": score})

        yield doc # This must be returned, otherwise the doc is considered to be dumped


    def _cramp(self, value):
        return min(1, max(0, value))


# ============================================================================
# For running as a script
# ============================================================================

import argparse, sys
from eslib.prog import progname


def main():
    help_a = "File containing actions in special JSON format. (see detailed doc)"
    help_A = "File containing actors in special JSON format. (see detailed doc)"
    help_t = "File containing targets in special JSON format. (see detailed doc)"
    help_f = "Field to analyze. Defaults to 'text'."
    epilog = """This script adds complex data to nodes 'actors', 'actions', 'targets' and
        scores to 'actor_score', 'action_score', 'target_score'."""
    
    parser = argparse.ArgumentParser(
        usage="\n  %(prog)s -a actionFile [-t targetsFile] [-A actorsFile] [-f field] [file ...]",
        epilog=epilog)
    parser._actions[0].help = argparse.SUPPRESS
    parser.add_argument("-a", "--actions" , help=help_a, required=True , metavar="file")
    parser.add_argument("-A", "--actors"  , help=help_A, required=False, metavar="file" , default=None)
    parser.add_argument("-t", "--targets" , help=help_t, required=True , metavar="file")
    parser.add_argument("-f", "--field"   , help=help_f, required=False, metavar="field", default="text")
    parser.add_argument(      "--debug"   , help="Display debug info." , action="store_true")
    parser.add_argument("filenames", nargs="*", help="If not specified stdin will be used instead.")

    if len(sys.argv) == 1:
        parser.print_usage()
        sys.exit(0)

    args = parser.parse_args()

    # Set up and run this processor
    dp = TweetAnalyzer(progname())
    dp.actions_file = args.actions
    dp.actors_file  = args.actors 
    dp.targets_file = args.targets
    dp.field        = args.field

    dp.DEBUG = args.debug

    dp.run(args.filenames)


if __name__ == "__main__": main()

