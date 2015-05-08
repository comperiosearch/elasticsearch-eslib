__author__ = 'Hans Terje Bakke'

#TODO: Bulk check Elasticsearch for existing items
#TODO: Batch up channel updates for writing back to Elasticsearch after a fetch(?)
#TODO: Convert call to Elasticsearch to get item count per channel to ONE aggregation call instead?
#TODO: Elasticsearch mappings use 'facet' analyzer
#TODO: Find a way to filter out duplicates (e.g. search for all item ideas in the channel batch and drop those we found)
#TODO:    This is needed especially for channels that do not have "updated" info. (e.g. digi.no)
#TODO: Make fetch items not be a generator

#region Elasticsearch mappings

es_default_settings = {
    "analysis": {
        "analyzer": {
            "facet" : {
                "type" : "custom",
                "tokenizer" : "keyword",
                "filter" : [ "lowercase" ]
            }
        }
    }
}

es_default_mapping_channel = {
    "_timestamp": {"enabled": "true", "store": "yes"},
    "properties" : {
        "name"         : {"type": "string"   , "index": "not_analyzed", "include_in_all": False},
        "title"        : {"type": "string"   , "index": "analyzed"    , "include_in_all": True},
        "url"          : {"type": "string"   , "index": "not_analyzed", "include_in_all": False},
        "version"      : {"type": "string"   , "index": "not_analyzed", "include_in_all": False},
        "updated"      : {"type": "date"     , "index": "analyzed"    , "include_in_all": False,
                          "format": "dateOptionalTime", "store": "yes"},
        "generator"    : {"type": "string"   , "index": "analyzed"    , "include_in_all": False},
        "language"     : {"type": "string"   , "index": "not_analyzed", "include_in_all": False},
        "description"  : {"type": "string"   , "index": "analyzed"    , "include_in_all": True},

        "lastFetch"    : {"type": "date"     , "index": "analyzed"    , "include_in_all": False,
                          "format": "dateOptionalTime", "store": "yes"}
    }
}

es_default_mapping_item = {
    "_timestamp": {"enabled": "true", "store": "yes"},
    "properties" : {
        "channel"      : {"type": "string"   , "index": "not_analyzed", "include_in_all": False},
        "title"        : {"type": "string"   , "index": "analyzed"    , "include_in_all": True},
        "link"         : {"type": "string"   , "index": "no"          , "include_in_all": False},
        "updated"      : {"type": "date"     , "index": "analyzed"    , "include_in_all": False,
                          "format": "dateOptionalTime", "store": "yes"},
        "author"       : {"type": "string"   , "index": "not_analyzed", "include_in_all": False},
        "description"  : {"type": "string"   , "index": "analyzed"    , "include_in_all": True},
        "comments"     : {"type": "string"   , "index": "no"          , "include_in_all": False},
        "categories"   : {"type": "string"   , "index": "not_analyzed", "include_in_all": False},
        "location"     : {"type": "geo_point", "index": "analyzed"    , "include_in_all": False},
        "page"         : {"type": "string"  , "index": "analyzed"     , "include_in_all": True}
    }
}

#endregion Elasticsearch mappings

from ..Monitor import Monitor
from ..time import date2iso, iso2date
from datetime import datetime
import requests, feedparser
import uuid, time, logging
import elasticsearch, elasticsearch.exceptions

class RssMonitor(Monitor):
    """
    Fetch items from RSS feeds ("channels") at regular intervals.

    Note: The reason we use regular intervals to check is that the channel publish time info cannot always be trusted.

    Sockets:
        item                 (esdoc.rss)    : RSS item with linked-to web page added (if so configured).

    Config:
        elasticsearch_hosts  = []           : List of elasticsearch hosts within same cluster (list: for failover).
                                              Default "localhost:9200" if empty.
        index                = None         : If 'index' is set it will override both 'channel_index' and 'item_index'!
        channel_index        = "rss"        : Index where channel metadata will be stored as doctype 'channel'.
        item_index           = None         : Index where items will be stored as doctype 'items'.
                                              If not set and not overridden by 'index', items will not be written automatically.
                                              Also if there are connectors on the output socket 'item' then they are not written automatically
        include_linked_page  = False        : Whether to fetch referenced link to article from RSS item.
        interval             = 600          : Check for new RSS items every 'interval' seconds. Defaults to 10 minutes by default.
        channels             = []           : All channels if empty.
        simulate             = False        : If 'simulate' is set, fetched items and channel metadata will not be written to the index.
    """

    DOCTYPE_ITEM         = "item"
    DOCTYPE_CHANNEL      = "channel"
    MAX_CHANNELS = 1000

    def __init__(self, **kwargs):
        super(RssMonitor, self).__init__(**kwargs)

        self._output = self.create_socket("item" , "esdoc.rss"  , "RSS item with linked-to web page added (if so configured).")

        self.config.set_default(
            elasticsearch_hosts = [],
            index               = None,
            channel_index       = "rss",
            item_index          = None,
            include_linked_page = False,
            interval            = 10*60,         # 10 minutes
            channels            = [],
            simulate            = False
        )

        self._last_get_time = 0  # For use only by on_tick()

    @property
    def _channel_index(self):
        return self.config.index or self.config.channel_index

    @property
    def _item_index(self):
        return self.config.index or self.config.item_index

    def on_open(self):
        # Note: Intentionally not resetting _last_get_time, so that 'interval' will pass before we do another fetch if we restart.

        # Make sure we have an index for the channel data configured
        if not self._channel_index:
            raise ValueError("Missing index; neither given as argument nor 'index' or 'channel_index' configured.")

        # Ensure at least an index for the channel data exists in Elasticsearch
        es = self._get_es()
        self._create_index(es, self._channel_index, expected_to_exist=True)

    def on_tick(self):
        if (time.time() - self._last_get_time) > self.config.interval:
            # This one also writes to Elasticsearch and/or item socket

            self.log.debug("***TICK")

            es = self._get_es()
            # We have to burn through this as fast as possible. It is a generator that also serves
            # an offline fetch mode (i.e. not part of the run loop), but here we need it to process
            # the items fast and be done and write back the new channel status immediately.
            self.log.debug("***FETCHING ITEMS BEGIN")
            for item in self._fetch_items(es, self.config.channels, simulate=self.config.simulate):
                pass
            self.log.debug("***FETCHING ITEMS END")

            self._last_get_time = time.time()

    #region Misc core methods

    def _get_es(self):
        hosts = None
        # Funny that we have to do this rather than supplying the empty list as it is...
        if self.config.elasticsearch_hosts:
            hosts = self.config.elasticsearch_hosts
        return elasticsearch.Elasticsearch(hosts=hosts)

    def _create_index(self, es, index, expected_to_exist=False):
        try:
            self.log.debug("Checking if index '%s' already exists." % index)
            if es.indices.exists(index=index):
                msg = "Index '%s' already exists. Leaving it as it is." % index
                if expected_to_exist:
                    self.log.debug(msg)
                else:
                    self.log.warning(msg)
                return False
        except elasticsearch.exceptions.ConnectionError as e:
            msg = "Failed to connect to Elasticsearch: %s: %s" % (e.__class__.__name__, e)
            self.log.critical(msg)
            raise Exception(msg)

        body = {
            "settings": es_default_settings,
            "mappings": {
                self.DOCTYPE_CHANNEL: es_default_mapping_channel,
                self.DOCTYPE_ITEM   : es_default_mapping_item,
            }
        }

        self.log.info("Creating index '%s'." % index)

        try:
            es.indices.create(index=index, body=body)
        except elasticsearch.exceptions.ConnectionError as e:
            msg = "Failed to connect to Elasticsearch: %s: %s" % (e.__class__.__name__, e)
            self.log.critical(msg)
            raise Exception(msg)

        self.log.status("Index '%s' created with channel and item mappings." % index)
        return True

    def _fetch_items(self, es, channel_names=None, force=False, simulate=False, offline=False):

        self.log.debug("***GET CHANNEL NAMES BEGIN")
        existing_channels = self._get_channels(es, channel_names)

        # Check and report if given channel_names are not registered
        if channel_names:
            for name in channel_names:
                if not name in existing_channels:
                    self.log.warning("Warning: Channel '%s' is not registered. (skipping)" % name)
        self.log.debug("***GET CHANNEL NAMES END")

        # Fetch RSS feeds and write items and new channel info with new lastFetch time (== now)
        for name, existing_channel in existing_channels.items():

            # Only allow leaving the fetch loop here between full channel fetches, to avoid incomplete data.
            # Except abort, that can happen inside item loop
            if not offline and self.end_tick_reason or self.suspended:
                self.log.debug("***END TICK REASON -- BREAKING")
                break

            url  = existing_channel["url"]
            last_fetch_date = None
            if "lastFetch" in existing_channel and existing_channel["lastFetch"]:
                last_fetch_date = iso2date(existing_channel["lastFetch"])

            # Get RSS feed
            self.log.debug("***GET RSS FEED BEGIN")
            rss = self._get_rss(name, url)
            self.log.debug("***GET RSS FEED END")
            if not rss:
                self.log.error("Failed to read RSS feed from channel '%s'. (skipping)" % name)
                continue
            channel, items = rss

            # Check if it is time to process the feed (or 'force' is specified)
            updated = iso2date(channel["updated"])
            qualified = False
            if not updated or not last_fetch_date or updated > last_fetch_date:
                qualified = True

            if not qualified:
                if force:
                    self.log.verbose("Nothing new to fetch in '%s', but proceeding since 'force' was specified." % name)
                else:
                    self.log.verbose("Nothing new to fetch in '%s'." % name)
                    continue

            # Feed items to ES
            nItems = len(items)
            nNewItems = 0
            nReplacedItems = 0
            for item in items:
                # Aborting will stop further processing immediately
                if not offline and self.aborted:
                    return

                # Is it new?
                itemUpdatedDate = iso2date(item.get("updated", None))
                itemQualified = False
                if not itemUpdatedDate or not last_fetch_date or itemUpdatedDate > last_fetch_date:
                    itemQualified = True
                if not itemQualified and not force:
                    continue

                if self.config.include_linked_page:
                    self._add_linked_page(item)

                self.log.debug("***YIELD ITEM BEGIN")
                yield item
                self.log.debug("***YIELD ITEM END")

                self.log.debug("***PUT ITEM BEING")
                new, replaced = self._put_item(es, name, item, simulate)
                self.log.debug("***PUT ITEM END")
                nNewItems += new
                nReplacedItems += replaced

            # Update channel info with new lastFetch time (== now)
            channel["lastFetch"] =  date2iso(datetime.utcnow())
            if simulate:
                self.log.debug("Simulating update to channel '%-10s': %3d new, %3d replaced." % (name, nNewItems, nReplacedItems))
            else:
                self.log.info("Channel '%-10s': %3d new, %3d replaced." % (name, nNewItems, nReplacedItems))
                self.log.debug("***PUT CHANNEL INFO BEGIN")
                self._put_channel(es, channel)
                self.log.debug("***PUT CHANNEL INFO END")

        self.log.debug("***_FETCH_ITEMS GENERATOR COMPLETED")

    @staticmethod
    def _create_query_filter(filter):
        return {"query":{"filtered":{"filter":filter}}}

    def _get_channels(self, es, channel_names=None):
        "Get channels from ES."
        body = None
        if channel_names:
            body = self._create_query_filter({"terms": {"name": channel_names}})
        else:
            body = {"query": {"match_all": {}}}
        body.update({"size": self.MAX_CHANNELS})

        msg = None
        try:
            res = es.search(index=self._channel_index, doc_type=self.DOCTYPE_CHANNEL, body=body);
        except elasticsearch.exceptions.ConnectionError as e:
            msg = "Failed to connect to Elasticsearch: %s: %s" % (e.__class__.__name__, e)
        except elasticsearch.exceptions.NotFoundError:
            msg = "Index/channel '%s/%s' not found." % (self._channel_index, self.DOCTYPE_CHANNEL)
        except elasticsearch.exceptions.TransportError as e:
            msg = "Elasticsearch TransportError: %s: %s" % (e.__class__.__name__, e)
        except Exception as e:
            msg = "Exception while writing channel to Elasticsearch: %s: %s" % (e.__class__.__name__, e)
        if msg:
            self.log.error(msg)
            return {}

        channels = {}
        for hit in res["hits"]["hits"]:
            name = hit["_id"]
            channel = hit["_source"]
            channels[name] = channel
        return channels

    def _put_channel(self, es, channel):
        "Write channel to ES."
        # Do not raise exceptions. It might be able to write again on next attempt.
        try:
            res = es.index(index=self._channel_index, doc_type=self.DOCTYPE_CHANNEL, id=channel["name"], body=channel)
        except elasticsearch.exceptions.ConnectionError as e:
            msg = "Failed to connect to Elasticsearch: %s: %s" % (e.__class__.__name__, e)
            self.log.error(msg)
        except elasticsearch.exceptions.NotFoundError:
            self.log.warning("Index/channel '%s/%s' not found." % (self._channel_index, self.DOCTYPE_CHANNEL))
        except elasticsearch.exceptions.TransportError as e:
            self.log.error("Elasticsearch TransportError: %s: %s" % (e.__class__.__name__, e))
        except Exception as e:
            self.log.error("Exception while writing channel to Elasticsearch: %s: %s" % (e.__class__.__name__, e))

    def _add_linked_page(self, doc):
        link = doc["_source"].get("link")
        if link:
            # TODO: try/except; for now, let it fail here
            try:
                res = requests.get(link, verify=False)
                doc["_source"]["page"] = res.text
            except Exception as e:
                # Note: Logging this to doclog rather than processor log, since this is probably an invalid URL related to the item.
                self.doclog.warning("Failed to fetch referenced URL: %s" % link)
                self.doclog.debug("Exception when fetching referenced URL: %s: %s" % (e.__class__.__name__, e))

    def _put_item(self, es, channel_name, doc, simulate):

        new = 0; replaced = 0

        # Write
        if self._output.has_output:
            self._output.send(doc)
            new = 1  # Consider it new; don't bother checking
        elif simulate:
            self.doclog.debug("Simulating new item in %-10s: %s" % (channel_name, doc["_source"]["title"]))
            new = 1
        elif self._item_index:
            ok = False
            try:
                res = es.index(index=self._item_index, doc_type=self.DOCTYPE_ITEM, id=doc["_id"], body=doc["_source"])
                if res["created"]:
                    new = 1
                else:
                    replaced = 1
                ok = True
            except elasticsearch.exceptions.ConnectionError as e:
                msg = "Failed to connect to Elasticsearch: %s: %s" % (e.__class__.__name__, e)
                self.log.error(msg)
            except elasticsearch.exceptions.NotFoundError as e:
                self.log.warning("Index/type '%s/%s' not found." % (self._item_index, self.DOCTYPE_ITEM, e.args[1]))
            except elasticsearch.exceptions.TransportError as e:
                self.log.error("Elasticsearch TransportError: %s: %s" % (e.__class__.__name__, e))
            except Exception as e:
                self.log.error("Exception while writing item to Elasticsearch: %s: %s" % (e.__class__.__name__, e))

            title = doc["_source"]["title"]
            if not ok:
                self.doclog.warning("Failed to write item in channel '%s': %s" % (channel_name, title))
            elif new:
                self.doclog.debug("New      item in %-10s: %s" % (channel_name, doc["_source"]["title"]))
            elif replaced:
                self.doclog.trace("Replaced item in %-10s: %s" % (channel_name, doc["_source"]["title"]))

        return (new, replaced)

    def _get_rss_channel_info(self, channel_name, url):
        rss = self._get_rss(channel_name, url, skip_items=True)
        return rss[0] if rss else None

    def _delete_channel(self, es, channel_name):
        try:
            res = es.delete(index=self._channel_index, doc_type=self.DOCTYPE_CHANNEL, id=channel_name)
        except Exception as e:
            self.log.warning("Failed to delete channel info for channel '%s'." % channel_name)
            self.log.debug("Exception when deleting: %s: %s" % (e.__class__.__name__, e))
            return 0
        self.log.info("Channel '%s' deleted." % channel_name)
        return 1

    def _delete_items(self, es, channel_names, before_date):

        if not self._item_index:
            self.log.debug("Item index not configured; cannot delete items.")
            return 0

        and_parts = []

        if channel_names:
            and_parts.append({"terms": {"channel": channel_names}})

        if before_date:
            iso_before = date2iso(before_date)
            and_parts.append({"range": {"updated": { "to": iso_before }}})

        body = None
        if and_parts:
            body = self._create_query_filter({"and": and_parts})
        else:
            body = {"query": {"match_all": {}}}

        count = 0

        try:
            # First find how many we expect to delete...
            res = es.count(index=self._item_index, doc_type=self.DOCTYPE_ITEM, body=body)
            # ...then delete them.
            es.delete_by_query(index=self._item_index, doc_type=self.DOCTYPE_ITEM, body=body)
            count = res["count"]
        except elasticsearch.exceptions.ConnectionError as e:
            msg = "Failed to connect to Elasticsearch: %s: %s" % (e.__class__.__name__, e)
            self.log.error(msg)
            return 0  # Proceed with the next channel... maybe we'll have better luck there (if tmp error)
        except elasticsearch.exceptions.NotFoundError:
            self.log.warning("Index/type '%s/%s' not found." % (self._item_index, self.DOCTYPE_ITEM))
            return 0  # Proceed with the next channel... maybe we'll have better luck there (if tmp error)

        return count

    def _list_channels(self, es, channel_names=None, since_date=None):

        channels = self._get_channels(es, channel_names)

        if not self._item_index:
            self.log.warning("No item index configured, so we cannot retrieve item count per channel. (skipping)")
        else:
            # TODO: CONVERT THIS TO *ONE* AGGREGATION CALL INSTEAD

            total = 0
            for name, channel in channels.items():

                # Get item count from ES

                name_part = {"term": {"channel": name}}
                since_part = None
                if since_date:
                    iso_since = date2iso(since_date)
                    since_part = {"range": {"updated": { "from" : iso_since }}}

                query = None
                if since_part:
                    query = self._create_query_filter({"and" : [name_part, since_part]})
                else:
                    query = self._create_query_filter(name_part)

                try:
                    res = es.count(index=self._item_index, doc_type=self.DOCTYPE_ITEM, body=query)
                except elasticsearch.exceptions.ConnectionError as e:
                    msg = "Failed to connect to Elasticsearch: %s: %s" % (e.__class__.__name__, e)
                    self.log.error(msg)
                    continue

                count = res["count"]
                channel["count"] = count
                total += count

                if res["_shards"]["successful"] < res["_shards"]["total"]:
                    self.log.error("Only partial result returned from Elasticsearch. Elasticsearch problem?")

        return list(channels.values())

    def _list_items(self, es, channel_names, since_date, limit):

        if not self._item_index:
            self.log.debug("Item index not configured; cannot retrieve items.")
            return

        body = {}
        desc = {"order": "desc"}
        body.update({"size": limit, "sort": [{"updated": desc}, {"_timestamp": desc}]})
        and_parts = []

        if channel_names:
            and_parts.append({"terms": {"channel": channel_names}})

        if since_date:
            iso_since = date2iso(since_date)
            and_parts.append({"range": {"updated": { "from": iso_since }}})

        if and_parts:
            body.update(self._create_query_filter({"and": and_parts}))
        else:
            body.update({"query": {"match_all": {}}})

        msg = None
        try:
            res = es.search(index=self._item_index, doc_type=self.DOCTYPE_ITEM, body=body)
        except elasticsearch.exceptions.ConnectionError as e:
            msg = "Failed to connect to Elasticsearch: %s: %s" % (e.__class__.__name__, e)
        except elasticsearch.exceptions.NotFoundError:
            msg = "Index/type '%s/%s' not found." % (self._item_index, self.DOCTYPE_ITEM)
        except elasticsearch.exceptions.TransportError as e:
            msg = "Elasticsearch TransportError: %s: %s" % (e.__class__.__name__, e)
        except Exception as e:
            msg = "Exception while writing channel to Elasticsearch: %s: %s" % (e.__class__.__name__, e)

        if msg:
            self.log.critical(msg)
            raise Exception(msg)

        partial = False
        if res["_shards"]["successful"] < res["_shards"]["total"]:
            partial = True
        if partial:
            self.log.error("Only partial result returned from Elasticsearch. Elasticsearch problem?")

        #total = res["hits"]["total"]
        #print "%d HITS:" % total

        for item in res["hits"]["hits"]:
            # Convert updated date string to a datetime type
            #updated_iso = item["_source"].get("updated")
            #if updated_iso:
            #    item["_source"]["updated"] = iso2date(updated_iso)
            yield item

    #endregion Misc core methods

    #region Get RSS item

    @staticmethod
    def _getUUID(from_str):
        return str(uuid.uuid3(uuid.NAMESPACE_URL, from_str.encode("ascii", "ignore")))

    @staticmethod
    def _add_if(target, from_dict, from_name, to_name=None):

        if to_name is None: to_name = from_name

        if from_name.startswith("*"):
            search_key = from_name[1:]
            for key in from_dict:
                if key.endswith(search_key):
                    target.update({to_name: from_dict[key]})
                    break
        elif from_name in from_dict:
            target.update({to_name: from_dict[from_name]})

    def _get_channel_updated_iso_string(self, channel_name, channel):
        t = time.gmtime()
        updated_parsed = channel.get("updated_parsed")
        if updated_parsed and type(updated_parsed) is time.struct_time:
            t = channel["updated_parsed"]
        elif self.log.isEnabledFor(logging.DEBUG):
            self.log.debug("Warning: Channel '%s' missing update time in channel meta data. Using processing time instead." % channel_name)
        return time.strftime("%Y-%m-%dT%H:%M:%SZ", t)

    def _get_item_updated_iso_string(self, channel_name, item):
        t = time.gmtime()
        if "updated_parsed" in item and type(item["updated_parsed"]) is time.struct_time:
            t = item["updated_parsed"]
        elif "published_parsed" in item and type(item["published_parsed"]) is time.struct_time:
            t = item["published_parsed"]
        elif self.log.isEnabledFor(logging.DEBUG):
            self.doclog.debug("Warning: An item in channel '%s' is missing both update and publish time in item data. Using processing time." % channel_name)
        return time.strftime("%Y-%m-%dT%H:%M:%SZ", t)

    def _get_rss(self, channel_name, url, skip_items=False):

        try:
            feed = feedparser.parse(url)
        except Exception as e:
            self.log.error("Failed to read RSS feed from channel '%s' with URL: %s" % (channel_name, url))
            self.log.debug("Exception when trying to read RSS feed: %s: %s" % (e.__class__.__name__, e))
            return None

        channel = feed["channel"]
        items = feed["items"]

        feed_url = feed.get("url")
        if feed_url is None:
            self.log.error("FIELD 'url' MISSING IN METADATA FOR CHANNEL '%s', CHANNEL OBJECT: %s" % (channel_name, channel))
        elif not url == feed_url:
            self.log.warning("Registered URL and URL in metadata for channel '%s' differ: Our='%s', Remote='%s'." % (channel_name, url, feed["url"]))

        # Create channel info part
        cinfo = {"name": channel_name, "url": url, "updated": self._get_channel_updated_iso_string(channel_name, channel)}
        self._add_if(cinfo, feed   , "version")
        self._add_if(cinfo, feed   , "url")
        self._add_if(cinfo, channel, "title")
        self._add_if(cinfo, channel, "link")
        self._add_if(cinfo, channel, "subtitle", "description")
        self._add_if(cinfo, channel, "language")
        self._add_if(cinfo, channel, "generator")
        #self._add_if(cinfo, channel, "*_updateperiod", "update_period")
        #self._add_if(cinfo, channel, "*_updatefrequency", "update_frequency")
        #self._add_if(cinfo, channel, "ttl")

        #print "Debug: PROCESSED JSON FOR FEED [%s] %s" % (cinfo["version"], cinfo["title"])
        #print json.dumps(cinfo, indent=2)


        if skip_items:
            return (cinfo, None)

        docs = []
        for i in items:

            # Prefer "id", alternative "link" as "id", or skip (missing ID is too serious)
            # Our IDs must be globally unique, so we add "<channel_name>#" prefix to the ID.
            rid = channel_name + "#"
            if "id" in i:
                rid += i["id"]
            elif "link" in i:
                rid += i["link"]
                self.doclog.debug("Found item in channel '%s' without 'id', using 'link' instead." % channel_name)
            else:
                self.doclog.warning("Dropping item from channel '%s' with neither 'id' nor 'link'." % channel_name)
                continue

            # Prefer "updated", alternative "published", alternative current processing time
            updatedStr = self._get_item_updated_iso_string(channel_name, i)

            # Extract categories from "tags/term"
            categories = []
            if "tags" in i:
                for t in i["tags"]:
                    categories.append(t["term"])

            iinfo = {"channel": channel_name, "updated": updatedStr, "categories": categories}
            self._add_if(iinfo, i, "link")
            self._add_if(iinfo, i, "title")
            self._add_if(iinfo, i, "author")
            self._add_if(iinfo, i, "comments")

            #self._add_if(iinfo, i, "summary") #, "description"
            #self._add_if(iinfo, i, "content")
            # "content" comes with sub elements ("base", "type", "value"). Simplifying for now by extracting only value.
            # This actually again comes from the non-list field "description" in RSS, AFAIK. So calling it "description" here..
            # But there is not always "content", so use "summary" if it does not exist
            if "content" in i and i["content"]:
                iinfo.update({"description": i["content"][0]["value"]})
            else:
                self._add_if(iinfo, i, "summary", "description")

            # Note: Skip "location" for now (in ES mapping)

            # Build the esdoc
            doc = {"_id": self._getUUID(rid), "_index": self._item_index, "_type": self.DOCTYPE_ITEM, "_source": iinfo}

            docs.append(doc)

        return (cinfo, docs)

    #endregion Get RSS item

    #region Utility methods

    def create_index(self, index=None):
        """
        Create specified index or the configured effective 'channel_index' with mappings for both 'channel' and 'item'
        document types.

        :param   str  index   : Override index to create, otherwise it is assumed from the configured 'index' or 'channel_index'.
        :returns bool success : Whether the index was created.

        :raises ValueError, Exception:
        """
        if not index:
            index = self._channel_index
        if not index:
            raise ValueError("Missing index; neither given as argument nor 'index' or 'channel_index' configured.")

        es = self._get_es()
        return self._create_index(es, index)

    def delete_index(self, index=None):
        """
        Delete specified index or the configured effective 'channel_index' in Elasticsearch.

        :param   str  index   : Index to delete. Default is to use configured index or channel_index.
        :returns bool success : Whether if index was deleted.

        :raises ValueError, Exception:
        """
        if not index:
            index = self._channel_index
        if not index:
            raise ValueError("Missing index; netiher given as argument nor 'index' or 'channel_index' configured.")

        self.log.info("Deleting index '%s'." % index)
        es = self._get_es()
        try:
            es.indices.delete(index=index)
        except elasticsearch.exceptions.ConnectionError as e:
            msg = "Failed to connect to Elasticsearch: %s: %s" % (e.__class__.__name__, e)
            self.log.critical(msg)
            raise Exception(msg)
        except elasticsearch.exceptions.NotFoundError:
            self.log.warning("Index '%s' not found." % index)
            return False
        self.log.status("Index '%s' deleted." % index)
        return True

    def list_channels(self, channel_names=None, since_date=None):
        """
        Get a list of channels and their info.

        :param  list     channel_names : List of channel names, otherwise all channels.
        :param  datetime since_date    : Only get item counts since this date, if any.
        :returns list    channels      : List of channel info items (dict).
        """
        self.log.info("Retrieving channels to list.")
        es = self._get_es()
        channels = self._list_channels(es, channel_names, since_date)
        self.log.info("Channels retrieved.")
        return channels

    def add_channels(self, *channel_infos):
        """
        Add channels to watch. Will look up the URLs to get the channels' published metadata.

        :param   channel_infos  : List of tuples of (channel_name, url).
        :returns int     number : Number of channels registered, whether new or updated.
        """
        self.log.info("Adding %d channels." % len(channel_infos))

        es = self._get_es()

        totalCount = 0
        replaceCount = 0

        existing_channels = self._get_channels(es)

        for channel_name, url in channel_infos:
            totalCount += 1
            found = False
            if channel_name in existing_channels:
                replaceCount += 1
                found = True

            channel = self._get_rss_channel_info(channel_name, url)
            if not channel:
                self.log.error("Failed to read channel '%s' from URL: %s" % (channel_name, url))
                continue

            if not found:
                self.log.info("Adding   channel: %s" % channel_name)
            else:
                self.log.info("Updating channel: %s" % channel_name)

            self._put_channel(es, channel)

        self.log.status("Registered %d channels. (%d new, %d updated)" % (totalCount, totalCount - replaceCount, replaceCount))
        return totalCount

    def delete_channels(self, channel_names=None, delete_items=False):
        """
        Delete all or specified channels, optionally also delete all associated items.

        :param  list channel_names : List of channel names to delete, or all if nothing listed.
        :param  bool delete_items  : Also delete all items associated with the deleted channels.
        :return int  number        : Number of channels actually deleted.
        """
        es = self._get_es()
        channels = self._get_channels(es, channel_names)

        self.log.info("Deleting %d channels." % len(channels))

        # Check and report if given channel_names are not registered
        if channel_names:
            for name in channel_names:
                if not name in channels:
                    self.log.warning("Channel '%s' not found." % name)

        # Delete
        count = 0; item_count = 0
        if delete_items:
            item_count = self._delete_items(es, [name], None)
        for name, channel in channels.items():
            count += self._delete_channel(es, name)

        incl_str = (", including %d items" % item_count) if item_count else ""
        self.log.status("%d channels(s) removed%s." % (count, incl_str))

        return count

    def list_items(self, channel_names=None, since_date=None, limit=10):
        """
        Get list of items, filtered by channels or for all channels.

        :param  list     channel_names : Channels to filter on, or all not specified.
        :param  datetime since_date    : Get items newer than this date.
        :param  int      limit         : Max number of items to retrieve.
        :return list     items         : Returns a list of items in 'esdoc' format.
        """
        self.log.info("Retrieving items to list.")
        es = self._get_es()
        for item in self._list_items(es, channel_names, since_date, limit):
            yield item
        self.log.info("Items retrieved.")

    def fetch_items(self, channel_names=None, force=False, simulate=False):
        """
        Fetch new items from RSS feeds or all available items if 'force' is True.

        :param   list channel_names : Fetch items for given channel names, or any if none specified.
        :param   bool force         : Ignore last fetch date, publishing dates, etc, and retrieve all that is available.
        :param   bool simulate      : Do not write to item index or update channel index with last fetch date, etc.
                                      The fetch will still be simulated if the config.simulate is set; regardless of this.
        :returns generator items    : Returns the list of items fetched, in 'esdoc' format.
        """
        self.log.info("Fetching items.")
        es = self._get_es()
        count = 0
        for item in self._fetch_items(es, channel_names, force, simulate or self.config.simulate, offline=True):
            count += 1
            yield item
        self.log.status("Fetch completed. %d items fetched." % count)

    def delete_items(self, channel_names=None, before_date=None):
        """
        Delete items for given channels or all channels, older than 'before_data', or all.

        :param   list     channel_names : Channels to filter on, or all not specified.
        :param   datetime before_date   : Only delete items older than this date, or all if not specified.
        :returns int      number        : Returns number of items actually deleted.
        """
        if not self._item_index:
            self.log.critical("Item index not configured; cannot delete items.")
            return 0

        self.log.info("Deleting items...")

        es = self._get_es()
        num = self._delete_items(es, channel_names, before_date)

        if not channel_names:
            self.log.status("%d items deleted from all channels." % num)
        else:
            self.log.status("%d items deleted from: %s" % (num, ", ".join(channel_names)))

        return num

    #endregion Utility methods
