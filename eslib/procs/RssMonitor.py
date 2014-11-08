__author__ = 'Hans Terje Bakke'

#TODO: Bulk check Elasticsearch for existing items
#TODO: cmd script with ESLIB_RSS_ELASTICSEARCH_HOSTS environment variable
#TODO: cmd script use logs for verbose output
#TODO: Elasticsearch mappings for channels and items
#TODO: Delete/Remove indices entirely
#TODO: Elasticsearch mappings use 'facet' analyzer

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
        "channel"      : {"type": "string"   , "index": "not_analyzed", "include_in_all": False},
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
        "enclosures" :
        {
            "properties" : {
                "url"    : {"type": "string", "index": "no"          , "include_in_all": False},
                "type"   : {"type": "string", "index": "not_analyzed", "include_in_all": False},
                "length" : {"type": "long"  , "index": "no"          , "include_in_all": False}
            }
        },

        "page"         : {"type": "string"  , "index": "analyzed"     , "include_in_all": True}
    }
}

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
    """
    #TODO: Document
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

        self._last_get_time = 0

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
            es = self._get_es()
            self._fetch_items(es, self.config.channels, simulate=self.config.simulate)

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
            msg = "Failed to connect to Elasticsearch: " + e.args[1]
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
            msg = "Failed to connect to Elasticsearch: " + e.args[1]
            self.log.critical(msg)
            raise Exception(msg)

        self.log.status("Index '%s' created with channel and item mappings." % index)
        return True

    def _fetch_items(self, es, channel_names=None, force=False, simulate=False, offline=False):

        existing_channels = self._get_channels(es, channel_names)

        # Check and report if given channel_names are not registered
        if channel_names:
            for name in channel_names:
                if not name in existing_channels:
                    self.log.warning("Warning: Channel '%s' is not registered. (skipping)" % name)

        # Fetch RSS feeds and write items and new channel info with new lastFetch time (== now)
        for name, existing_channel in existing_channels.items():

            # Only allow leaving the fetch loop here between full channel fetches, to avoid incomplete data.
            # Except abort, that can happen inside item loop
            if not offline and self.end_tick_reason or self.suspended:
                break

            url  = existing_channel["url"]
            last_fetch_date = None
            if "lastFetch" in existing_channel and existing_channel["lastFetch"]:
                last_fetch_date = iso2date(existing_channel["lastFetch"])

            # Get RSS feed
            rss = self._get_rss(name, url)
            if not rss:
                self.log.error("Failed to read RSS feed from channel '%s'. (skipping)" % name)
                continue
            channel, items = rss

            # Check if it is time to process the feed (or 'force' is specified)
            updated = iso2date(channel["updated"])
            #========
            #print "LAST_FETCH    ", lastFetchDate
            #print "CHANNEL UPDATE", updated
            #========
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

                yield item

                if not simulate:
                    new, replaced = self._put_item(es, name, item)
                    nNewItems += new
                    nReplacedItems += replaced

            # Update channel info with new lastFetch time (== now)
            channel["lastFetch"] =  date2iso(datetime.utcnow())
            if not simulate:
                self._put_channel(es, channel)
                self.log.info("Channel '%-10s': %3d new, %3d replaced." % (name, nNewItems, nReplacedItems))
                self._last_get_time = time.time()

    @staticmethod
    def _create_query_filter(filter):
        return {"query":{"filtered":{"filter":filter}}}

    def _get_channels(self, es, channel_names=None):
        "Get channels from ES."
        body = None
        if channel_names:
            body = self._create_query_filter({"terms": {"channel": channel_names}})
        else:
            body = {"query": {"match_all": {}}}
        body.update({"size": self.MAX_CHANNELS})

        try:
            res = es.search(index=self._channel_index, doc_type=self.DOCTYPE_CHANNEL, body=body);
        except elasticsearch.exceptions.ConnectionError as e:
            msg = "Failed to connect to Elasticsearch: " + e.args[1]
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
        # TODO: try/except
        res = es.index(index=self._channel_index, doc_type=self.DOCTYPE_CHANNEL, id=channel["channel"], body=channel)

    def _add_linked_page(self, doc):
        link = doc["_source"].get("link")
        if link:
            # TODO: try/except; for now, let it fail here
            res = requests.get(link, verify=False)
            page = res.text

            doc["_source"]["page"] = page
            #print "Debug: In feed '%s', read item in linked-to page; size = %d bytes." % (feedname, len(page))

    def _put_item(self, es, channel_name, doc):

        new = 0; replaced = 0

        # Write
        if self._output.has_output:
            self._output.send(doc)
            new = 1  # Consider it new; don't bother checking
        elif self._item_index:
            # TODO: try/except; for now, let it fail here
            #print json.dumps(body, indent=2) # DEBUG
            res = es.index(index=self._item_index, doc_type=self.DOCTYPE_ITEM, id=id, body=doc["_source"])
            if res["created"]:
                self.doclog.debug("New item in %s: %s" % (channel_name, doc["_source"]["title"]))
                new = 1
            else:
                replaced = 1

        return (new, replaced)

    def _get_rss_channel_info(self, channel_name, url):
        rss = self._get_rss(channel_name, url, skip_items=True)
        return rss[0] if rss else None

    def _delete_channel(self, es, channel_name):
        try:
            res = es.delete(index=self._channel_index, doc_type=self.DOCTYPE_CHANNEL, id=channel_name)
        except:
            self.log.warning("Failed to delete channel info for channel '%s'." % channel_name)
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

        #print "***DELETING"; print json.dumps(body)
        #TODO: try/except
        res = es.count(index=self._item_index, doc_type=self.DOCTYPE_ITEM, body=body)
        es.delete_by_query(index=self._item_index, doc_type=self.DOCTYPE_ITEM, body=body)

        return res["count"]

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
                    msg = "Failed to connect to Elasticsearch: " + e.args[1]
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

        #print json.dumps(body,indent=2)

        # TODO: try/except
        res = es.search(index=self._item_index, doc_type=self.DOCTYPE_ITEM, body=body)

        partial = False
        if res["_shards"]["successful"] < res["_shards"]["total"]:
            partial = True
        if partial:
            self.log.error("Only partial result returned from Elasticsearch. Elasticsearch problem?")

        #total = res["hits"]["total"]
        #print "%d HITS:" % total

        for item in res["hits"]["hits"]:
            # Convert updated date string to a datetime type
            updated_iso = item["_source"].get("updated")
            if updated_iso:
                item["_source"]["updated"] = iso2date(updated_iso)
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
        if "updated_parsed" in channel and type(channel["updated_parsed"]) is time.struct_time:
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
        except:
            self.log.error("Failed to read RSS feed from channel '%s' with URL: %s" % (channel_name, url))
            return None

        channel = feed["channel"]
        items = feed["items"]

        if not url == feed["url"]:
            self.log.warning("Registered URL and URL in channel meta differ: Our='%s', Remote='%s'." % (url, feed["url"]))

        # Create channel info part
        cinfo = {"channel": channel_name, "url": url, "updated": self._get_channel_updated_iso_string(channel_name, channel)}
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
            updatedStr = self._get_item_updated_iso_tring(channel_name, i)

            # Extract categories from "tags/term"
            categories = []
            if "tags" in i:
                for t in i["tags"]:
                    categories.append(t["term"])

            iinfo = {"channel": channel_name, "updated": updatedStr, "categories": categories}
            self.add_if(iinfo, i, "link")
            self.add_if(iinfo, i, "title")
            self.add_if(iinfo, i, "author")
            self.add_if(iinfo, i, "comments")

            #addIf(iinfo, i, "summary") #, "description"
            #addIf(iinfo, i, "content")
            # "content" comes with sub elements ("base", "type", "value"). Simplifying for now by extracting only value.
            # This actually again comes from the non-list field "description" in RSS, AFAIK. So calling it "description" here..
            # But there is not always "content", so use "summary" if it does not exist
            if "content" in i and i["content"]:
                iinfo.update({"description": i["content"][0]["value"]})
            else:
                self.add_if(iinfo, i, "summary", "description")

            # Note: Skip "location" for now (in ES mapping)
            # Note: Skip "enclosures" (with "url", "type", "length") for now (in ES mapping)

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

        :param index:
        :returns bool success: Whether the index was created.
        raises ValueError, Exception
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

        :param str index: Index to delete. Default is to use configured index or channel_index.
        :returns bool success: Whether if index was deleted.
        :raises ValueError, Exception
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
            msg = "Failed to connect to Elasticsearch: " + e.args[1]
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
        :param list channel_names: List of channel names, otherwise all channels.
        :param since_date: Only get item counts since this date, if any.
        :returns list channels: List of channel info items (dict).
        """
        self.log.info("Retrieving channels to list.")
        es = self._get_es()
        channels = self._list_channels(es, channel_names, since_date)
        self.log.info("Channels retrieved.")
        return channels

    def add_channels(self, *channel_infos):
        """
        Add channels to watch. Will look up the URLs to get the channels' published metadata.
        :param channel_infos: List of tuples of (channel_name, url).
        :returns int number: Number of channels registered, whether new or updated.
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
        :param list channel_names: List of channel names to delete, or all if nothing listed.
        :param bool delete_items: Also delete all items associated with the deleted channels.
        :return int number: Number of channels actually deleted.
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

    def list_items(self, channel_names=None, since_date=None, limit=0):
        #TODO: Document
        self.log.info("Retrieving items to list.")
        es = self._get_es()
        for item in self._list_items(es, channel_names, since_date, limit):
            yield item
        self.log.info("Items retrieved.")

    def fetch_items(self, channel_names=None, force=False, simulate=False):
        #TODO: DOCUMENT ARGS
        self.log.info("Fetching items.")
        es = self._get_es()
        count = 0
        for item in self._fetch_items(channel_names, force, simulate, offline=True):
            count += 1
            yield item
        self.log.status("Fetch completed. %d items fetched." % count)

    def delete_items(self, channel_names=None, before_date=None):

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
