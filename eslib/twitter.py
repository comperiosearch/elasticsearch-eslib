from __future__ import absolute_import
from eslib import Configurable
from TwitterAPI import TwitterAPI
from dateutil.parser import parse

import json
import time



class Twitter(Configurable):
    """Connects to twitter and retrieves information. Cares about rate limits"""

    def __init__(self, **kwargs):
        """
        Establish a connection to twitter. Expects the following variables
        to be present in self.config:
            - consumer_key
            - consumer_secret
            - access_token
            - access_token_secret

        """
        
        super(Twitter, self).__init__(**kwargs)
        self.api = TwitterAPI(self.config.consumer_key,
                              self.config.consumer_secret,
                              self.config.access_token,
                              self.config.access_token_secret)

        self.set_rate_limits()

        self.config.set_default(
            user_fields={"id", "location", "description", "screen_name",
                         "lang", "name", "created_at"}
        )

    def set_rate_limits(self):
        """ 
        Connects to twitter and asks for the number of requests allowed
        per 15 minutes.
        Results are put in the self.limits dict.

        """

        raw_rq = self.api.request("application/rate_limit_status", 
                                  {"resources": "users"}).text
        user_rq = json.loads(raw_rq)
        user_lim = int(user_rq["resources"]["users"]["/users/lookup"]["limit"])
        
        raw_rq = self.api.request("application/rate_limit_status", 
                                  {"resources": "followers"}).text

        flw_rq = json.loads(raw_rq)
        flw_lim = flw_rq["resources"]["followers"]["/followers/ids"]["limit"]
        flw_lim = int(flw_lim)

        self.last_call = {"users": 0,
                          "followers": 0}
        self.limits = {"users": user_lim, "followers": flw_lim}

    def blew_rate_limit(self, protocol):
        raw_rq = self.api.request("application/rate_limit_status",
                                  {"resources": protocol}).text
        resp = json.loads(text)
        if protocol == "users":
            wait_time = int(resp["resources"]["users"]["/users/lookup"]["remaining"])
        else:
            wait_time = int(resp["resources"]["followers"]["/followers/ids"]["remaining"])
        print("waiting for {0} seconds".format(wait_time))
        time.sleep(wait_time)


    def sleep_for_necessary_time(self, protocol):
        """
        Calculate the necessary time we need to wait so that 
        we don't go over the rate limit over at twitter. Yay.
        
        So. If we're allowed to do 180 calls per 15 minutes,
        we're allowed to do 12 call per minute. That means
        there should be 60/12=5 seconds between each subsequent call.

        """
        time_since_last = time.time() - self.last_call[protocol]
        rqs_per_minute = (self.limits[protocol] / 15)
        wait_time = (60/rqs_per_minute) - time_since_last + 1 # padding
        if wait_time > 0:
            print("waiting for {0} seconds".format(wait_time))
            time.sleep(wait_time)

        self.last_call[protocol] = time.time()

    def get_users(self, uids=[], names=[]):
        """
        Get the users specified in uids or names. The users/lookup
        method of the twitterAPI allows the retrieval of 100 uids each time.
        This method returns a generator.

        """
        users = []
        while names or uids:
            self.sleep_for_necessary_time("users")
            name_slice, names = self.split_list(names, 100)
            uids_slice, uids = self.split_list(uids, 100)
            params = {"screen_name": name_slice, "user_id": uids_slice}
            rq = self.api.request("users/lookup", params)
            for item in rq.get_iterator():
                yield item

    def get_user(self, uid=None, name=None):
        """Get a single user from Twitter and retun its json formatted text"""
        self.sleep_for_necessary_time("users")
        rq = self.api.request("users/show", {"screen_name": name, "user_id": uid})
        return rq.text
        

    def get_followers(self, uid=None, name=None):
        """
        Returns a list of followers for a give uid.

        Args:
            uid: the user id for the twitter user. Defaults to None
            name: the screen name of the twitter user. Defaults to None.

        Yields:
            The ids of all the followers of the specified twitter user.

        """
        if uid:
            print("Getting followers of %i" % uid)
        params = {"screen_name": name, "user_id": uid,
                  "count": "5000"}
        cursor = -1
        while cursor != 0:
            self.sleep_for_necessary_time("followers")
            params["cursor"] = cursor
            rq = self.api.request("followers/ids", params)
            res = json.loads(rq.text)
            try:
                cursor = res["next_cursor"]
            except KeyError:
                print res
                cursor = 0
            for id in res["ids"]:
                yield id

    def raw_to_dict(self, raw_user):
        user = {}
        for field in self.config.user_fields:
            if field in raw_user:
                if field == "created_at":
                    date = parse(raw_user[field])
                    user["created_at"] = date.isoformat()
                else:
                    user[field] = raw_user[field]
        return user

    @staticmethod
    def split_list(ls, idx):
        """ Split the list ls on the idx. """
        l1 = []
        l2 = []
        for i, item in enumerate(ls):
            if i < idx:
                l1.append(item)
            else:
                l2.append(item)
        return l1, l2
