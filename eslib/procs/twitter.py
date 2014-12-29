from __future__ import absolute_import
from ..Configurable import Configurable
from TwitterAPI import TwitterAPI, TwitterResponse
from dateutil.parser import parse

import requests
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
                         "lang", "name", "created_at"},
            batchsize=100,
            max_reconnect_wait=30*60,  # 30 minutes
            start_reconnect_wait=2  # 2 seconds
        )

    def set_rate_limits(self):
        """ 
        Connects to twitter and asks for the number of requests allowed
        per 15 minutes.
        Results are put in the self.limits dict.

        """

        resp = self.api.request("application/rate_limit_status")
        resp.response.raise_for_status()
        parsed = resp.response.json()

        user_lim = parsed["resources"]["users"]["/users/lookup"]["limit"]
        flw_lim = parsed["resources"]["followers"]["/followers/ids"]["limit"]
        fr_lim = parsed["resources"]["friends"]["/friends/ids"]["limit"]

        self.last_call = {"users": 0,
                          "followers": 0,
                          "friends": 0}
        self.limits = {"users": int(user_lim),
                       "followers": int(flw_lim),
                       "friends": int(fr_lim)}

    def blew_rate_limit(self, protocol):

        raw = self.api.request("application/rate_limit_status",
                                     {"resources": protocol})
        resp = raw.response.json()
        if protocol == "users":
            wait_time = int(resp["resources"][protocol]["/users/lookup"]["remaining"])
        else:
            wait_time = int(resp["resources"][protocol]["/%s/ids" % protocol]["remaining"])

        #TODO: There should be loggin here in to future
        # print("waiting for {0} seconds".format(wait_time))
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
            #print("waiting for {0} seconds".format(wait_time))
            time.sleep(wait_time)

        self.last_call[protocol] = time.time()

    def get_users(self, uids=[], names=[]):
        """
        Get the users specified in uids or names. The users/lookup
        method of the twitterAPI allows the retrieval of 100 uids each time.
        This method returns a generator.

        """
        protocol = "users/lookup"
        while names or uids:
            self.sleep_for_necessary_time("users")
            name_slice, names = self.split_list(names, self.config.batchsize)
            uids_slice, uids = self.split_list(uids, self.config.batchsize)
            params = {"screen_name": name_slice, "user_id": uids_slice}
            resp = None
            try:
                resp = self.api.request(protocol, params)
                resp.response.raise_for_status()
            except requests.exceptions.ConnectionError as ce:
                resp = self._handle_error(protocol, params, exception=ce)
            except requests.exceptions.HTTPError:
                resp = self._handle_error(protocol, params, resp=resp)

            for item in resp.get_iterator():
                yield item

    def get_user(self, uid=None, name=None):
        """Get a single user from Twitter and return its json formatted text"""
        self.sleep_for_necessary_time("users")
        rq = self.api.request("users/show", {"screen_name": name, "user_id": uid})
        return rq.text

    def _handle_error(self, protocol, params, resp=None, exception=None):
        """
        If the response does not have a 200 code, we handle the error here, and
        eventually return the expected response.


        :param TwitterAPI.TwitterResponse resp:
            the response corresponding to a non-200 http code.

        :param str protocol: A string representing the api protocol. e.g.
                         "users/lookup"
        :param dict params: A dictionary holding the params for the queries
        :raise requests.Exceptions.HTTPError: if bad request
        :return TwitterResponse: A twitterResponse object.

        """

        if exception is not None:
            return self._retry(protocol, params) or resp
        elif resp is not None:
            #Response should exist here
            code = resp.response.status_code
            if code == 304:
                # Not modified
                pass
            elif code == 401:
                # Unauthorized
                # Our credentials don't work. Did we just get banned?
                resp.response.raise_for_status()
            elif code == 400:
                # Bad Request
                resp.response.raise_for_status()
            elif code in {403, 404, 410}:
                # Forbidden, Not Found, Gone
                # Return the same response
                pass
            elif code == 429:
                # Too Many Requests
                # Here we ignore the fact that we may have been rate_limited again.
                self.blew_rate_limit(protocol.split("/")[0])
                resp = self.api.request(protocol, params)
            elif code in {500, 502, 503, 504}:
                # Internal errors at Twitter. Try again.
                resp = self._retry(protocol, params, resp) or resp
            else:
                #TODO: Log that we don't enter any of the above cases
                pass
            return resp
        else:
            print "We should never be here"
            return TwitterResponse(None, None)


    def _retry(self, protocol, params, resp=None):
        """
        Retry until we get a nice response.

        :param str protocol: A string represeting the api protocol.
        :param dict params: A dictionary holding the params for the query
        :param TwitterResponse resp: Optional. May be None.
        :raise: Exception: if we couldn't establish a connection.
        :return TwitterResponse:

        """
        wait = self.config.start_reconnect_wait
        while resp is None or resp.response.status_code in {500, 502, 503, 504}:
            if wait > self.config.max_reconnect_wait:
                raise Exception("To many reconnect attempts")
            time.sleep(wait)
            try:
                resp = self.api.request(protocol, params)
            except requests.exceptions.ConnectionError:
                resp = None
            wait *= 2
        return resp

    def get_follows(self, uid=None, name=None, outgoing=False):
        """
        Yields the followers or friends for a given uid.

        :param str uid: the user id for the twitter user. Defaults to None
        :param str name: the screen name of the twitter user. Defaults to None.
        :param str outgoing: Determines whether to get friends or followers
        :yield str ids: yields the ids of the followers or friends.

        """
        method = "friends" if outgoing else "followers"
        protocol = "{0}/ids".format(method)
        params = {"screen_name": name,
                  "user_id": uid,
                  "count": "5000"}

        cursor = -1
        while cursor != 0:
            self.sleep_for_necessary_time(method)
            params["cursor"] = cursor
            resp = None
            try:
                resp = self.api.request(protocol, params)
                resp.response.raise_for_status()
            except requests.exceptions.ConnectionError as ce:
                resp = self._handle_error(protocol, params, exception=ce)
            except requests.exceptions.HTTPError:
                resp = self._handle_error(protocol, params, resp=resp)

            res = resp.response.json()
            try:
                cursor = res["next_cursor"]
            except KeyError:
                cursor = 0
            if "ids" not in res:
                # Should log that something went wrong
                return
            for id_ in res["ids"]:
                yield id_

    def raw_to_dict(self, raw_user):
        """
        Get the relevant fields of raw_user. The field names resides in
        self.config.user_fields.

        :param raw_user: the dict holding the response from twitter
        :return user: the dict holding the relevant field of the response

        """
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
