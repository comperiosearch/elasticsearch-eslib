__author__ = 'mats'


import unittest
import requests
import time
import itertools
from mock import Mock
from dateutil.parser import parse
from eslib.procs import TwitterUserGetter, TwitterFollowerGetter
from eslib import Twitter, Config


MOCK_RESPONSE = {'lang': u'en',
                 'description': u'My views do not reflect the views of my employer',
                 'created_at': '2014-01-31T02:01:01+00:00',
                 'name': u'Adam Schweinsteiger',
                 'location': u'Oslo',
                 'id': 2320059392,
                 'screen_name': u'scwheinxxx',
                 'ass': u'True',
                 'norwegian': u'False'}

WANTED_FIELDS = {"id", "location", "description", "screen_name",
                 "lang", "name", "created_at"}

DATE_PARSED = parse(MOCK_RESPONSE['created_at']).isoformat()

MOCK_RESPONSE_IDS = [123123, "12312312a", "5421123", "1243 123", 12312312]
VALID_IDS = 3
ORIGIN_ID = 9999
OUTGOING = True

ENDPOINTS = {
             "application/rate_limit_status",
             "users/lookup",
             "friends/ids",
             "followers/ids"
            }


RATE_LIMIT = 5
WAIT_TIME = 15
RATE_LIMITS = {"resources":
                {
                  "friends": {"/friends/ids": {"limit": RATE_LIMIT, "remaining": WAIT_TIME}},
                  "followers": {"/followers/ids": {"limit": RATE_LIMIT, "remaining": WAIT_TIME}},
                  "users": {"/users/lookup": {"limit": RATE_LIMIT, "remaining": WAIT_TIME}}
                }
              }

SCREEN_NAMES = {"einar", "per"}
USER_IDS = {"42112", "323123"}

user_results = []
def get_users(x):
    user_results.append(x)

follower_results = []
def get_followers(x):
    follower_results.append(x)


class TestTwitterGetters(unittest.TestCase):

    def test_simple(self):

        twitter = Twitter.__new__(Twitter)
        twitter.config = Config()
        twitter.config.set_default(
            user_fields=WANTED_FIELDS
        )
        user_getter = TwitterUserGetter(twitter=twitter)
        user_getter.add_callback(get_users, "user")

        follower_getter = TwitterFollowerGetter(twitter=twitter,
                                                outgoing=OUTGOING)
        follower_getter.add_callback(get_followers, "edge")

        twitter.get_users = Mock(return_value=[MOCK_RESPONSE]*VALID_IDS)
        twitter.get_follows = Mock(return_value=MOCK_RESPONSE_IDS)

        # Test User Getters Internals
        user_getter._incoming(12312312)
        user_getter.get()

        #wait for processing to finish
        while not (len(user_results) == VALID_IDS):
            pass

        self.assertTrue(len(user_results) == VALID_IDS)
        user = user_results[0]
        self.assertTrue(user['lang'] == MOCK_RESPONSE['lang'])
        self.assertTrue(user['description'] == MOCK_RESPONSE['description'])
        self.assertTrue(user['name'] == MOCK_RESPONSE['name'])
        self.assertTrue(user['location'] == MOCK_RESPONSE['location'])
        self.assertTrue(user['id'] == MOCK_RESPONSE['id'])
        self.assertTrue(user['screen_name'] == MOCK_RESPONSE['screen_name'])
        self.assertTrue(user['created_at'] == DATE_PARSED)
        self.assertTrue('ass' not in user)
        self.assertTrue('norwegian' not in user)

        # Test Follower Getters Internals
        follower_getter._incoming(ORIGIN_ID)

        #Wait for processing to finish
        while not (len(follower_results) == len(MOCK_RESPONSE_IDS)):
            pass

        self.assertTrue(len(follower_results) == len(MOCK_RESPONSE_IDS))
        for edge in follower_results:
            if OUTGOING:
                self.assertTrue(edge["from"] == ORIGIN_ID)
                self.assertTrue(edge["to"] in MOCK_RESPONSE_IDS)
            else:
                self.assertTrue(edge["to"] == ORIGIN_ID)
                self.assertTrue(edge["from"] in MOCK_RESPONSE_IDS)
            self.assertTrue(edge["type"] == follower_getter.config.reltype)



class TestTwitterAPIWrapper(unittest.TestCase):

    def setUp(self):
        self.twitter = Twitter.__new__(Twitter)
        self.twitter.api = Mock()
        self.twitter.api.request = mock_request
        self.twitter.sleep_for_necessary_time = Mock()
        self.twitter.config = Config()
        self.twitter.config.set_default(
            user_fields={"id", "location", "description", "screen_name",
                         "lang", "name", "created_at"},
            batchsize=100,
            max_reconnect_wait=30*60,  # 30 minutes
            start_reconnect_wait=2  # 2 seconds
        )

    def test_rate_limit(self):
        self.twitter.set_rate_limits()

        # Test that rate limits are set
        for key, val in self.twitter.limits.iteritems():
            self.assertTrue(val == RATE_LIMIT)

        # Test that we are calling time.sleep with the right argument
        time.sleep = Mock()
        self.twitter.blew_rate_limit("users")
        time.sleep.assert_called_with(WAIT_TIME)

    def test_get_users(self):

        #Test with some existing uids and names
        users = self.twitter.get_users(uids=[next(iter(USER_IDS))],
                                       names=[next(iter(SCREEN_NAMES))])

        for i, user in enumerate(users):
            self.assertTrue(user == MOCK_RESPONSE)
        self.assertTrue(i == 1)

        # Test with a nonexistent uid
        new_users = self.twitter.get_users(uids=["mewwts22222"])
        self.assertTrue(list(new_users) == [])

    def test_get_user_HTTPError(self):
        def code_rq(endpoint, params):
            return mock_request_error(code, endpoint, params)

        #These should just return empty responses
        self.twitter.api.request = code_rq
        for code in [304, 403]:
            #doesn't matter what the uid is here
            users = list(self.twitter.get_users(uids=[123123]))
            self.assertTrue(users == [])

        # This should raise an exception
        for code in [400]:
            with self.assertRaises(requests.exceptions.HTTPError):
                list(self.twitter.get_users(uids=[123123]))

        mock_retry = Mock()
        mock_resp = Mock()
        mock_resp.get_iterator.return_value = [MOCK_RESPONSE]
        mock_retry.return_value = mock_resp
        self.twitter._retry = mock_retry
        for code in [500]:
            # we get a single  user as response
            self.assertTrue(len(list(self.twitter.get_users(uids=[23123123]))) == 1)
            # the retry method is called
            self.assertTrue(mock_retry.called)


def mock_request_error(error_code, endpoint, params=None):
    resp = Mock()
    resp.get_iterator.return_value = []
    resp.response.status_code = error_code
    resp.response.raise_for_status.side_effect = requests.exceptions.HTTPError
    return resp


def mock_request(endpoint, params=None):
    resp = Mock()
    if endpoint == "application/rate_limit_status":
        resp.response.raise_for_status.return_value = None
        resp.response.json.return_value = RATE_LIMITS

    elif endpoint == "users/lookup":
        if ("screen_name" in params and params["screen_name"]) or \
                ("user_id" in params and params["user_id"]):
            vals = []
            resp.response.raise_for_status.return_value = None
            for thing in itertools.chain(params["screen_name"], params["user_id"]):
                if thing in (SCREEN_NAMES | USER_IDS):
                    vals.append(MOCK_RESPONSE)
            resp.get_iterator.return_value = vals
            if not vals:
                resp.response.raise_for_status.side_effect = requests.exceptions.HTTPError
                resp.response.status_code = 404

    return resp

if __name__ == "__main__":
    unittest.main()
