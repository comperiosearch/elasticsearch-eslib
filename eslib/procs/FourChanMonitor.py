__author__ = 'Hans Terje Bakke'

#region 4chan client

import requests
import email.utils
import logging

class FourChanClient(object):

    url = "http://a.4cdn.org"

    def __init__(self, logger=None):
        if logger:
            self.log = logger
        else:
            self.log = logging.getLogger("4chan")

    def get_boards(self, board_ids=None):
        "Returns list of board objects."
        res = requests.get("%s/boards.json" % self.url).json()["boards"]
        boards = []
        for board in res:
            if not board_ids or (board["board"] in board_ids):
                boards.append(board)
        return boards

    def get_archived_thread_ids(self, board_id):
        "Returns list of thread ids."
        res = requests.get("%s/%s/archive.json" % (self.url, board_id))
        if res.status_code != 200:
            self.log.debug("Call to get archived threads for board '%s' returned status code = %d" % (board_id, res.status_code))
            #res.raise_for_status()
            return []
        return res.json()

    def get_thread_ids(self, board_id, after=0):
        "Returns list thread ids."

        # This did not work...
        headers = {"If-Modified-Since": email.utils.formatdate(after, usegmt=True)}

        res = requests.get("%s/%s/threads.json" % (self.url, board_id), headers=headers)
        if res.status_code != 200:
            self.log.debug("Call to get thread ids for board '%s' returned status code = %d" % (board_id, res.status_code))
            #res.raise_for_status()
            return []

        thread_ids = []
        for page in res.json():
            for thread in page["threads"]:
                if thread["last_modified"] > after:
                    thread_ids.append(thread["no"])
        return thread_ids

    def get_posts(self, board_id, thread_number, after=0):
        res = requests.get("%s/%s/thread/%d.json" % (self.url, board_id, thread_number))
        if res.status_code != 200:
            self.log.debug("4chan API call to get posts for thread '%d' returned status code = %d" % (thread_number, res.status_code))
            #res.raise_for_status()
            return []

        posts = []
        for post in res.json()["posts"]:
            if post["time"] > after:
                posts.append(post)
        return posts

#endregion 4chan client

#region 4chan monitor

from ..Monitor import Monitor
import time

class FourChanMonitor(Monitor):
    """
    Monitor 4chan image boards.

    NOTE: Although 4chan is an image board, we do not (yet) download the images themselves (i.e. the binary data).

    Protocols:

        esdoc.4chan:

            _id                int   # Post number at 4chan
            _type              str   # "4chan"
            _source
                id             int   # Post number at 4chan
                board          str   # Board id
                thread         int   # Thread id
                timestamp      int   # Time of posting
                author         str   # Name of author, most commonly "Anonymous"
                comment        str   # Text comment
                filename       str   # Filename, with extension
                response_to    int   # Post number this post is a response to. 0 if original posting (i.e. not a response)

    Sockets:
        esdoc  (esdoc.4chan) (default)    : Simplified 4chan document, with board and thread info.
        raw    (*)                        : The 4chan document in raw format, exactly as returned by the 4chan API.

    Config:
        boards             = [],    : Which boards to monitor; empty list means 'all'
        request_delay      = 1.0,   : Seconds between API calls
        poll_frequency     = 20.0,  : Seconds between checks for new posts
        start_from         = None   : Start timestamp (time.time()). If None, it will start from whenever the proc is FIRST started. 0 is from epoc.
    """

    def __init__(self, **kwargs):
        super(FourChanMonitor, self).__init__(**kwargs)

        self._output = self.create_socket("esdoc", "esdoc.4chan", "Simplified 4chan documents.", is_default=True)
        self._output_raw = self.create_socket("raw", "esdoc", "Simplified 4chan documents.")

        self.config.set_default(
            boards         = [],    # Which boards to monitor; empty list means 'all'
            request_delay  = 1.0,   # Seconds between API calls
            poll_frequency = 20.0,  # Seconds between checks for new posts
            start_from     = None   # Start timestamp (time.time()). If None, it will start from whenever the proc is FIRST started. 0 is from epoc.
        )

        # TODO: This ought to be stored somewhere:
        self._last_poll_time = 0.0  # When we last started working on an iteration
        self._board_poll_times = {}
        self._thread_poll_times = {}

        self._board_ids = []
        self._thread_ids = []
        self._board_ptr = 0
        self._thread_ptr = 0
        self._closed_thread_ids = {}

        self._last_request = 0.0  # Used by _nice()
        self._poll_start_time = 0.0

        self._client = FourChanClient(logger=self.log)

    def _nice(self):
        "Call this before each call to 4chan to ensure a 'request_delay' has passed since the last call was made."
        remaining = max(0.0, self.config.request_delay - (time.time() - self._last_request))
        if remaining > 0.0:
            time.sleep(remaining)
        self._last_request = time.time()

    def on_open(self):
        # TODO: LOAD _last_poll_time, _board_poll_times AND _thread_poll_times FROM SOMEWHERE

        # Get all boards ids if none specified
        if self.config.boards:
            self._board_ids = self.config.boards[:]
        else:
            self._nice()
            self._board_ids = [board["board"] for board in self._client.get_boards()]
        self.log.info("Using boards: %s" % ", ".join(self._board_ids))

        if not self._last_poll_time:
            if self.config.start_from is None:
                self._last_poll_time = time.time()
            else:
                self._last_poll_time = self.config.start_from
        self.log.info("Starting with last poll timestamp = %d (now = %d)" % (self._last_poll_time, time.time()))

        self._working = False

    def on_close(self):
        # TODO: SAVE last_poll_time, _board_poll_times AND _thread_poll_times SOMEWHERE
        self._working = False

    def on_tick(self):

        # Start working if we are not working already...
        if not self._working:
            now = time.time()
            if now - self._last_poll_time > self.config.poll_frequency:
                # It is time to start working again...
                self._poll_start_time = now
                self._working = True
                self._board_ptr  = -1
                self._thread_ptr = 0
                self._thread_ids = []
                self.log.debug("Starting new iteration.")
            return

        # Do work here...

        if self._thread_ptr < len(self._thread_ids):
            # Get modified posts for this thread
            board_id = self._board_ids[self._board_ptr]
            thread_id = self._thread_ids[self._thread_ptr]
            last_poll = self._thread_poll_times.get(thread_id) or self._last_poll_time
            self._nice()
            self._thread_poll_times[thread_id] = time.time()
            posts = self._client.get_posts(board_id, thread_id, 0)
            modified_posts = [p for p in posts if p["time"] > last_poll]
            self.log.debug("Found %d/%d new posts for board '%s', thread '%d'." % (len(modified_posts), len(posts), board_id, thread_id))

            is_closed = (True if posts and posts[0].get("closed") == 1 else False)
            if is_closed:
                self.log.debug("Thread '%d' was closed; we never want to see it again." % thread_id)
                self._closed_thread_ids[thread_id] = True
                del self._thread_poll_times[thread_id]

            for post in modified_posts:
                self._output_raw.send(post)
                if self._output.has_output:  # No point in creating simplified doc unless there is a listener
                    # Create new post object
                    doc = {
                        "_id"  : post["no"],
                        "_type": "4chan",
                        "_source": {
                            "id"         : post["no"],
                            "board"      : board_id,
                            "thread"     : thread_id,
                            "timestamp"  : post["time"],
                            "author"     : post["name"],
                            "comment"    : post.get("com"),
                            "filename"   : None if not "filename" in post else ("%s%s" % (post["filename"], post["ext"])),
                            "response_to": post.get("resto")
                            # Skipping spoilers, image dimensions, and more..
                        }
                    }
                    # Send new post to socket
                    self._output.send(doc)

            # Now move on to the next thread and let the next tick work on it...
            self._thread_ptr += 1
            return

        # Move on to next board if there are any left, otherwise finish...

        self._board_ptr += 1
        self._thread_ptr = 0

        if self._board_ptr >= len(self._board_ids):
            self._last_poll_time = self._poll_start_time
            self._working = False
            self.log.debug("Finished iterating boards.")
            return
        else:
            # Retrieve list of modified threads for board
            board_id = self._board_ids[self._board_ptr]
            self.log.trace("Moving on to next board, '%s'." % board_id)
            last_poll = (self._board_poll_times.get(board_id) or self._last_poll_time)
            self._nice()
            self._board_poll_times[board_id] = time.time()
            ids = self._client.get_thread_ids(board_id, last_poll)
            self._thread_ids = [i for i in ids if not i in self._closed_thread_ids]
            self._thread_ptr = 0
            self.log.debug("Board '%s' has '%d' modified threads." % (board_id, len(self._thread_ids)))
            # Now let the next tick fetch the posts...
            return

#endregion 4chan monitor
