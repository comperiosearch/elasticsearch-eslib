__author__ = 'Eivind Eidheim Elseth'
import time
import subprocess

from ..Generator import Generator


class CLIReader(Generator):
    """
    The purpose of this processor is to
    """


    def __init__(self, **kwargs):
        super(CLIReader, self).__init__(**kwargs)
        self._output = self.create_socket("ids", "str", "ids of documents")
        self.config.set_default(
            interval = 10
        )
        self._queue = []
        self.last_get = None

        self.has_properties = set([])

    def on_tick(self):
        """
        Commit items in queue if queue exceeds batchsize or it's been long
        since last commit.
        """
        if not self.last_get or (time.time() - self.last_get  > self.config.interval):
            p = subprocess.Popen(self.cmd, shell=False, stdout=subprocess.PIPE)
            p.wait()
            (output, err) = p.communicate()
            self._output.send(output)
            self.last_get = time.time()
