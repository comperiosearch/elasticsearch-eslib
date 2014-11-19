__author__ = 'Eivind Eidheim Elseth'
import time
import subprocess

from ..Monitor import Monitor
import logging

class CLIReader(Monitor):
    """
    The CLIReader is a Generator that will periodically call a command line utility

    Sockets:
        stdout     (str)   (default)   : Output from the command line utility's stdout
        stderr     (str)               : Output from the command line utility's stderr

    Config:
        cmd             = None   : The command to run
        interval        = 10     : The waiting period in seconds between each time the command is run

    """

    def __init__(self, **kwargs):
        super(CLIReader, self).__init__(**kwargs)
        self._stdout = self.create_socket("stdout", "str", "The output to stdout from the command line utility", is_default=True)
        self._stderr = self.create_socket("stderr", "str", "The output to stderr from the command line utility")
        self.config.set_default(
            interval = 10
        )
        self.last_get = None

    def on_tick(self):
        if not self.last_get or (time.time() - self.last_get  > self.config.interval):
            # Since the next call may crash, at least mark the last attempt as now,
            # so we don't try again on every tick, but wait for the next interval.
            self.last_get = time.time()

            p = subprocess.Popen(self.config.cmd, shell=False, stdout=subprocess.PIPE)
            p.wait()
            (output, err) = p.communicate()
            if output:
                if self.doclog.isEnabledFor(logging.TRACE):
                    self.doclog.trace("Output doc: %s" % str(output))
                self._stdout.send(output)
            if err:
                self.log.error("Received message from subprocess on stderr: %s" % str(err))
                self._stderr.send(err)

            self.last_get = time.time()
