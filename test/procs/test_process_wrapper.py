# -*- coding: utf-8 -*-

import unittest
from eslib.procs import ProcessWrapper, FileWriter
from time import sleep

import logging
LOG_FORMAT = ('%(levelname) -10s %(name) -55s %(funcName) -30s %(lineno) -5d: %(message)s')
logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)

class TestProtocolWrapper(unittest.TestCase):

    def test_func_one_lambda(self):

        p = ProcessWrapper(command=["./wrapped_process.py"], raw_output=True)
        w = FileWriter()
        w.subscribe(p)

        p.start()
        p.put("a")
        sleep(1)
        p.put("b")
        sleep(1)
        p.put("c")
        sleep(1)
        p.put("d")

        try:
            w.wait()
        except KeyboardInterrupt:
            p.stop()
            w.wait()

def main():
    unittest.main()

if __name__ == "__main__":
    main()

# TODO: TEST SIGHUP
# TODO: TEST SIGKILL
# TODO: TEST EXCEPTION INSIDE PROC
# TODO: TEST PROC ENDING
# TODO: TEST WRAPPER ERROR ALSO SHUTTING FOWN PROC PROPERLY
