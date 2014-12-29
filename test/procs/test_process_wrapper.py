# -*- coding: utf-8 -*-

import unittest
from eslib.procs import ProcessWrapper, FileWriter
from time import sleep
import signal

import logging
LOG_FORMAT = '%(levelname) -10s %(className) -15s %(instanceName) -15s %(funcName) -15s %(lineno) -5d: %(message)s'
logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)

class TestProtocolWrapper(unittest.TestCase):

    def test_subprocess_completes(self):

        p = ProcessWrapper(command=["./wrapped_process.py"])
        w = FileWriter()
        w.subscribe(p)
        output = []
        p.add_callback(lambda doc: output.append(doc))

        print "outer/starting"

        p.start()
        p.put("a")
        sleep(1)
        p.put("b")
        sleep(1)
        p.put("*HANGUP*")

        try:
            w.wait()
        except KeyboardInterrupt:
            p.stop()
            w.wait()

        print "outer/done"

        self.assertEqual(len(output), 6)
        self.assertEqual(output[-2], "INNER/HANGING UP ON *HANGUP* REQUEST")
        self.assertEqual(output[-1], "INNER/EXITING")

    def test_subprocess_exception(self):

        p = ProcessWrapper(command=["./wrapped_process.py"])
        w = FileWriter()
        w.subscribe(p)
        output = []
        p.add_callback(lambda doc: output.append(doc))

        print "outer/starting"

        p.start()
        p.put("a")
        sleep(1)
        p.put("b")
        sleep(1)
        p.put("*RAISE*")

        try:
            w.wait()
        except KeyboardInterrupt:
            p.stop()
            w.wait()

        print "outer/done"

        self.assertEqual(len(output), 4)
        self.assertEqual(output[-1], "INNER/ECHO: *RAISE*")

    def test_subprocess_stop(self):

        p = ProcessWrapper(command=["./wrapped_process.py"])
        w = FileWriter()
        w.subscribe(p)
        output = []
        p.add_callback(lambda doc: output.append(doc))

        print "outer/starting"

        p.start()
        p.put("a")
        sleep(1)
        p.put("b")

        p.stop()
        w.wait()

        print "outer/done"

        self.assertEqual(len(output), 5)
        self.assertEqual(output[-2], "INNER/STDIN WAS HUNG UP -- GOOD BYE")
        self.assertEqual(output[-1], "INNER/EXITING")

    def test_subprocess_terminate(self):

        p = ProcessWrapper(command=["./wrapped_process.py"])
        w = FileWriter()
        w.subscribe(p)
        output = []
        p.add_callback(lambda doc: output.append(doc))

        print "outer/starting"

        p.start()
        p.put("a")
        sleep(1)
        p.put("b")

        p.send_signal(signal.SIGTERM)
        w.wait()

        print "outer/done"

        self.assertEqual(len(output), 4)  # 'b' did not have time to be echoed.
        self.assertEqual(output[-2], "INNER/RECEIVED SIGTERM -- terminating")
        self.assertEqual(output[-1], "INNER/EXITING")

    def _sighup_handler(self, signal, frame):
        self.sighup = True
        print "***SIGHUP"

    def test_subprocess_backfire(self):

        self.sighup = False
        signal.signal(signal.SIGHUP, self._sighup_handler)

        p = ProcessWrapper(command=["./wrapped_process.py"])
        w = FileWriter()
        w.subscribe(p)
        output = []
        p.add_callback(lambda doc: output.append(doc))

        print "outer/starting"

        p.start()
        p.put("a")
        sleep(1)
        p.put("b")

        p.send_signal(signal.SIGHUP)

        p.put("c")
        sleep(1)
        p.stop()
        w.wait()

        print "outer/done"

        self.assertEqual(len(output), 7)
        self.assertEqual(output[ 2], "INNER/RECEIVED SIGHUP -- ignoring")
        self.assertEqual(output[-2], "INNER/STDIN WAS HUNG UP -- GOOD BYE")
        self.assertEqual(output[-1], "INNER/EXITING")
        self.assertFalse(self.sighup)  # MEANING signal did not backfire

    def test_subprocess_abort(self):

        p = ProcessWrapper(command=["./wrapped_process.py"])
        w = FileWriter()
        w.subscribe(p)
        output = []
        p.add_callback(lambda doc: output.append(doc))

        print "outer/starting"

        p.start()
        p.put("a")
        sleep(1)
        p.put("b")

        p.abort()
        w.wait()

        print "outer/done"

        self.assertEqual(len(output), 2)
        self.assertEqual(output[-1], "INNER/ECHO: a")  # There should have been no time to receive 'b'

    def test_subprocess_unicode(self):

        ustr = u"Ære være løven Åge!"

        p = ProcessWrapper(command=["./wrapped_process.py"])
        w = FileWriter()
        w.subscribe(p)
        output = []
        p.add_callback(lambda doc: output.append(doc))

        print "outer/starting"

        p.start()
        p.put("a")
        sleep(1)
        p.put(ustr)
        sleep(1)
        p.put("*HANGUP*")

        try:
            w.wait()
        except KeyboardInterrupt:
            p.stop()
            w.wait()

        print "outer/done"

        self.assertEqual(len(output), 6)
        self.assertEqual(output[-4], "INNER/ECHO: " + ustr)
        self.assertEqual(output[-2], "INNER/HANGING UP ON *HANGUP* REQUEST")
        self.assertEqual(output[-1], "INNER/EXITING")

    def test_subprocess_command_with_args(self):

        p = ProcessWrapper(command=["./wrapped_process_cmd.py", u"Adne", u"Vannø"])
        w = FileWriter()
        w.subscribe(p)
        output = []
        p.add_callback(lambda doc: output.append(doc))

        print "outer/starting"

        p.start()
        w.wait()

        print "outer/done"

        self.assertEqual(len(output), 3)
        self.assertEqual(output[-2], u"INNER/Adne Vannø")
        self.assertEqual(output[-1], "INNER/EXITING")

    def test_subprocess_json_io(self):

        p = ProcessWrapper(
            command=["./wrapped_process_json.py"],
            deserialize=True)
        w = FileWriter()
        w.subscribe(p)
        output = []
        p.add_callback(lambda doc: output.append(doc))

        print "outer/starting"

        import json

        p.start()
        p.put(json.dumps({"outer": "word!"}))
        sleep(1)
        p.put(json.dumps({"outer": "silence"}))
        #sleep(1)
        p.stop()
        w.wait()

        print "outer/done"

        self.assertEqual(len(output), 4)
        #self.assertEqual(output[-3].get("inner"), "echo: silence")
        self.assertEqual(output[-2].get("inner"), "stdin was hung up")
        self.assertEqual(output[-1].get("inner"), "finished")

def main():
    unittest.main()

if __name__ == "__main__":
    main()

