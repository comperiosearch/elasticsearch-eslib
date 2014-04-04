# ============================================================================
# Base class for pipeline stages.
# ============================================================================

import sys, signal, logging


class PipelineStage(object):
    "Base class for pipeline stage."

    def __init__(self, name):
        self.name = name

        self.DEBUG       = False
        self.VERBOSE     = False
        self.failOnError = False # Whether a processing error should raise and terminate the process or continue
        self.terminal    = False # True if this is the last stage and should not produce any more output

        self.abort_request = False
        logging.basicConfig(level=logging.WARNING)
        self.log = logging.getLogger(__name__ + "." + self.__class__.__name__)

    # Implemented by inheriting classes:

    def configure(self, config=None):
        pass

    def load(self):
        pass

    def start(self):
        pass

    def process(self, doc):
        return doc # Pure passthrough by default

    def finish(self):
        pass

    def write(self, text):
        if not self.terminal:
            print(text, file=sys.stdout)


    def convert(self, line):
        line = line.strip()
        if line and not line.startswith("#"):
            return line

        
    def read(self, filenames=None):
        """Generator that reads lines from from file or stdin and yields them."""

        count = 0
        if not filenames: filenames = ["-"]
        for filename in filenames:
            input = None
            if filename == "-":
                if self.VERBOSE: self.vout("Reading from stdin...")
                input = sys.stdin
            else:
                if self.VERBOSE: self.vout("Reading from file '%s'..." % filename)
                input = open(filename, "rt")

            # Read lines from stream
            for line in input:
                converted = self.convert(line.rstrip("\r\n"))
                if not converted == None:
                    yield converted
                    count += 1

            if not input == sys.stdin:
                input.close()

        if self.VERBOSE: self.vout("All files read. Total items = %d" % count)


    def _keyboard_interrupt_handler(self, signal, frame):
        if not self.abort_request:
            # A soft request, so one may insist by interrupting once more
            self.abort_request = True
        else:
            # This is the second request, now die hard
            text = "Abort insisted. Aborting immediately."
            print("%s: %s" % (self.name, text), file=sys.stderr)
            raise KeyboardInterrupt

    def run(self, filenames=None):
        """Method used when running the class as a separate script"""

        self.abort_request = False
        signal.signal(signal.SIGINT, self._keyboard_interrupt_handler)

        try:
            self.configure()
            self.load()
            self.start()
            for doc in self.read(filenames):
                processed = self.process(doc)
                if processed: self.write(processed)
            # In case there is unfinished business...
            self.finish()
        except KeyboardInterrupt:
            # Only follow-up keyboard interrupts (SIGINT) will get here
            pass
        except BrokenPipeError:
            print("%s: BrokenPipeError" % self.name, file=sys.stderr)


    # Output

    def dout(self, text):
        if self.DEBUG: print("%s: %s" % (self.name, text), file=sys.stderr)

    def dout_raw(self, obj):
        if self.DEBUG: print(obj, file=sys.stderr)

    def vout(self, text):
        if self.VERBOSE: print("%s: %s" % (self.name, text), file=sys.stderr)

    def eout(self, text=None, exception=None):
        if not text and exception: text = exception.args[0]
        if not text: text = "???"
        print("ERROR, %s: %s" % (self.name, text), file=sys.stderr)
        if self.failOnError and exception:
            raise exception

    def report_soft_abort(self):
        if self.abort_request:
            text = "Abort requested. Terminating softly."
            print("%s: %s" % (self.name, text), file=sys.stderr)
            return True
        return False
