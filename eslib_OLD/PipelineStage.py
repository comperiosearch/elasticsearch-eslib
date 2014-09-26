# ============================================================================
# Base class for pipeline stages.
# ============================================================================

import sys, signal
import logging
import eslib.prog


class PipelineStage(object):
    "Base class for pipeline stage."

    def __init__(self, name):
        self.name = name

        self.debuglevel  = -1 # Off
        self.failOnError = False # Whether a processing error should raise and terminate the process or continue
        self.terminal    = False # True if this is the last stage and should not produce any more output

        self.abort_request = False

        parts = []
        if not self.__module__ == "__main__": parts.append(self.__module__)
        className = self.__class__.__name__
        parts.append(className)
        if name:
            if name.endswith(".py"):
                name = name[:-3]
            if not name == className: parts.append(name)
        fullPath = ".".join(parts)

        #print("FULL=[%s]" % fullPath, file=sys.stderr)

        self._doclog = logging.getLogger("doclog.%s"  % fullPath)
        self.log     = logging.getLogger("proclog.%s" % fullPath)


    # Implemented by inheriting classes:

    def configure(self, config=None):
        pass

    def load(self):
        pass

    def start(self):
        pass

    def process(self, doc):
        yield doc # Pure passthrough by default

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
                input = sys.stdin
            else:
                input = open(filename, "rt")

            # Read lines from stream
            for line in input:
                converted = self.convert(line.rstrip("\r\n"))
                if not converted == None:
                    yield converted
                    count += 1

            if not input == sys.stdin:
                input.close()


    def print(self, text):
        self.log.info(text)


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
            eslib.prog.initlogs()
            self.configure()
            self.load()
            self.start()
            for doc in self.read(filenames):
                for processed in self.process(doc):
                    if processed: self.write(processed)
            # In case there is unfinished business...
            self.finish()
        except KeyboardInterrupt:
            # Only follow-up keyboard interrupts (SIGINT) will get here
            pass
        except BrokenPipeError:
            print("%s: BrokenPipeError" % self.name, file=sys.stderr)


    # Output

    def error(self, text=None, exception=None):
        if not text and exception: text = exception.args[0]
        if not text: text = "???"
        #self.console.error(text)
        self.log.error(text)
        if self.failOnError and exception:
            raise exception

    def report_soft_abort(self):
        if self.abort_request:
            text = "Abort requested. Terminating softly."
            print("%s: %s" % (self.name, text), file=sys.stderr)
            return True
        return False
