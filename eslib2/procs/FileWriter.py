from ..Processor import Processor
import sys, json

class Config:
    pass

class FileWriter(Processor):
    """
    Write incoming documents to specified file or standard output.
    Documents of dict type are written as json documents, per line. Other types are written directly with
    their string representation.
    """
    def __init__(self, name=None):
        super(FileWriter, self).__init__(name)
        self.create_connector(self.incoming, "input", None, "Incoming documents to write to file as string or json objects per line.")

        self.config.filename = None
        self.config.append = False # Whether to append to existing file, rather than overwrite

        self.file = None

    def on_startup(self):

        if self.file:
            print "*** FileWriter MULTIPLE STARTUP" # DEBUG
            return

        if not self.config.filename:
            # Assuming stdout
            self.file = sys.stdout
        else:
            # May raise exception:
            self.file = open(self.config.filename, "a" if self.config.append else "w")

    def on_close(self):
        if self.file and self.file != sys.stdout:
            self.file.close()
        self.file = None

    def incoming(self, document):
        if document:
            if type(document) is dict:
                print >> self.file, json.dumps(document)
            else:
                print >> self.file, document
