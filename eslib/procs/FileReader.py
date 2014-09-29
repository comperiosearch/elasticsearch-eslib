__author__ = 'Hans Terje Bakke'

from ..Generator import Generator
import sys, os, os.path, errno
import json

# TODO: VERIFY ENCODING WORKING OK, ESPECIALLY WHEN READING FROM STDIN

class FileReader(Generator):
    """
    Read documents from specified files or standard input.
    Reads entire file as one document, or per line, according to config.
    Documents starting with '{' are considered JSON documents and converted to 'dict', unless otherwise configured.

    Sockets:
        output     (*)       : Documents read. Either entire file as one, or per line. Either raw string or dict.

    Config:
        filename          = None    : Appended to filenames, for simplicity.
        filenames         = None    : If not set then 'stdin' is assumed. Can take a list of files.
        document_pre_file = False   : Read each file as one string to be treated as one document.
        raw_lines         = False   : If the line starts with a '{' it is generally expected to be of JSON/dict format.
                                      Setting this to True treats the line as a string.
        strip_line        = True    : Whether to remove leading and trailing spaces on a line.
        skip_blank_line   = True    : Whether to skip empty lines (after stripping).
        skip_comment_line = True    : Whether to skip comment lines
        comment_prefix    = "#"     : Lines beginning with this string is considered to be a comment line if
                                      'skip_comment_line' is True.
    """

    def __init__(self, name=None):
        super(FileReader, self).__init__(name)
        self.output = self.create_socket("output", None, "Documents read. Either entire file as one, or per line. Either raw string or dict.")

        self.config.filename          = None
        self.config.filenames         = []
        self.config.document_per_file = False
        self.config.raw_lines         = True
        self.config.strip_line        = True
        self.config.skip_blank_line   = True
        self.config.skip_comment_line = True
        self.config.comment_prefix    = "#"

        self._filenames = []
        self._file = None
        self._filename_index = 0

    def on_open(self):

        if self._file:
            print "*** FileReader ATTEMPTED STARTUP/open WHEN _file WAS ALREADY SET" # DEBUG
            return

        # Create a more usable filenames array
        self._filenames = []
        if self.config.filename:
            self._filenames.append(self.config.filename)
        if not self.config.filenames:
            if not self.config.filename:
                self._filenames.append(None)  # stdin will be expected
        elif type(self.config.filenames) in [str, unicode]:
            self._filenames.append(self.config.filenames)
        else:
            self._filenames.extend(self.config.filenames)

        # Verify that files exists and that we can read them upon starting
        for filename in self._filenames:
            if filename:
                if not os.path.isfile(filename):
                    e = IOError("File not found: %s" % filename)
                    e.filename = filename
                    e.errno = errno.ENOENT  # No such file or directory
                    raise e
                elif not os.access(filename, os.R_OK):
                    e = IOError("Failed to read file: %s" % filename)
                    e.filename = filename
                    e.errno = errno.EACCES  # Permission denied
                    raise e

    def _close_file(self):
        if self._file and self._file != sys.stdin:
            self._file.close()
        self._file = None

    def on_close(self):
        # If we have an open file, this is our last chance to close it
        self._close_file()

    def _handle_data(self, incoming):
        data = incoming
        if data == None:
            return
        if self.config.strip_line:
            data = data.strip()
        if self.config.skip_comment_line and data.startswith(self.config.comment_prefix):
            return
        if self.config.skip_blank_line and not data:
            return
        if not self.config.raw_lines and data.startswith("{"):
            # NOTE: May raise ValueError:
            data = json.loads(data)
        self.output.send(data)


    def on_tick(self):

        if self._file:
            # We were working on a file... keep reading
            if self.config.document_per_file:
                all = self._file.read()
                self._handle_data(all)
                self._close_file()
            else:
                for line in self._file:
                    self._handle_data(line)
                    if self.end_tick_reason() or self.suspended:
                        return
                # If we get here, it means we're done reading this file. Close it and let next tick continue with next file.
                self._close_file()
        elif self._filename_index >= len(self._filenames):
            # We're done!
            self.stop()
            return
        else:
            filename = self._filenames[self._filename_index]
            if not filename:
                #print "*** FileReader USING stdin"  # DEBUG
                self._file = sys.stdin
            else:
                #print "*** FileReader OPENING FILE '%s'" % filename  # DEBUG
                self._file = open(filename, "r" if self.config.document_per_file else "rt")
            self._filename_index += 1
            # Return from tick and reenter later with a file to process
            return