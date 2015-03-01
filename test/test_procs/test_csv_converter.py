import os
import unittest
from eslib.procs import FileReader, FileWriter, CsvConverter


res = []

class TestCsvConverter(unittest.TestCase):

    def _setup(self, filename):

        r = FileReader(raw_lines=True)
        r.config.filename = filename

        c = CsvConverter()

        c.config.index      = "myindex"
        c.config.type_field = "initials"
        c.config.id_field   = "id"

        w = FileWriter()  # Write to stdout

        r.attach(c.attach(w))

        output = []
        c.add_callback(lambda doc: output.append(doc))

        return (r, c, w, output)

    def _verify(self, output):
        self.assertTrue(len(output) == 3, "Expected 3 results.")
        self.assertTrue(output[1]["_type"] == "eee")
        self.assertTrue(output[1]["_index"] == "myindex")
        self.assertTrue(output[1]["_id"] == "2")
        self.assertTrue(len(output[1]["_source"]) == 2)


    def test_read(self):
        r = FileReader(raw_lines=True)
        self_dir, _ = os.path.split(__file__)
        r.config.filename = os.path.join(self_dir, "data/csv_with_header.csv")
        w = FileWriter()  # Write to stdout
        w.subscribe(r)
        r.start()

    def test_first_line_is_columns(self):
        self_dir, _ = os.path.split(__file__)
        r, c, w, output = self._setup(os.path.join(self_dir, "data/csv_with_header.csv"))
        r.start()
        w.wait()

        self._verify(output)

    def test_no_header_line(self):
        self_dir, _ = os.path.split(__file__)
        r, c, w, output = self._setup(os.path.join(self_dir, "data/csv_no_header.csv"))
        c.config.columns = ["id", "name", "last name", "initials"]
        r.start()
        w.wait()

        self._verify(output)

    def test_skip_header_line(self):
        self_dir, _ = os.path.split(__file__)
        r, c, w, output = self._setup(os.path.join(self_dir, "data/csv_with_header.csv"))
        c.config.columns = ["id", "name", "last name", "initials"]
        c.config.skip_first_line = True
        r.start()
        w.wait()

        self._verify(output)

    # def test_fewer_fields(self):
    #     self_dir, _ = os.path.split(__file__)
    #
    #     r, c, w, output = self._setup(os.path.join(self_dir, "data/csv_no_header.csv"))
    #     c.config.id_field = "_id"
    #     c.config.type_field = "_type"
    #     c.config.columns = ["_id", None, "last name", "initials"]
    #     r.start()
    #     w.wait()
    #
    #     self.assertTrue(len(output) == 3, "Expected 3 results.")
    #     self.assertTrue(output[1]["_type"] == None)
    #     self.assertTrue(output[1]["_index"] == "myindex")
    #     self.assertTrue(output[1]["_id"] == "2")
    #     keys = output[1]["_source"].keys()
    #     self.assertTrue(len(keys) == 2)
    #     self.assertTrue("last name" in keys and "initials" in keys, "Expected 'last name' and 'initials' as result fields.")

def main():
    unittest.main()

if __name__ == "__main__":
    main()
