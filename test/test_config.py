import unittest
from eslib import Config

class TestConfig(unittest.TestCase):

    def test_access(self):
        config = Config()
        config.set_default(a="A", b="B")

        print config["a"]
        print config.a

        self.assertEqual("A", config["a"])
        self.assertEqual("A", config.a)

    def test_assignment(self):
        config = Config()
        #config.set_default(a="A", b="B")

        config["a"] = "A"
        config.a = "B"

        print config["a"]
        print config.a

        self.assertEqual("B", config["a"])
        self.assertEqual("B", config.a)


    def test_defaults_and_overrides(self):
        config = Config()
        config.set_default(a="A", b="B", x="X")

        config.set(a="D", b=None)

        print config["a"]
        print config.a
        self.assertEqual("D", config.a)

        config.a = "C"
        print config.a
        self.assertEqual("C", config.a)

        print config["b"]
        print config.b
        self.assertEqual(None, config.b)

        print config.x
        self.assertEqual("X", config.x)

def main():
    unittest.main()

if __name__ == "__main__":
    main()
