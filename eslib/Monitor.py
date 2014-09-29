from .Generator import Generator

class Monitor(Generator):
    def __init__(self, name):
        super(Monitor, self).__init__(name)

        self.keepalive = True  # A monitor never stops, unless told to
