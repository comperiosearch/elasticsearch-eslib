from .Generator import Generator

class Monitor(Generator):
    def __init__(self, **kwargs):
        super(Monitor, self).__init__(**kwargs)

        self.keepalive = True  # A monitor never stops, unless told to
