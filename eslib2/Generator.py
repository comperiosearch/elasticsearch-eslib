from .Processor import Processor

class Generator(Processor):
    def __init__(self, name):
        super(Generator, self).__init__(name)
        self.is_generator = True

    # These methods could/should be implemented by inheriting classes:

    # startup_handler(self)
    # shutdown_handler(self)
    # abort_handler(self)
    # processing_tick_handler(self)

    # If processing_tick_handler finishes on its own without external stop call, call self.stop() from there when done.
