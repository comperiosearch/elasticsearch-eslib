from .Processor import Processor

class Generator(Processor):
    def __init__(self, **kwargs):
        super(Generator, self).__init__(**kwargs)
        self.is_generator = True

    # These methods could/should be implemented by inheriting classes:

    # on_open(self)     # from Processor
    # on_close(self)    # from Processor

    # on_startup(self)
    # on_shutdown(self)
    # on_abort(self)    # from Processor
    # on_tick(self)
    # on_suspend(self)
    # on_resume(self)

    # If on_tick finishes on its own without external stop call, call self.stop() from there when done.

    @property
    def end_tick_reason(self):
        "If 'aborted', 'stopping' or not 'running'. 'suspended' is not a reason to leave the tick; handle this yourself."
        return self.aborted or self.stopping or self.restarting or not self.running
