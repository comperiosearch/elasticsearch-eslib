__author__ = 'Hans Terje Bakke'

# TODO: Logging

from .Service import Service


class PipelineService(Service):
    def __init__(self, **kwargs):
        super(PipelineService, self).__init__(**kwargs)

        self.head = None
        self.tail = None

    #region Controller overrides

    def on_status(self):
        return {"head": self.head.status, "tail": self.tail.status}

    def on_start(self):
        self.head.start()
        return True  # TODO

    def on_restart(self):
        self.head.restart()
        return True  # TODO

    def on_stop(self):
        self.head.stop()
        self.tail.wait()
        return True  # TODO

    def on_abort(self):
        self.head.abort()
        self.tail.wait()
        return True  # TODO

    def on_suspend(self):
        self.head.suspend()
        return True  # TODO

    def on_resume(self):
        self.head.resume()
        return True  # TODO

    #endregion Controller overrides@