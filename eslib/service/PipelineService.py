__author__ = 'Hans Terje Bakke'

from .Service import Service
import time

class PipelineService(Service):
    def __init__(self, **kwargs):
        super(PipelineService, self).__init__(**kwargs)

        self.head = None
        self.tail = None

    def _log_finished(self, proc):
        self.log.status("Processing finished.")
        self._processing = False  # This will shortcut further evaluation of whether we are processing
        self.stat_processing_ended = time.time()

    def _log_aborted(self, proc):
        self.log.status("Processing finished after abort.")
        self._processing_aborted = True  # This will shortcut further evaluation of whether we are aborted
        self.stat_processing_ended = time.time()

    def link(self, *processors):
        "Link processors together and assign head and tail."
        prev = None
        for proc in processors:
            if prev:
                proc.subscribe(prev)
            prev = proc
        self.head = processors[0]
        self.tail = processors[-1]

    #region Service overrides

    def is_processing(self):
        "Evaluate whether processing is in progress."
        return self.tail.running

    def is_aborted(self):
        "Evaluate whether processing is in progress."
        return self.head.aborted

    def is_suspended(self):
        "Evaluate whether processing is suspended."
        return self.head.suspended

    def on_processing_start(self):
        if not self._log_finished in self.tail.event_stopped:
            self.tail.event_stopped.append(self._log_finished)
        if not self._log_aborted in self.tail.event_aborted:
            self.tail.event_aborted.append(self._log_aborted)

        self.head.start()
        return True

    def on_restart(self):
        # if not self.head.running:
        #     self.head.start()
        # else:
        #     return True
        return True  # Well, not really, but still.. it didn't fail either.

    def on_processing_stop(self):
        "This method should block until the process is fully stopped."
        self.head.stop()
        self.tail.wait()
        return True

    def on_processing_abort(self):
        self.head.abort()
        self.tail.wait()
        return True

    def on_processing_suspend(self):
        self.head.suspend()
        return True

    def on_processing_resume(self):
        self.head.resume()
        return True

    # TODO
    def on_update(self, config):
        # Auto-start on update
        if not self.head.running:
            self.head.start()
        else:
            return True

    def on_count(self):
        # It is probably better to count what has been handled by the tail, than what the head received or generaterd, so:
        return self.tail.count

    def on_count_total(self):
        return self.head.total

    #endregion Service overrides
