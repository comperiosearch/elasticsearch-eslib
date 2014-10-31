__author__ = 'Hans Terje Bakke'

from ..Processor import Processor

class ProtocolConverter(Processor):

    def __init__(self, func, input_protocol=None, output_protocol=None, **kwargs):
        super(ProtocolConverter, self).__init__(**kwargs)
        self.create_connector(self._incoming, "input", input_protocol)
        self._output = self.create_socket("output", output_protocol)

        self._func = func

    def _incoming(self, incoming):

        try:
            outgoing = self._func(incoming)
        except Exception as e:
            self.doclog.exception("Error in protocol converter function call.")

        if outgoing:
            self._output.send(outgoing)
