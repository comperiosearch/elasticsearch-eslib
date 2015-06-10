__author__ = 'Hans Terje Bakke'

from ..Processor import Processor

class Transformer(Processor):
    """
    Convert input to output protocol.
    Returns a LIST of zero or more documents converted to the output protocol.

    The following parameters are not part of the processors 'config' object, and can and must be set only upon
    instantiation:

        input_protocol  = None
        output_protocol = None
        func            = None     # Mandatory! Must be a function returning a list (or generator) of zero or more
                                     documents complying with the output protocol. Function signature must be
                                     func(proc, doc), where proc is this transformer processor, so you can address it
                                     in your function.
    """
    def __init__(self, func=None, input_protocol=None, output_protocol=None, **kwargs):
        super(Transformer, self).__init__(**kwargs)
        self.create_connector(self._incoming, "input", input_protocol)
        self._output = self.create_socket("output", output_protocol)

        self._func = func

    def _incoming(self, incoming):

        try:
            ll = self._func(self, incoming)
            if ll:
                for outgoing in ll:
                    if outgoing:
                        self._output.send(outgoing)
        except Exception as e:
            self.doclog.exception("Error in protocol converter function call.")
