class Config(object):
    def __init__(self, **config):
        if config is not None:
            self.__dict__ = config

    def set_if_missing(self, **kwargs):
        for key,value in kwargs.iteritems():
            if not key in self.__dict__:
                self.__dict__[key] = value

class Configurable(object):
    def __init__(self, **kwargs):
        super(Configurable, self).__init__()
        self.config = Config(**kwargs)
