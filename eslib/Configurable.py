class Config(object):
    def __init__(self, **config):
        super(Config, self).__init__()
        if config is not None:
            self.__dict__ = config
            self.defaults = {}

    def set_default(self, **kwargs):
        for key,val in kwargs.iteritems():
            self.defaults[key] = val
            if not key in self.__dict__:
                self.__dict__[key] = val

    def set(self, **kwargs):
        for key,val in kwargs.iteritems():
            self.__dict__[key] = val

    def get_default_attributes(self):
        return self.defaults

    def get_user_attributes(self):
        return {key: val for key, val in self.__dict__.iteritems() if key not in self.defaults}

class Configurable(object):
    def __init__(self, **kwargs):
        super(Configurable, self).__init__()
        self.config = Config(**kwargs)
