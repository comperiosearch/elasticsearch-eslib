class Config(object):
    def __init__(self, **config):
        super(Config, self).__init__()
        if config is not None:
            self.__dict__ = config
            self.defaults = {}

    def set_default(self, **kwargs):
        for key,val in kwargs.iteritems():
            self.defaults[key] = val
            # if not key in self.__dict__:
            #     self.__dict__[key] = val

    def __getattr__(self, key):
        if key in self.__dict__:
            return self.__dict__.__getattr__(key)
        elif key in self.defaults:
            return self.defaults[key]
        else:
            raise AttributeError("'%s' has no attribute '%s'" % (self.__class__.__name__, key))

    def __getitem__(self, key):
        if key in self.__dict__:
            return self.__dict__[key]
        elif key in self.defaults:
            return self.defaults[key]
        else:
            raise AttributeError("'%s' has no attribute '%s'" % (self.__class__.__name__, key))

    def __setitem__(self, key, value):
            self.__dict__[key] = value

    def set(self, ignore_none=False, **kwargs):
        "ignore_none means that fields with value None are not set."
        for key,val in kwargs.iteritems():
            if ignore_none and val is None:
                continue
            self.__dict__[key] = val

    def get_default_attributes(self):
        return self.defaults

    def get_user_attributes(self):
        return {key: val for key, val in self.__dict__.iteritems() if key not in self.defaults}

class Configurable(object):
    def __init__(self, **kwargs):
        super(Configurable, self).__init__()
        self.config = Config(**kwargs)
