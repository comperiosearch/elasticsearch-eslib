# -*- coding: utf-8 -*-

"""
eslib.prog
~~~~~~~~~~

Helper functions for running as an executable program.
"""


__all__ = ( "progname", "initlogs")

import os, sys, logging.config, yaml


def progname():
    return os.path.basename(sys.argv[0])

def initlogs(config_file=None):
    # if config_file:
    #     config_file = os.path.join(os.getcwd(), config_file)
    # else:
    #     location = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
    #     config_file = os.path.join(location, 'logging.yml')
    #
    # config = yaml.load(open(config_file)) # TODO: YAML files are in UTF-8... if terminal is something else, make sure we convert correctly
    # logging.config.dictConfig(config=config)

    if config_file:
        config_file = os.path.join(os.getcwd(), config_file)
        config = yaml.load(open(config_file)) # TODO: YAML files are in UTF-8... if terminal is something else, make sure we convert correctly
        logging.config.dictConfig(config=config)
    else:
        console = logging.StreamHandler()
        console.setLevel(logging.TRACE)
        LOG_FORMAT = '%(firstName) -20s %(levelname) -10s %(className) -20s %(instanceName) -20s %(funcName) -25s %(lineno) -5d: %(message)s'
        console.setFormatter(logging.Formatter(LOG_FORMAT))

        servicelog = logging.getLogger("servicelog")
        servicelog.setLevel(logging.TRACE)
        servicelog.propagate = False
        servicelog.addHandler(console)

        proclog = logging.getLogger("proclog")
        proclog.setLevel(logging.TRACE)
        proclog.propagate = False
        proclog.addHandler(console)

        doclog  = logging.getLogger("doclog")
        doclog.setLevel(logging.TRACE)
        doclog.propagate = False
        doclog.addHandler(console)

        rootlog = logging.getLogger()
        rootlog.setLevel(logging.WARNING)
        rootlog.addHandler(console)
