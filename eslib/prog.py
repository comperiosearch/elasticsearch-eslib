# -*- coding: utf-8 -*-

"""
eslib.prog
~~~~~~~~~~

Helper functions for running as an executable program.
"""


__all__ = ( "progname", "initlogs")


import os, sys, codecs, logging.config, yaml

# Fix stdin and stdout encoding issues
_encoding_stdin  = sys.stdin.encoding or "UTF-8"
_encoding_stdout = sys.stdout.encoding or _encoding_stdin
sys.stdin = codecs.getreader(_encoding_stdin)(sys.stdin)
sys.stdout = codecs.getwriter(_encoding_stdout)(sys.stdout)

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
        console.setFormatter(logging.Formatter("%(lastname)s: %(message)s"))

        proclog = logging.getLogger("proclog")
        proclog.setLevel(logging.TRACE)
        proclog.addHandler(console)

        doclog  = logging.getLogger("doclog")
        doclog.setLevel(logging.TRACE)
        doclog.addHandler(console)
