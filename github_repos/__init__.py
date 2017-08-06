import sys
import logging as _logging

_log = _logging.getLogger()
_log.setLevel(_logging.NOTSET)

_log_handler = _logging.FileHandler('run.log')
_log_handler.setFormatter(_logging.Formatter('%(asctime)s [%(name)-24.24s:%(lineno)d] [%(levelname)5.5s] %(message)s'))
_log_handler.setLevel(_logging.DEBUG)
_log.addHandler(_log_handler)

_console_handler = _logging.StreamHandler(sys.stdout)
_console_handler.setFormatter(_logging.Formatter('%(message)s'))
_console_handler.setLevel(_logging.INFO)
_log.addHandler(_console_handler)

from . import config
from . import db
from . import graphql
from . import util
from . import scraper


def reload():
    _log.info('Reloading main module')
    import importlib
    importlib.reload(config)
    importlib.reload(db)
    importlib.reload(graphql)
    importlib.reload(util)
    importlib.reload(scraper)
    del importlib
