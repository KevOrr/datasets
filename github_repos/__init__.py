import sys
import os.path
import logging as _logging
import logging.handlers as _logging_handlers
import atexit

_log = _logging.getLogger()
_log.setLevel(_logging.INFO)

_log_handler = _logging_handlers.RotatingFileHandler(os.path.join(os.path.dirname(__file__), 'logs', 'run.log'),
                                                     mode='a', maxBytes=1024**2, backupCount=3)
_log_handler.setFormatter(_logging.Formatter('%(asctime)s [%(name)s:%(lineno)d] [%(levelname)s] %(message)s'))
_log_handler.setLevel(_logging.DEBUG)
_log.addHandler(_log_handler)

_console_handler = _logging.StreamHandler(sys.stdout)
_console_handler.setFormatter(_logging.Formatter('%(message)s'))
_console_handler.setLevel(_logging.INFO)
_log.addHandler(_console_handler)

log = _logging.getLogger(__name__)

atexit.register(log.info, "Exiting")

from . import config
from . import db
from . import graphql
from . import util
from . import scraper


def reload():
    log.info('Reloading main module')
    import importlib
    importlib.reload(config)
    importlib.reload(db)
    importlib.reload(graphql)
    importlib.reload(util)
    importlib.reload(scraper)
    del importlib
