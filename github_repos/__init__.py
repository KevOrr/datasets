from . import config
from . import graphql
from . import util
from . import scraper

if config.debug:
    import importlib
    importlib.reload(config)
    importlib.reload(graphql)
    importlib.reload(util)
    importlib.reload(scraper)
    del importlib
