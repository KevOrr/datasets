import time

import sqlalchemy.orm.exc

from . import config
from . import db
from . import graphql
from . import util
from . import scraper

def reload():
    import importlib
    importlib.reload(config)
    importlib.reload(db)
    importlib.reload(graphql)
    importlib.reload(util)
    importlib.reload(scraper)
    del importlib

def scrape_repos():
    session = db.Session()
    rate_limit_remaining = 5000

    while True:
        rate_limit_remaining, reset_time, local_reset_time_str = scraper.expand_repos_from_db(session, rate_limit_remaining)
        if rate_limit_remaining <= 2:
            now = time.gmtime()
            sleep_time = 10 + max(0, reset_time - now)
            print('Sleeping {} seconds until {}'.format(sleep_time, local_reset_time_str))
            time.sleep(sleep_time)

        rate_limit_remaining, reset_time, local_reset_time_str = scraper.fetch_new_repo_info(session, rate_limit_remaining)
        if rate_limit_remaining <= 2:
            now = time.gmtime()
            sleep_time = 10 + max(0, reset_time - now)
            print('Sleeping {} seconds until {}'.format(sleep_time, local_reset_time_str))
            time.sleep(sleep_time)
