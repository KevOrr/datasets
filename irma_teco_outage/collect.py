#!/usr/bin/env python3

import os.path
import sys
import time
import datetime
import logging
import re

import requests
import dateutil, dateutil.tz

DATA_URL = 'http://www.tampaelectric.com/residential/outages/outagemap/datafilereader/index.cfm'
DATA_HEADERS = {'Referer': 'http://www.tampaelectric.com/residential/outages/outagemap/'}
DATA_OUTPUT_PREFIX = 'data'

OUTAGE_PERCENT_URL = 'http://www.tampaelectric.com/residential/outages/outagemap/'
OUTAGE_PERCENT_TAG_ID = 'pCentCustomersIn2'
OUTAGE_PERCENT_FILE = 'outage_percent.log'

DELAY = 60 * 5 # seconds

KML_URL = 'http://www.tampaelectric.com/files/kml/service_territory.kml'

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

file_handler = logging.FileHandler('log')
file_handler.setFormatter(logging.Formatter('%(asctime)s [%(name)s:%(lineno)d] [%(levelname)s] %(message)s'))
file_handler.setLevel(logging.DEBUG)
log.addHandler(file_handler)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(logging.Formatter('%(message)s'))
console_handler.setLevel(logging.INFO)
log.addHandler(console_handler)


def fetch_data():
    now = datetime.datetime.now(dateutil.tz.tzlocal()).replace(microsecond=0)
    r = requests.get(DATA_URL, headers=DATA_HEADERS)
    log.info('Got HTTP %d from data endpoint', r.status_code)

    now_str = now.isoformat()
    filename = os.path.join(DATA_OUTPUT_PREFIX, now_str + '.json')
    with open(filename, 'w') as f:
        size = f.write(r.text)
    log.info('Wrote %d chars to %s', size, filename)

def fetch_total_cust_count():
    r = requests.get(OUTAGE_PERCENT_URL)
    log.info('Got HTTP %d from outage map', r.status_code)

    match = re.search('var\s+tnbrc\s*=\s*(\d+);', r.text)
    return match.group(1)

def round(n, base):
    return n - (n % base)


def main():
    next_interval = datetime.datetime.now().replace(second=0, microsecond=0)
    next_interval = next_interval.replace(minute=(round(next_interval.minute, 5)))
    next_interval += datetime.timedelta(minutes=5)

    # with open('total_cust_count', 'w') as f:
    #     cust_count = fetch_total_cust_count()
    #     log.info('Got %s as customer count', cust_count)
    #     f.write(cust_count)

    log.info('Sleeping until %s', next_interval.ctime())
    time.sleep(max(0, (next_interval - datetime.datetime.now()).total_seconds()))

    while True:
        try:
            fetch_data()
        except Exception as e:
            log.error(e)
            time.sleep(10)
        else:
            next_interval = next_interval.replace(minute=(round(next_interval.minute, 5)))
            next_interval += datetime.timedelta(minutes=5)
            log.info('Sleeping until %s', next_interval.ctime())
            time.sleep(max(0, (next_interval - datetime.datetime.now()).total_seconds()))


if __name__ == '__main__':
    main()
