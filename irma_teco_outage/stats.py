import os, os.path
import json
import operator as op
from decimal import Decimal

import pandas as pd
from matplotlib import pyplot as plt
import dateutil

DATA_DIR = 'data'
FILE_STRF = '%Y-%m-%d_%H:%M:%S'

data = []

print('Loading data...')
for filename in os.listdir(DATA_DIR):
    if filename.rpartition('.')[2].lower() != 'json':
        continue

    dt = dateutil.parser.parse(filename.rpartition('.')[0])
    with open(os.path.join(DATA_DIR, filename)) as f:
        j = json.load(f, parse_float=Decimal)
        for loc in j['markers']:
            data.append((dt, (loc['lat'], loc['lng']), loc['nbrCust']))

data = pd.DataFrame(data, columns=('date', 'location', 'customers_affected'))

print('Sorting...')
s = data['location'].apply(op.itemgetter(0))
print(s)
exit()
data = data.set_index(s).sort_values('index')

print('Pivoting...')
data = data.pivot(index='date', columns='location', values='customers_affected')

print('Plotting...')
data.plot.area(legend=False, colormap='Set3')
plt.show()
