
'''
Create a DCS-compatible Excel file from set of CSV files or directory structure

Usage:
  dcsmake.py EXCEL PATH...


'''

import pandas as pd
import os
from docopt import docopt

config = docopt(__doc__)

master = None
# it's hard in pandas to append or concat a bunch of relatively small dataframes
# into a large one efficiently, it's always reallocating and copying memory
# To boost speed, we aggregate one dirctory at a time and than append to the master
# which should result in a lot fewer in-memory copy operations of the master frame
for path in config['PATH']:
    if os.path.isdir(path):
        for root,dirs,files in os.walk(path):
            sd = None
            # aggregate the subdirectory to a single data frame
            for i in files:
                (base,ext) = os.path.splitext(i)
                if ext.lower() == '.csv':
                    fullpath = '{}/{}'.format(root, i)
                    print('Reading {}'.format(fullpath))
                    df = pd.read_csv(fullpath)
                    df.insert(0, 'series', base)
                    if sd is None:
                        sd = df
                    else:
                        sd = sd.append(df)

            # now copy the sub-directory frame to the master
            if master is None:
                master = sd
            elif sd is not None:
                master = master.append(sd)
    else:
        (base,ext) = os.path.splitext(os.path.basepath(path))
        df = pd.read_csv(path)
        df.insert(0, 'series', base)
        if master is None:
            df = master
        else:
            master = master.append(df)

# transform
print('Writing {} '.format(config['EXCEL']), end='', flush=True)
master = master.pivot(index=['economy','series'], columns='time', values='value')
master.insert(0, 'SCALE', 0)
master.sort_index(inplace=True)
master.reset_index(inplace=True)

# create and insert 2 header columns by hand
columns = list(master.columns)
columns[0:3] = ['', '', 'Time']

master.loc[-2] = columns
master.loc[-1] = ['Country', 'Series', 'SCALE'] + ([''] * (len(master.columns)-3))

master.sort_index(inplace=True)

master.to_excel(config['EXCEL'], index=False, header=False)
print('')
