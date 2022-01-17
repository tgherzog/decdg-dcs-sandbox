
'''
Make utility. Create or update a repository from DCS. File can be a CSV with
recognizable field names in row 1, or an Excel file from DCS in either wide
or long format. If neither --wide or --long is explicitly specified the script
will try to guess on its own.

Additional files can be specified to be "merged" with the first file. Each merge
identifier must be in the form column:filename where column is the name of a
dimension, such as "series" or "time" (following the standardized names in the
utility package). The utility will load the filename, identify the unique
elements in the specified column and delete these from the original file, then
replace them with the merged file. The column layout and orientation (long or wide)
should be the same across all files.

After loading and merging input files, the utility splits the file by series,
writing each series to its own CSV output. CSV files are generally sorted first by
time and then by remaining columns although this is somewhat flexible.

Usage:
    make.py [--limit=CETS] [--long | --wide | --bulk] [--no-excludes] FILE [MERGE...]

Options:
    --limit=CETS    Make/update just this CETS code

    --long          Explicit long input format

    --wide          Explicit wide input format

    --bulk          Explicit public bulk download format (ala Databank)

    --no-excludes   Ignore the exclusion lists and write the entire DCS file

'''

import decispy.dcs as dcs
from docopt import docopt
import pandas as pd
import numpy as np
import yaml
import os
import sys

config = docopt(__doc__)

# hard-coded config: this sets the line-terminator for CSV output. None for default or '\r\n' for DOS
config['--eol'] = None

config['exclude'] = {'series': [], 'economy': [], 'time': []}
# load yaml config options, if any
try:
    with open('make.yaml', 'r') as fd:
        yaml_config = yaml.safe_load(fd)
        for row in config.keys():
            if row in yaml_config and type(config[row]) is type(yaml_config[row]):
                if type(config[row]) is dict:
                    config[row].update(yaml_config[row])
                else:
                    config[row] = yaml_config[row]

except FileNotFoundError:
    pass

def read_input(path):

    if os.path.splitext(path)[1].lower() == '.csv':
        return pd.read_csv(path)
    elif config['--long']:
        return dcs.read_long(path)
    elif config['--wide']:
        return dcs.read_wide(path)
    elif config['--bulk']:
        return dcs.read_bulk(path)
    else:
        return dcs.read(path)

print('Reading {}'.format(config['FILE']) )
df = read_input(config['FILE'])

# all columns except the last one are treated as index values
index = list(df.columns[:-1])

# give highest priority to series and lowest to time if those exist
if 'series' in index:
    index.remove('series')
    index.insert(0, 'series')
if 'time' in index:
    index.remove('time')
    index.insert(1, 'time')

df = df.set_index(index)

for m in config['MERGE']:
    (key,name) = m.split(':', 1)
    if not key or not name:
        raise ValueError('Merge arguments must be in column:filename format')

    if key not in df.index.names:
        raise ValueError('{} is not a column in {}'.format(key, config['FILE']))

    print('Merging {}'.format(name))
    z = read_input(name).set_index(index)

    # get a list of unique time values (last index level) and drop them from the master
    t = list(z.index.get_level_values(key).unique())
    df = df.drop(t, level=key).append(z)

# exclude elements
if not config['--no-excludes']:
    for k,v in config['exclude'].items():
        if k in df.index.names:
            df.drop(v, level=k, errors='ignore', inplace=True)

series = df.index.get_level_values(0).unique()
for elem in series:
    if config['--limit'] and config['--limit'] != elem:
        continue

    # create the directory hierarchy as necessary
    if '.' in elem:
        path = 'data/{}'.format(elem.split('.')[0])
    else:
        path = 'data'

    try:
        os.makedirs(path)
    except:
        pass

    path = '{}/{}.csv'.format(path, elem)
    print('Writing {}'.format(path))

    # pandas is sloppy about decimal points and integer values frequently get written as floats
    # we try to correct for this by looking for remainders and if none, we specify a formatting string
    xs = df.xs(elem)
    s = xs.iloc[:,0]    # 1st column as a series
    if ((np.remainder(s, 1) == 0) | np.isnan(s)).all():
        fstr = '%.0f'
    else:
        fstr = None

    xs.sort_index().to_csv(path, line_terminator=config['--eol'], float_format=fstr)
