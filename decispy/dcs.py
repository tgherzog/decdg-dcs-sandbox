
'''
Script and module for loading Excel files from DCS into pandas. Also can be run from the command line
to export files to CSV long format.

Usage:
  dcs.py EXCEL
  dcs.py EXCEL CSV

'''

import pandas as pd
import sys

def read(fp):
  '''Read Excel file from DCS and try to guess long or wide
  '''

  df = read_excel(fp)

  # try to sniff if this is wide or long format
  if df.columns[0] == 'Country Name':
    return translate_bulk_to_long(df)
  elif 'Time' in df.columns and df.loc[0, 'Time'] == 'SCALE':
    return translate_to_long(df)

  return standardize(df)


def read_excel(fp):
  '''Read an excel file with no wrangling, but trap for a common read issue
  '''

  try:
    return pd.read_excel(fp)
  except KeyError:
    # This likely indicates the file wasn't saved quite right when generated from the back-end
    raise RuntimeError("An error was encountered reading this file which suggests a formatting issue. Often you can fix this by loading and saving the file in Excel.")

def read_long(fp):
  '''Read Excel file from DCS in long format.
  '''

  return standardize(pd.read_excel(fp))

def read_wide(fp):
  '''Read Excel file from DCS in wide format
  '''

  return translate_to_long(pd.read_excel(fp))

def read_bulk(fp):
  '''Read a bulk download file, like the find provided publicly on the Databank server, e.g.
     https://databank.worldbank.org/data/download/archive/WDI_excel_2021_09_15.zip
     Note that files must be unzipped first, and often fixed by saving them in Excel
  '''

  return translate_bulk_to_long(read_excel(fp))


def translate_bulk_to_long(df):

  df.drop(['Country Name', 'Indicator Name'], axis=1, inplace=True)
  df.rename({'Country Code': 'economy', 'Indicator Code': 'series'}, axis=1, inplace=True)
  df = df.melt(id_vars=df.columns[:2], value_vars=df.columns[2:], var_name='time')
  df['time'] = df['time'].apply(lambda x: 'YR' + str(x))
  return df

    
def translate_to_long(df):
  '''Translate a data frame from wide to long format 
  '''
 
  # the actual column names will be split between rows 1 and 2. The "Time" label in row 1
  # actually holds the Scale variable and will be deleted. Everything to the right of the Time
  # column hold the actual values in wide format.

  # Our first step is to create correct column names from rows 1 and 2 and drop the Scale column along with the 1st row
  p = df.columns.get_loc('Time')
  c = list(df.columns[p:])
  c2 = list(df.loc[0][:p])
  df.columns = c2 + c
  df.drop(0, inplace=True)
  df.drop('Time', axis=1, inplace=True)

  # pivot columns at or to the right of what previously was the time column
  return standardize(df.melt(id_vars=df.columns[:p], value_vars=df.columns[p:], var_name='time'))

def standardize(df):
  '''Drop and rename DCS columns to standards
  '''

  return df.drop('SCALE', axis=1, errors='ignore').rename({
        'Time': 'time',
        'Country': 'economy',
        'Economy': 'economy',
        'Series': 'series',
        'Data': 'value'
    }, axis=1)

if __name__ == '__main__':
    from docopt import docopt

    config = docopt(__doc__)
    df = read(config['EXCEL'])

    if config['CSV']:
        df.to_csv(config['CSV'], index=False)
    else:
        df.to_csv(sys.stdout, index=False)
