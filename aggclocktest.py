
# A quick test to see how fast we can aggregate indicators. These
# are all simple sums with a 60% threshold and calculated WLD indicators
# regardless of the suitability of each indicator. Accordingly, all
# output should be considered bogus

from datetime import datetime
import os
import pandas as pd
import numpy as np
import decispy as dec

repo = dec.git.Session()

# reverse engineer a list of indicators from the working directory
cets_list = []
for root,dirs,files in os.walk('data'):
  for elem in files:
    (base,ext) = os.path.splitext(elem)
    if ext.lower() == '.csv':
      cets_list.append(base)

regions = pd.read_csv('regions.csv').set_index(['region','country']).drop('type', axis=1)

start = datetime.now()
for elem in cets_list:
    print('Processing: {}'.format(elem))

    df = repo.load([elem]).assign(N=1)
    time_periods = df.index.get_level_values('time').unique()
    
    # we need to create a left-side data frame keyed by time (from the data), region and country (from regions)
    # this will have len(regions) * len(time_periods) rows

    # create a dictionary of keys (time periods) and corresponding data frames; in this case,
    # the same (blank) data frame for each time period
    i = {t:regions for t in time_periods}
    master = pd.concat(i, keys=time_periods, names=['time']).reset_index()

    agg = master.join(df, on=['time', 'country'])

    a1 = agg.groupby(['time', 'region']).sum(min_count=1)

    # FIXME
    # add WLD aggregates
    wld = df.groupby('time').sum(min_count=1).assign(region='WLD').set_index('region', append=True)
    a1 = a1.append(wld).sort_index()

    # calculate mask values
    mask_values = agg.groupby(['time', 'region']).count()

    wld = df.groupby('time').count().assign(region='WLD').set_index('region', append=True)
    mask_values = mask_values.append(wld).sort_index()

    a1.loc[(mask_values[elem]/mask_values['N'])<0.6, elem] = np.nan
    a1.index.rename('economy', 'region', inplace=True)
    repo.save(a1[elem], prefix='aggregates')

finish = datetime.now()
print('Time elapsed: {}'.format(finish - start))
