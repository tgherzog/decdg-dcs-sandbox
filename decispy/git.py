
'''Git wrapper to a DECIS data repository
'''

import pandas as pd
import numpy as np
import io
import os
import logging
import yaml
from .util import init_view

try:
    import git as pygit
except ImportError:
    pygit = None

class Session():

    def __init__(self, path='.'):
        if pygit is None:
            raise ModuleNotFoundError('You must install GitPython to use this module: see https://gitpython.readthedocs.io')
            
        self.repo = pygit.Repo().init(path)

        self.path = path
        init_view(path)
        self.line_terminator = None

        # keys sets the key columns and order within a CSV file. These can be customized for subnational or additional dimensions
        self.keys = ['time', 'economy']
        self.values = {}

        # load parameters from yaml if present
        config_path = os.path.join(path, 'config.yaml')
        if os.path.exists(config_path):
            with open(config_path, 'r') as fd:
                config = yaml.safe_load(fd)
                for k,v in config.items():
                    if k == 'keys':
                        self.keys = v
                    else:
                        self.values[k] = v

        if type(self.keys) is not list:
            raise ValueError('keys must be a list in config.yaml')

        for k,v in self.values.items():
            if type(v) is not list:
                raise ValueError('{} must be a list in config.yaml'.format(k))


    def __repr__(self):
        return '<Session: path="{}" keys=[{}]>'.format(self.path, ', '.join(self.keys))

    @property
    def git(self):
        return self.repo.git


    def Series(self, series, *args):
        '''Return an empty series populated with the given values. These can be single values or lists, but must be in the
           same order as self.keys. Unspecified dimensions will be populated with default values
        '''

        values = {}
        if series:
            if type(series) is str or type(series) is int:
                values['series'] = [str(series)]
            else:
                values['series'] = series   # assume an iterable

        if len(args) > len(self.values):
            raise ValueError('Too many keys provided')

        i = 0
        for elem in self.keys:
            if i < len(args):
                # values are specified by function argument
                if type(args[i]) is str or type(args[i]) is int:
                    values[elem] = [str(args[i])]
                else:
                    values[elem] = args[i]
            elif elem in self.values:
                # take values from config
                values[elem] = self.values[elem]
            else:
                raise ValueError('No configured values for {}'.format(elem))
                
            i += 1

        return pd.Series(np.nan, index=pd.MultiIndex.from_product(values.values(), names=values.keys()), name='value')
                

    def changes(self, prefix='data', simplify=True):
        '''Return changed (dirty) files in repository

           prefix: only return changes from the specified directory prefix

           simplify: convert paths to system IDs (e.g. CETS codes). changes will be limited to CSV files
        '''

        repo = self.repo
        files = []
        if prefix:
            prefix += '/'

        for row in repo.git.status('--short').split('\n'):
            # currently only report files marked as 'modified' not added, deleted, moved etc
            if 'M' in row[0:2]:
                path = row[3:]
                if prefix is None or path.startswith(prefix):
                    if simplify:
                        (base,ext) = os.path.splitext(os.path.basename(path))
                        if ext.lower() == '.csv':
                            files.append(base)
                    else:
                        files.append(path)

        return files


    def read_csv(self, path, ref=None):
        '''Read a CSV either from the working directory or a specified commit
            
            path:      path to CSV (relative to git root)

            ref:       a commit ref or None for current working directory copy
        '''
        if ref:
            c = self.commit_for_path(ref, path)
            if c:
                buf = io.BytesIO(c.tree[path].data_stream.read())
                return pd.read_csv(buf)
        else:
            return pd.read_csv(path)


    def load(self, id, prefix='data', ref=None, long=False):
        '''Load a data frame or series from disk or a git repository

           id:     Strings are interpreted as CETS codes and return pandas.Series. List-likes are
                   interpreted as multiple CETS codes and return pandas.DataFrame, if long=False, or
                   pandas.Series, if long=True. Can also pass a path to a CSV to reference files generally
                  
           prefix: path prefix

           ref:    a git commit or branch reference.

           long:   If true, function will return an object with series in the index. If multiple
                   series will be loaded into rows instead of columns
        '''

        if type(id) is not str:
            if long:
                series = []
                for elem in id:
                    series.append(self.load(elem, prefix=prefix, ref=ref, long=True))

                s = pd.concat(series)
                s.name = 'value'
                return s

            else:
                df = None
                for elem in id:
                    s = self.load(elem, prefix=prefix, ref=ref, long=False).rename(elem)
                    if df is None:
                        df = pd.DataFrame(s)
                    else:
                        df =  df.join(s, how='outer')

                return df
                
        # else we should return a pandas.Series
        id = id.upper()
        path = '{}/{}.csv'.format(id.split('.')[0], id)
        if prefix:
            path = '{}/{}'.format(prefix, path)

        df = self.read_csv(path, ref)

        if long:
            return df.assign(series=id).set_index(['series'] + self.keys)['value']

        return df.set_index(self.keys)['value']


    def commit_for_path(self, ref, path):
        '''Return the nearest commit that contains the specified object
        '''

        for elem in self.repo.iter_commits(self.repo.commit(ref), paths=path):
            return elem

        # else this objectd is unknown at this reference; fall through to None


    def prepare_path(self, id, prefix):
        '''Formulate a repository path from a given CETS id and prefix, making sure
           the parent directories exist
        '''

        if not id:
            raise ValueError('Unspecified CETS code')

        if '.' not in id:
            raise ValueError('{} is not a valid CETS code'.format(id))

        path = []
        if prefix:
            path.append(prefix)

        id = id.upper()
        path.append(id.split('.')[0])

        if path:
            try:
                os.makedirs('/'.join(path))
            except FileExistsError:
                pass

        path.append('{}.csv'.format(id))
        return '/'.join(path)
    

    def save(self, obj, id=None, prefix='data'):
        '''Save a pandas object to one or more CSV files in the repository

           obj:      a data frame or series. If a data frame then each column will be saved as a series

           id:       CETS id, if not provided in df

           prefix:   path prefix
        '''

        def format_str(s):

            # pandas is sloppy about decimal points and integer values frequently get written as floats
            # we try to correct for this by looking for remainders and if none, we specify a formatting string
            if ((np.remainder(s, 1) == 0) | np.isnan(s)).all():
                return '%.0f'

            return None

        keys = ['series'] + self.keys

        if type(obj) is pd.DataFrame:
            # this form will save each column as a series. We should perform sanity checks
            if 'series' in obj.index.names and len(obj.columns) > 1:
                raise KeyError('Unsupported index/column declaration')
            elif id:
                raise ValueError('id cannot be specified for DataFrames')
                
            for col in obj.columns:
                self.save(obj[col], prefix=prefix)

            return

        # we try to do this as efficiently and with as few transformations as possible
        if obj.index.names == keys:
            # in this case, the series name(s) is in the first level of our index, so we
            # iterate over those and save each to a file
            if id:
                raise ValueError('id cannot be specified if series is included in MultiIndex')

            for id in obj.index.get_level_values(0).unique():
                xs = obj.xs(id).sort_index() # make sure these are sorted
                xs.name = 'value'
                path = self.prepare_path(id, prefix)
                xs.to_csv(path, line_terminator=self.line_terminator, float_format=format_str(xs))

        elif obj.index.names == self.keys:
            # in this case we save as is. 
            xs = obj.sort_index()
            if not id:
                id = xs.name
            
            path = self.prepare_path(id, prefix)
            xs.name = 'value'
            xs.to_csv(path, line_terminator=self.line_terminator, float_format=format_str(xs))

        # we also support indexes that are simply in the wrong order
        # this won't be efficient if a DataFrame is passed, but that's easily fixed prior to saving
        elif len(obj.index.names) == len(keys) and all(item in keys for item in obj.index.names):
            self.save(obj.reorder_levels(keys))

        elif len(obj.index.names) == len(self.keys) and all(item in self.keys for item in obj.index.names):
            self.save(obj.reorder_levels(self.keys))

        else:
            raise KeyError('Valid MultiIndex required')
            
