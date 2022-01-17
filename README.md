This is a sandbox for doing data management in Git using
python/pandas or RStudio alongside or in lieu of DCS.
This repository consists of two components:

1. A set of scripts, python packages and samples that demonstrate
   and implement various data production processes using open source tools, and;
2. WDI data saved as a set of CSV files, one per indicator, and
   stored as git commits

## Data ##

For demonstration purposes, I populated the repository using
bulk download files stored on Databank. At this point the Databank
server includes five bulk download files for FY22
(July, September, October, November, and December, 2021).
For each release, I used the `make.py` script to load these files
and split each indicator into its own optimized CSV file (omitting
known aggregations and derived indicators). Then I saved each
release as a new commit, deleted the data directory, and ran the
script again with the next bulk download file. This process
allowed me to use git tools to identify high-level and granular
changes across each release, while ensuring that indicators
deleted in any release would also be reflected in the repository
(although in this case there were none).

After each update, I recorded the aggregate size of the data
directory and the git database to gauge how the repository might grow
over time:


Release | Data Size (Mb) | Git DB Size (Mb) | Total CSVs | Changed CSVs
------- | -------------- | ---------------- | ---------- | ------------
07/2021 |            305 |               95 |       1401 |         1401
09/2021 |            305 |              104 |       1401 |          162
10/2021 |            306 |              132 |       1401 |          353
11/2021 |            306 |              133 |       1401 |           19
12/2021 |            306 |              169 |       1401 |          416


## Repository Contents ##

The `data/` directory is an extract of the "WDI working" database
in DCS. Each indicator is its own CSV file. These can easily
be loaded into data frames either individually or collectively
through simple interface functions.

`decispy` is the beginnings of a python package to support this work.
It includes modules for converting Excel files exported from DCS
and bulk download files from databank. It also includes code to
read indicators into pandas dataframes, either from CSV files or
from prior git commits.

`make.py` is a utility for outputting the contents of the `data/`
directory from an export file, overwriting what was previously
there. If the previous contents are part of a commit, this allows
you to see changes (albeit unattributed) in "WDI working" over time.
Note that by default, `make.py` excludes aggregates and some derived
indicators, as defined in `make.yaml`.

`dcsmake.py` goes the other way and creates a single Excel file
from the CSVs in a directory structure (xlsx is in wide format)

`aggclocktest.py` runs mock aggregations (sums with a 2/3rds threshold)
on all indicators just to get a sense of how long this would take.
Note that output is bogus and should not be used; this is just there
to get a sense of how long a system-wide aggregation could take.

`cookbook.ipynb` is a jupyter notebook with some "recipes" for
common data management tasks using python/pandas.
