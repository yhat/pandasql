# [mypandas](https://github.com/yrom1/mypandas) — MySQL for Pandas

A package that lets you query pandas DataFrames with MySQL!

## Notice
This is a work in progress!

## Install

Currently available on [PyPI](https://pypi.org/project/mypandas/), to install:
```
pip install mypandas
```

## Example

```py
from shared import add_mypandas_to_path  # isort: ignore

add_mypandas_to_path()

from mypandas.sqldf import MyPandas
from mypandas import load_births
import pandas as pd

births = load_births()
assert type(births) == pd.DataFrame
URI = "mysql://root:password@localhost/"
QUERY = """
SELECT b1.date d1
    , b1.births b1
    , b2.date d2
    , b2.births b2
FROM births b1, births b2
"""
print(MyPandas(URI)(QUERY, locals()))

```
```
               d1      b1         d2      b2
0      2012-12-01  340995 1975-01-01  265775
1      2012-11-01  320195 1975-01-01  265775
2      2012-10-01  347625 1975-01-01  265775
3      2012-09-01  361922 1975-01-01  265775
4      2012-08-01  359554 1975-01-01  265775
...           ...     ...        ...     ...
166459 1975-05-01  254545 2012-12-01  340995
166460 1975-04-01  247455 2012-12-01  340995
166461 1975-03-01  268849 2012-12-01  340995
166462 1975-02-01  241045 2012-12-01  340995
166463 1975-01-01  265775 2012-12-01  340995

[166464 rows x 4 columns]

```
