import sqlite3 as sqlite
import pandas as pd
import numpy as np
from pandas.io.sql import to_sql, read_sql
import re
import os

def _ensure_data_frame(obj, name):
    """
    obj a python object to be converted to a DataFrame

    take an object and make sure that it's a pandas data frame
    """
    # we accept pandas Dataframe, and also dictionaries, lists, tuples
    # we'll just convert them to Pandas Dataframe
    if isinstance(obj, pd.DataFrame):
        df = obj
    elif isinstance(obj, (tuple, list)) :
        #tuple and list case
        if len(obj)==0:
            return pd.Dataframe()

        firstrow = obj[0]

        if isinstance(firstrow, (tuple, list)):
            #multiple-columns
            colnames = ["c%d" % i for i in range(len(firstrow))]
            df = pd.DataFrame(obj, columns=colnames)
        else:
            #mono-column
            df = pd.DataFrame(obj, columns=["c0"])

    if not isinstance(df, pd.DataFrame) :
        raise Exception("%s is not a Dataframe, tuple, list, nor dictionary" % name)

    for col in df:
        if df[col].dtype==np.int64:
            df[col] = df[col].astype(np.float)
        elif isinstance(df[col].get(0), pd.tslib.Timestamp):
            df[col] = df[col].apply(lambda x: str(x))

    return df

def _extract_table_names(q):
    "extracts table names from a sql query"
    # a good old fashioned regex. turns out this worked better than actually parsing the code
    rgx = '(?:FROM|JOIN)\s+([A-Za-z0-9_]+)'
    tables = re.findall(rgx, q, re.IGNORECASE)
    return list(set(tables))

def _write_table(tablename, df, conn):
    "writes a dataframe to the sqlite database"

    for col in df.columns:
        if re.search("[() ]", col):
            msg = "please follow SQLite column naming conventions: "
            msg += "http://www.sqlite.org/lang_keywords.html"
            raise Exception(msg)

    to_sql(df, name=tablename, con=conn, flavor='sqlite')


def sqldf(q, env, inmemory=True):
    """
    query pandas data frames using sql syntax

    Parameters
    ----------
    q: string
        a sql query using DataFrames as tables
    env: locals() or globals()
        variable environment; locals() or globals() in your function
        allows sqldf to access the variables in your python environment
    dbtype: bool
        memory/disk; default is in memory; if not memory then it will be 
        temporarily persisted to disk

    Returns
    -------
    result: DataFrame
        returns a DataFrame with your query's result

    Examples
    --------
    >>> import pandas as pd
    >>> df = pd.DataFrame({
        "x": range(100),
        "y": range(100)
    })
    >>> from pandasql import sqldf
    >>> sqldf("select * from df;", globals())
    >>> sqldf("select * from df;", locals())
    >>> sqldf("select avg(x) from df;", locals())
    """

    if inmemory:
        dbname = ":memory:"
    else:
        dbname = ".pandasql.db"
    conn = sqlite.connect(dbname, detect_types=sqlite.PARSE_DECLTYPES)
    tables = _extract_table_names(q)
    for table in tables:
        if table not in env:
            conn.close()
            if not inmemory :
                os.remove(dbname)
            raise Exception("%s not found" % table)
        df = env[table]
        df = _ensure_data_frame(df, table)
        _write_table(table, df, conn)

    try:
        result = read_sql(q, conn, index_col=None)
        del result['index']
    except Exception, e:
        result = None
    finally:
        conn.close()
        if not inmemory:
            os.remove(dbname)
    return result

