import sqlite3 as sqlite
import sqlparse
from sqlparse.tokens import Whitespace
import pandas as pd
import numpy as np
from pandas.io.sql import write_frame, frame_query
import os
import re

def _ensure_data_frame(obj, name):
    """
    obj a python object to be converted to a DataFrame

    take an object and make sure that it's a pandas data frame
    """
    #we accept pandas Dataframe, and also dictionaries, lists, tuples
        #we'll just convert them to Pandas Dataframe
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

    return df

def _extract_table_names(q):
    "extracts table names from a sql query"

    tables = set()
    next_is_table = False
    for query in sqlparse.parse(q):
        for token in query.tokens:
            if token.value.upper() == "FROM" or "JOIN" in token.value.upper():
                next_is_table = True
            elif token.ttype is Whitespace:
                continue
            elif token.ttype is None and next_is_table:
                tables.add(token.value)
                next_is_table = False
    return list(tables)


def _write_table(tablename, df, conn):
    "writes a dataframe to the sqlite database"

    for col in df.columns:
        if re.search("[() ]", col):
            msg = "please follow SQLite column naming conventions: "
            msg += "http://www.sqlite.org/lang_keywords.html"
            raise Exception(msg)

    write_frame(df, name=tablename, con=conn, flavor='sqlite')


def sqldf(q, env, inmemory=True):
    """
    query pandas data frames using sql syntax

    q: a sql query using DataFrames as tables
    env: variable environment; locals() or globals() in your function
         allows sqldf to access the variables in your python environment
    dbtype: memory/disk
        default is in memory; if not memory then it will be temporarily
        persisted to disk

    Example
    -----------------------------------------

    # example with a data frame
    df = pd.DataFame({
        x: range(100),
        y: range(100)
    })

    from pandasql import sqldf
    sqldf("select * from df;", locals())
    sqldf("select avg(x) from df;", locals())

    #example with a list

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
        result = frame_query(q, conn)
    except:
        result = None
    finally:
        conn.close()
        if not inmemory:
            os.remove(dbname)
    return result

