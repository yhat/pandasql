import sqlite3 as sqlite
import sqlparse
from sqlparse.tokens import Whitespace
import pandas as pd
from pandas.io.sql import write_frame, frame_query
import os
import re


def _extract_table_names(q):
    "extracts table names from a sql query"
    tables = set()
    next_is_table = False
    for query in sqlparse.parse(q):
        for token in query.tokens:
            if token.value.upper()=="FROM" or "JOIN" in token.value.upper():
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


def sqldf(q, env): 
    """
    query pandas data frames using sql syntax
    
    q: a sql query using DataFrames as tables
    env: variable environment; you must include locals() or globals() in your function
         call to allow sqldf to access the variables in your python environment

    Example
    -----------------------------------------
    df = pd.DataFame({
        x: range(100),
        y: range(100)
    })

    from pandasql import sqldf
    sqldf("select * from df;", locals())
    sqldf("select avg(x) from df;", locals())
    """
    with sqlite.connect('.pandasql.db', detect_types=sqlite.PARSE_DECLTYPES) as conn:
        tables = _extract_table_names(q)
        for table in tables:
            if table not in env:
                raise Exception("%s not found" % table)
            df = env[table]
            _write_table(table, df, conn)
        try:
            result = frame_query(q, conn)    
        except:
            result = None
        finally:
            os.remove('.pandasql.db')
        return result

