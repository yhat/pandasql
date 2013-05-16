import sqlite3 as sqlite
import sqlparse
from sqlparse.tokens import Whitespace
from pandas.io.sql import write_frame, frame_query
import os
import re


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
    df = pd.DataFame({
        x: range(100),
        y: range(100)
    })

    from pandasql import sqldf
    sqldf("select * from df;", locals())
    sqldf("select avg(x) from df;", locals())
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
        if hasattr(df, 'columns') :
            #standard case : we have a panda frame
            _write_table(table, df, conn)
        elif hasattr(df,'items') and hasattr(dict,'keys'):
            #other case :a dictionnary
            df_list=[(k,v) for k, v in df.items()]
            df_real=pd.DataFrame(df_list, columns=['c'+str(i) for i in range(len(df_list[0]))])
            _write_table(table, df_real, conn)
        elif hasattr(df,'index') :
            #other case : we suppose we have a simple python list
            #we build columns title "Ci" where i is going from 1 to the number of columns-1
            first_record=df[0]
            first_record_len=1
            if hasattr(first_record,'index') and not hasattr(first_record,'upper'):
                #standard case, multi-column list
                first_record_len=len(df[0])
                df_real=pd.DataFrame(df, columns=['c'+str(i) for i in range(first_record_len)])
                _write_table(table, df_real, conn)
            else:
                #mono-column case with not an integer at first record
                if not type(df[0])=='builtins.int' :
                    df_real=pd.DataFrame(df, columns=[ 'c0' ])
                    _write_table(table, df_real, conn)
                else:    
                    #creating floats to workaround a bug (in  ??) if first record is an integer
                    df_real=pd.DataFrame([x*1.0 for x in df], columns=[ 'c0' ])
                    print('r')
                    _write_table(table, df_real, conn)    
        else :
            raise Exception("%s is not a panda object, a list, nor a dictionary" % table)
    try:
        result = frame_query(q, conn)
    except:
        result = None
    finally:
        conn.close()
        if not inmemory:
            os.remove(dbname)
    return result

