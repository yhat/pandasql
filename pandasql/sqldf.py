import pandas as pd
import numpy as np
from pandas.io.sql import to_sql, read_sql
from sqlalchemy import create_engine
import re
from warnings import catch_warnings, filterwarnings
from sqlalchemy.exc import OperationalError


class PandaSQLException(Exception):
    pass


class PandaSQL:
    def __init__(self, db_uri='sqlite:///:memory:'):
        self.engine = create_engine(db_uri)
        if self.engine.name not in ('sqlite', 'postgresql'):
            raise PandaSQLException('Currently only sqlite and postgresql are supported.')

    def __call__(self, query, env={}):
        tables = _extract_table_names(query)
        for table in tables:
            if table not in env:
                continue
            df = env[table]
            df = _ensure_data_frame(df, table)
            _write_table(table, df, self.engine)

        try:
            result = read_sql(query, self.engine)
        except OperationalError as ex:
            raise PandaSQLException(ex)

        return result


def _ensure_data_frame(obj, name):
    """
    obj a python object to be converted to a DataFrame

    Take an object and make sure that it's a pandas data frame.
    Accepts pandas Dataframe, dictionaries, lists, tuples.
    """
    if isinstance(obj, pd.DataFrame):
        df = obj
    elif isinstance(obj, (tuple, list)):
        if len(obj) == 0:
            return pd.Dataframe()

        firstrow = obj[0]

        if isinstance(firstrow, (tuple, list)):
            # multiple-columns
            colnames = ["c%d" % i for i in range(len(firstrow))]
            df = pd.DataFrame(obj, columns=colnames)
        else:
            # mono-column
            df = pd.DataFrame(obj, columns=["c0"])

    if not isinstance(df, pd.DataFrame):
        raise PandaSQLException("%s is not of a supported data type" % name)

    # XXX: what is this for? Tests pass without it.
    for col in df:
        if df[col].dtype == np.int64:
            df[col] = df[col].astype(np.float)

    return df


def _extract_table_names(q):
    """extracts table names from a sql query"""
    # a good old fashioned regex. turns out this worked better than actually parsing the code
    rgx = '(?:FROM|JOIN)\s+([A-Za-z0-9_]+)'
    tables = re.findall(rgx, q, re.IGNORECASE)
    return list(set(tables))


def _write_table(tablename, df, conn):
    """writes a dataframe to the sqlite database"""

    for col in df.columns:
        if re.search("[()]", col):
            raise PandaSQLException("Column name '%s' doesn't match SQL naming conventions" % col)

    with catch_warnings():
        filterwarnings('ignore', message='The provided table name \'%s\' is not found exactly as such in the database' % tablename)
        to_sql(df, name=tablename, con=conn,
               schema='pg_temp' if conn.name == 'postgresql' else None,
               index=not any(name is None for name in df.index.names))


def sqldf(q, env={}, db_uri='sqlite:///:memory:'):
    """
    query pandas data frames using sql syntax

    Parameters
    ----------
    q: string
        a sql query using DataFrames as tables
    env: locals() or globals()
        variable environment; locals() or globals() in your function
        allows sqldf to access the variables in your python environment
    db_uri: string
        SQLAlchemy-compatible database URI

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
    return PandaSQL(db_uri)(q, env)
