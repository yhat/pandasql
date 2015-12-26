import inspect
from pandas.io.sql import to_sql, read_sql
from sqlalchemy import create_engine
import re
from warnings import catch_warnings, filterwarnings
from sqlalchemy.exc import DatabaseError, ResourceClosedError
from sqlalchemy.pool import NullPool


__all__ = ['PandaSQL', 'PandaSQLException', 'sqldf']


class PandaSQLException(Exception):
    pass


class PandaSQL:
    def __init__(self, db_uri='sqlite:///:memory:', persist=False):
        """
        Initialize with a specific database.

        :param db_uri: SQLAlchemy-compatible database URI.
        :param persist: keep tables in database between different calls on the same object of this class.
        """
        self.engine = create_engine(db_uri, poolclass=NullPool)
        if self.engine.name not in ('sqlite', 'postgresql'):
            raise PandaSQLException('Currently only sqlite and postgresql are supported.')

        self.persist = persist
        if self.persist:
            self.loaded_tables = set()
            self.conn = self.engine.connect()
            self._init_connection(self.conn)

    def __call__(self, query, env=None):
        """
        Execute the SQL query.
        Automatically creates tables mentioned in the query from dataframes before executing.

        :param query: SQL query string, which can reference pandas dataframes as SQL tables.
        :param env: Variables environment - a dict mapping table names to pandas dataframes.
        If not specified use local and global variables of the caller.
        :return: Pandas dataframe with the result of the SQL query.
        """
        if env is None:
            env = get_outer_frame_variables()

        if self.persist:
            for table_name in extract_table_names(query):
                if table_name not in env:
                    continue
                if table_name in self.loaded_tables:
                    continue
                self.loaded_tables.add(table_name)
                write_table(env[table_name], table_name, self.conn)

            try:
                result = read_sql(query, self.conn)
            except DatabaseError as ex:
                raise PandaSQLException(ex)
            except ResourceClosedError:
                # query returns nothing
                result = None
        else:
            with self.engine.connect() as conn:
                self._init_connection(conn)
                for table_name in extract_table_names(query):
                    if table_name not in env:
                        continue
                    write_table(env[table_name], table_name, conn)

                try:
                    result = read_sql(query, conn)
                except DatabaseError as ex:
                    raise PandaSQLException(ex)
                except ResourceClosedError:
                    # query returns nothing
                    result = None

        return result

    def _init_connection(self, conn):
        if self.engine.name == 'postgresql':
            conn.execute('set search_path to pg_temp')


def get_outer_frame_variables():
    """ Get a dict of local and global variables of the first outer frame from another file. """
    cur_filename = inspect.getframeinfo(inspect.currentframe()).filename
    outer_frame = next(f
                       for f in inspect.getouterframes(inspect.currentframe())
                       if f.filename != cur_filename)
    return {**outer_frame.frame.f_globals,
            **outer_frame.frame.f_locals}


def extract_table_names(query):
    """ Extract table names from an SQL query. """
    # a good old fashioned regex. turns out this worked better than actually parsing the code
    tables_blocks = re.findall(r'(?:FROM|JOIN)\s+(\w+(?:\s*,\s*\w+)*)', query, re.IGNORECASE)
    tables = [tbl
              for block in tables_blocks
              for tbl in re.findall(r'\w+', block)]
    return set(tables)


def write_table(df, tablename, conn):
    """ Write a dataframe to the database. """
    with catch_warnings():
        filterwarnings('ignore',
                       message='The provided table name \'%s\' is not found exactly as such in the database' % tablename)
        to_sql(df, name=tablename, con=conn,
               schema='pg_temp' if conn.engine.name == 'postgresql' else None,
               index=not any(name is None for name in df.index.names))


def sqldf(query, env=None, db_uri='sqlite:///:memory:'):
    """
    Query pandas data frames using sql syntax
    This function is meant for backward compatibility only. New users are encouraged to use the PandaSQL class.

    Parameters
    ----------
    query: string
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
    return PandaSQL(db_uri)(query, env)
