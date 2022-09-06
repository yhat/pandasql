import inspect
import re
from contextlib import contextmanager
from warnings import catch_warnings, filterwarnings, warn
import urllib

from pandas.io.sql import read_sql, to_sql
from sqlalchemy import create_engine
from sqlalchemy.event import listen
from sqlalchemy.exc import DatabaseError, ResourceClosedError
from sqlalchemy.pool import NullPool

__all__ = ["MyPandas", "PandaSQL", "MyPandasException", "sqldf"]

_PRINT = False
TEMP_DB_NAME = "__MYPANDAS_TEMP"


def _print(*args, **kwargs):
    if _PRINT:
        print(*args, **kwargs)


def _debug_func(func):
    def wrapper(*args, **kwargs):
        _print("QUALNAME", func.__qualname__)
        ans = func(*args, **kwargs)
        _print("OUTPUT", ans)
        return ans

    return wrapper


def _debug(cls):
    for key, value in vars(cls).items():
        if callable(value):
            setattr(cls, key, _debug_func(value))
            continue
    return cls


class MyPandasException(Exception):
    pass


@_debug
class PandaSQL:
    def __init__(self, db_uri="sqlite:///:memory:", persist=False):
        """
        Initialize with a specific database.

        :param db_uri: SQLAlchemy-compatible database URI.
        :param persist: keep tables in database between different calls on the same object of this class.
        """
        self.engine = create_engine(db_uri, poolclass=NullPool)

        if self.engine.name not in ("sqlite", "postgresql", "mysql"):
            raise MyPandasException("Unsupported database engine.")

        if self.engine.name == "sqlite":
            listen(self.engine, "connect", self._set_text_factory)

        if self.engine.name == "mysql":
            self._mysql_datbase = urllib.parse.urlsplit(db_uri).path.replace("/", "")

        self.persist = persist
        self.loaded_tables = set()
        if self.persist:
            self._conn = self.engine.connect()
            self._init_connection(self._conn)

    def __call__(self, query: str, env=None):
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

        with self.conn as conn:
            for table_name in extract_table_names(query):
                if table_name not in env:
                    # don't raise error because the table may be already in the database
                    continue
                if self.persist and table_name in self.loaded_tables:
                    # table was loaded before using the same instance, don't do it again
                    continue
                self.loaded_tables.add(table_name)
                write_table(env[table_name], table_name, conn)

            try:
                result = read_sql(query, conn)
            except DatabaseError as ex:
                raise MyPandasException(ex)
            except ResourceClosedError:
                # query returns nothing
                result = None

        return result

    @property
    @contextmanager
    def conn(self):
        if self.persist:
            # created in __init__
            yield self._conn
            # no cleanup needed
        else:
            conn = self.engine.connect()
            self._init_connection(conn)
            try:
                yield conn
            finally:
                _print("Closing conn")
                if self.engine.name == "mysql" and self._mysql_datbase == "":
                    conn.execute(f"DROP DATABASE {TEMP_DB_NAME};")
                conn.close()

    def _init_connection(self, conn):
        _print("Initing conn")
        if self.engine.name == "postgresql":
            conn.execute("SET search_path TO pg_temp")
        if self.engine.name == "mysql":
            _print("Before", (conn.execute("SELECT DATABASE();").fetchone()))
            if self._mysql_datbase == "":
                conn.execute(f"CREATE DATABASE {TEMP_DB_NAME};")
                conn.execute(f"USE {TEMP_DB_NAME};")
            _print("After", (conn.execute("SELECT DATABASE();").fetchone()))
            # doesnt support BLOB/TEXT:
            # conn.execute("SET default_storage_engine=MEMORY;")
            # conn.execute("SET default_tmp_storage_engine=MEMORY;")

    def _set_text_factory(self, dbapi_con, connection_record):
        # sqlite only
        dbapi_con.text_factory = str


@_debug_func
def get_outer_frame_variables():
    """Get a dict of local and global variables of the first outer frame from another file."""
    cur_filename = inspect.getframeinfo(inspect.currentframe()).filename
    outer_frame = next(
        f
        for f in inspect.getouterframes(inspect.currentframe())
        if f.filename != cur_filename
    )
    variables = {}
    variables.update(**outer_frame.frame.f_globals, **outer_frame.frame.f_locals)
    return variables


@_debug_func
def extract_table_names(query):
    """Extract table names from an SQL query."""
    # a good old fashioned regex. turns out this worked better than actually parsing the code
    tables_blocks = re.findall(
        r"(?:FROM|JOIN)\s+(\w+(?:\s*,\s*\w+)*)", query, re.IGNORECASE
    )
    tables = [tbl for block in tables_blocks for tbl in re.findall(r"\w+", block)]
    return set(tables)


@_debug_func
def write_table(df, tablename, conn):
    """Write a dataframe to the database."""
    with catch_warnings():
        filterwarnings(
            "ignore",
            message=f"The provided table name '{tablename}' is not found exactly as such in the database",
        )
        try:
            df.to_sql(name=tablename, con=conn, index=False)
            # What?:
            # index=not any(name is None for name in df.index.names)
            # load index into db if all levels are named
        except Exception as e:
            _print(f"Exception: {e}")
            _print(f"Table {tablename} already exists, dropping and readding!")
            conn.execute(f"DROP TABLE {tablename};")
            df.to_sql(name=tablename, con=conn, index=False)


@_debug_func
def sqldf(query, env=None, db_uri="sqlite:///:memory:"):
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
    warn(
        "sqldf is depricated, use of the MyPandas class is encouraged.",
        DeprecationWarning,
    )
    return PandaSQL(db_uri)(query, env)


MyPandas = PandaSQL
