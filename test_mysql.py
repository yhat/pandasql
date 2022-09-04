import inspect
import re
from contextlib import contextmanager
from warnings import catch_warnings, filterwarnings

from pandas.io.sql import read_sql, to_sql
from sqlalchemy import create_engine
from sqlalchemy.event import listen
from sqlalchemy.exc import DatabaseError, ResourceClosedError
from sqlalchemy.pool import NullPool

# engine = create_engine("mysql://root@localhost/leetcode")
# print(f"{engine.name=}")


# def _set_text_factory(dbapi_con, connection_record):
#     dbapi_con.text_factory = str


# listen(engine, "connect", _set_text_factory)

from mypandas.sqldf import PandaSQL

URI = "mysql://root@localhost/leetcode"
PandaSQL(URI, locals())
