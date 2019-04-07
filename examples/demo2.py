import os, time
import pandas as pd
from pandasql import sqldf

# dummy DataFrame
data = [ [ "abc", 123, True, "C:\\temp" ], [ "d'ef", -45.6, False, "C:\\windows" ], [ "xyz", 0.89, 0, "/usr/" ] ]
df = pd.DataFrame(data, columns = [ "id", "n", "b", "f" ])


# define 'pysqldf' as per pandasql documentation, with extra params and user-defined-functions registration

def my_sqlite_connect_listener( dbapi_con, con_record ):
    # registering a few extra functions to SQLite
    dbapi_con.create_function( 'IIF', 3, lambda b, t, f : t if b else f )
    dbapi_con.create_function( 'CUBE', 1, lambda x : x*x*x )
    dbapi_con.create_function( 'FileExists', 1, lambda f : os.path.exists(f) )
    dbapi_con.create_function( 'FileModificationDate', 1, lambda f : time.ctime(os.path.getmtime(f)) if os.path.exists(f) else None)

pysqldf = lambda q, params=None: sqldf(q, globals(), params=params, sqlite_connect_listener=my_sqlite_connect_listener)


# demo of request using the extra functions
print(pysqldf("select n, IIF(n<0, 'n is negative', 'n is positive') from df where id<>?", params = ('abc', )))
print(pysqldf("select CUBE(2), CUBE(3), CUBE(4), CUBE(5)"))
print(pysqldf("select f, FileExists(f), FileModificationDate(f) from df"))
