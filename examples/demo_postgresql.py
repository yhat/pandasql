from pandas import DataFrame
from pandasql import PandaSQL

# a dataframe
df1 = DataFrame({"a": [1, 2, 3, 1], "b": [7, 1, 5, 3]})

# establish postgresql connection using psycopg2 driver
# log in with username: tubh; password: tubh; using unix socket at /tmp
sqlClass = PandaSQL("postgresql+psycopg2://tubh:tubh@/tubh?host=/tmp")

# try function GREATEST which is not supported by sqlite by is supported by postgresql
result = sqlClass(("select GREATEST(tbl.a, tbl.b) as maxA_B from tbl"), {"tbl": df1})
print(result)
