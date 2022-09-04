from mypandas.sqldf import PandaSQL

URI = "mysql://root:password@localhost/leetcode"
QUERY = """
SELECT *
FROM Purchases p1, Purchases p2;
"""
print(PandaSQL(URI)(QUERY, locals()))
