from mypandas.sqldf import MyPandas

URI = "mysql://root:password@localhost/leetcode"
QUERY = """
SELECT *
FROM Purchases p1, Purchases p2;
"""
print(MyPandas(URI)(QUERY, locals()))
