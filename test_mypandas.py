from mypandas.sqldf import MyPandas
from mypandas import load_births
import pandas as pd

births = load_births()
assert type(births) == pd.DataFrame
URI = "mysql://root:password@localhost/"
QUERY = """
-- Yup, this works!
WITH cte AS (
    SELECT b1.date d1, b1.births b1, b2.date d2, b2.births b2
    FROM births b1, births b2
)
SELECT * FROM cte;
"""
print(MyPandas(URI)(QUERY, locals()))
