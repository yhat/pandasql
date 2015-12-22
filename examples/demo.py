from sklearn.datasets import load_iris
import pandas as pd
from pandasql import sqldf
from pandasql import load_meat, load_births
import re

births = load_births()
meat = load_meat()
iris = load_iris()
iris_df = pd.DataFrame(iris.data, columns=iris.feature_names)
iris_df['species'] = pd.Categorical.from_codes(iris.target, iris.target_names)
iris_df.columns = [re.sub("[() ]", "", col) for col in iris_df.columns]

print(sqldf("SELECT * FROM iris_df LIMIT 10;", locals()))
print(sqldf("SELECT sepalwidthcm, species FROM iris_df LIMIT 10;", locals()))

q = """
      select
        species
        , avg(sepalwidthcm)
        , min(sepalwidthcm)
        , max(sepalwidthcm)
      from
        iris_df
      group by
        species;
        
"""
print("*" * 80)
print("aggregation")
print("-" * 80)
print(q)
print(sqldf(q, locals()))


def pysqldf(q):
    "add this to your script if you get tired of calling locals()"
    return sqldf(q, globals())


print("*" * 80)
print("calling from a helper function")
print('''def pysqldf(q):)
    "add this to your script if you get tired of calling locals()"
        return sqldf(q, globals())''')
print("-" * 80)
print(q)
print(pysqldf(q))

q = """
    select
        a.*
    from
        iris_df a
    inner join
        iris_df b
            on a.species = b.species
    limit 10;
"""

print("*" * 80)
print("joins")
print("-" * 80)
print(q)
print(pysqldf(q))

q = """
    select
        *
    from
        iris_df
    where
        species = 'virginica'
        and sepallengthcm > 7.7;
"""
print("*" * 80)
print("where clause")
print("-" * 80)
print(q)
print(pysqldf(q))
iris_df['id'] = range(len(iris_df))
q = """
    select
        *
    from
        iris_df
    where
        id in (select id from iris_df where sepalwidthcm*sepallengthcm > 25);
"""
print("*" * 80)
print("subqueries")
print("-" * 80)
print(q)
print(pysqldf(q))

q = """
    SELECT
        m.*
        , b.births
    FROM
        meat m
    INNER JOIN
        births b
            on m.date = b.date
    ORDER BY
        m.date;
"""

print(pysqldf(q).head())
