pandasql
========

sqldf for pandas


<pre>

from sklearn.datasets import load_iris
import pandas as pd
from pandasql import sqldf
import re


iris = load_iris()
iris_df = pd.DataFrame(iris.data, columns=iris.feature_names)
iris_df['species'] = pd.Factor(iris.target, levels=iris.target_names)
iris_df.columns = [re.sub("[() ]", "", col) for col in iris_df.columns]

print sqldf("select * from iris_df limit 10;", locals())
print sqldf("select sepalwidthcm, species from iris_df limit 10;", locals())

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
print "*"*80
print "aggregation"
print "-"*80
print q
print sqldf(q, locals())


def pysqldf(q):
    "add this to your script if you get tired of calling locals()"
    return sqldf(q, globals())

print "*"*80
print "calling from a helper function"
print "-"*80
print q
print pysqldf(q)


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

print "*"*80
print "joins"
print "-"*80
print q
print pysqldf(q)


q = """
    select
        *
    from
        iris_df
    where
        species = 'virginica';
"""
print "*"*80
print "where clause"
print "-"*80
print q
print pysqldf(q)
iris_df['id'] = range(len(iris_df))
q = """
    select
        *
    from
        iris_df
    where
        id in (select id from iris_df where sepalwidthcm*sepallengthcm > 20);
"""
print "*"*80
print "subqueries"
print "-"*80
print q
print pysqldf(q)

</pre>
