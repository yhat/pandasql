pandasql
========

sqldf for pandas

<pre>
   sepallengthcm  sepalwidthcm  petallengthcm  petalwidthcm species
0            5.1           3.5            1.4           0.2  setosa
1            4.9           3.0            1.4           0.2  setosa
2            4.7           3.2            1.3           0.2  setosa
3            4.6           3.1            1.5           0.2  setosa
4            5.0           3.6            1.4           0.2  setosa
5            5.4           3.9            1.7           0.4  setosa
6            4.6           3.4            1.4           0.3  setosa
7            5.0           3.4            1.5           0.2  setosa
8            4.4           2.9            1.4           0.2  setosa
9            4.9           3.1            1.5           0.1  setosa
   sepalwidthcm species
0           3.5  setosa
1           3.0  setosa
2           3.2  setosa
3           3.1  setosa
4           3.6  setosa
5           3.9  setosa
6           3.4  setosa
7           3.4  setosa
8           2.9  setosa
9           3.1  setosa
********************************************************************************
aggregation
--------------------------------------------------------------------------------

      select
        species
        , avg(sepalwidthcm)
        , min(sepalwidthcm)
        , max(sepalwidthcm)
      from
        iris_df
      group by
        species;
        

      species  avg(sepalwidthcm)  min(sepalwidthcm)  max(sepalwidthcm)
0      setosa              3.418                2.3                4.4
1  versicolor              2.770                2.0                3.4
2   virginica              2.974                2.2                3.8
********************************************************************************
calling from a helper function
def pysqldf(q):
    "add this to your script if you get tired of calling locals()"
        return sqldf(q, globals())
--------------------------------------------------------------------------------

      select
        species
        , avg(sepalwidthcm)
        , min(sepalwidthcm)
        , max(sepalwidthcm)
      from
        iris_df
      group by
        species;
        

      species  avg(sepalwidthcm)  min(sepalwidthcm)  max(sepalwidthcm)
0      setosa              3.418                2.3                4.4
1  versicolor              2.770                2.0                3.4
2   virginica              2.974                2.2                3.8
********************************************************************************
joins
--------------------------------------------------------------------------------

    select
        a.*
    from
        iris_df a
    inner join
        iris_df b
            on a.species = b.species
    limit 10;

   sepallengthcm  sepalwidthcm  petallengthcm  petalwidthcm species
0            5.1           3.5            1.4           0.2  setosa
1            5.1           3.5            1.4           0.2  setosa
2            5.1           3.5            1.4           0.2  setosa
3            5.1           3.5            1.4           0.2  setosa
4            5.1           3.5            1.4           0.2  setosa
5            5.1           3.5            1.4           0.2  setosa
6            5.1           3.5            1.4           0.2  setosa
7            5.1           3.5            1.4           0.2  setosa
8            5.1           3.5            1.4           0.2  setosa
9            5.1           3.5            1.4           0.2  setosa
********************************************************************************
where clause
--------------------------------------------------------------------------------

    select
        *
    from
        iris_df
    where
        species = 'virginica'
        and sepallengthcm > 7.7;

   sepallengthcm  sepalwidthcm  petallengthcm  petalwidthcm    species
0            7.9           3.8            6.4             2  virginica
********************************************************************************
subqueries
--------------------------------------------------------------------------------

    select
        *
    from
        iris_df
    where
        id in (select id from iris_df where sepalwidthcm*sepallengthcm > 25);

   sepallengthcm  sepalwidthcm  petallengthcm  petalwidthcm    species   id
0            5.7           4.4            1.5           0.4     setosa   15
1            7.2           3.6            6.1           2.5  virginica  109
2            7.7           3.8            6.7           2.2  virginica  117
3            7.9           3.8            6.4           2.0  virginica  131

</pre>
