pandasql
========

This is a fork of the original pandasql, with support of multiple SQL
backends and more convenient interface. See below for more info.

``pandasql`` allows you to query ``pandas`` DataFrames using SQL syntax.
It works similarly to ``sqldf`` in R. ``pandasql`` seeks to provide a
more familiar way of manipulating and cleaning data for people new to
Python or ``pandas``.

Installation
============

::

    $ pip install -U pandasql

Basics
======

In addition to the original pandasql's ``sqldf`` function this fork has
a class ``PandaSQL``, which new users are encouraged to use.

``sqldf`` function
------------------

The main function used in original pandasql is ``sqldf``. ``sqldf``
accepts one three parameters: - sql query string, - dict of environment
variables (optional, if not specified assumed to be
``{**locals(), **globals()}``) - database URI in the same format as in
SQLAlchemy (optional, by default use in-memory SQLite database)

``PandaSQL`` class
------------------

The class is more convenient when you need to perform multiple queries
(almost always): - first create the class, specifying db\_uri if not
default: ``pdsql = PandaSQL(db_uri)`` - to execute queries just call
``pdsql(query)`` (environment can also be specified expicitly)

Querying
^^^^^^^^

Any ``pandas`` dataframes will be automatically detected by ``pandasql``
and you can query them as you would any regular SQL table.

::

    $ python
    >>> from pandasql import PandaSQL, load_meat, load_births
    >>> meat = load_meat()
    >>> births = load_births()
    >>> pdsql = PandaSQL()
    >>> print pdsql("SELECT * FROM meat LIMIT 10;").head()
                      date  beef  veal  pork  lamb_and_mutton broilers other_chicken turkey
    0  1944-01-01 00:00:00   751    85  1280               89     None          None   None
    1  1944-02-01 00:00:00   713    77  1169               72     None          None   None
    2  1944-03-01 00:00:00   741    90  1128               75     None          None   None
    3  1944-04-01 00:00:00   650    89   978               66     None          None   None
    4  1944-05-01 00:00:00   681   106  1029               78     None          None   None

joins and aggregations are also supported

::

    >>> q = """SELECT
            m.date, m.beef, b.births
         FROM
            meats m
         INNER JOIN
            births b
               ON m.date = b.date;"""
    >>> joined = pdsql(q)
    >>> print joined.head()
                        date    beef  births
    403  2012-07-01 00:00:00  2200.8  368450
    404  2012-08-01 00:00:00  2367.5  359554
    405  2012-09-01 00:00:00  2016.0  361922
    406  2012-10-01 00:00:00  2343.7  347625
    407  2012-11-01 00:00:00  2206.6  320195

    >>> q = "select
               strftime('%Y', date) as year
               , SUM(beef) as beef_total
               FROM
                  meat
               GROUP BY
                  year;"
    >>> print pdsql(q).head()
       year  beef_total
    0  1944        8801
    1  1945        9936
    2  1946        9010
    3  1947       10096
    4  1948        8766

More information and code samples (by the author of the original
version) available in the
`examples <https://github.com/yhat/pandasql/blob/master/examples/demo.py>`__
folder or on `his
blog <http://blog.yhathq.com/posts/pandasql-sql-for-pandas-dataframes.html>`__.
