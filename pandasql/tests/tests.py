import pandas as pd
from pandasql import sqldf, load_meat
import string
import pytest


@pytest.fixture(params=['sqlite:///:memory:', 'postgresql://postgres@localhost/'])
def db_uri(request):
    return request.param


def test_select(db_uri):
    df = pd.DataFrame({
        "letter_pos": [i for i in range(len(string.ascii_letters))],
        "l2": list(string.ascii_letters)
    })
    result = sqldf("SELECT * FROM df LIMIT 10;", locals(), db_uri)
    assert len(result) == 10


def test_join(db_uri):
    df = pd.DataFrame({
        "letter_pos": [i for i in range(len(string.ascii_letters))],
        "l2": list(string.ascii_letters)
    })

    df2 = pd.DataFrame({
        "letter_pos": [i for i in range(len(string.ascii_letters))],
        "letter": list(string.ascii_letters)
    })

    result = sqldf("SELECT a.*, b.letter FROM df a INNER JOIN df2 b ON a.l2 = b.letter LIMIT 20;", locals(), db_uri)
    assert len(result) == 20


def test_query_with_spacing(db_uri):
    df = pd.DataFrame({
        "letter_pos": [i for i in range(len(string.ascii_letters))],
        "l2": list(string.ascii_letters)
    })

    df2 = pd.DataFrame({
        "letter_pos": [i for i in range(len(string.ascii_letters))],
        "letter": list(string.ascii_letters)
    })

    result = sqldf("SELECT a.*, b.letter FROM df a INNER JOIN df2 b ON a.l2 = b.letter LIMIT 20;", locals(), db_uri)
    assert len(result) == 20

    q = """
        SELECT
        a.*
    FROM
        df a
    INNER JOIN
        df2 b
    on a.l2 = b.letter
    LIMIT 20
    ;"""
    result = sqldf(q, locals(), db_uri)
    assert len(result) == 20


def test_query_single_list(db_uri):
    mylist = [i for i in range(10)]

    result = sqldf("SELECT * FROM mylist", locals(), db_uri)
    assert len(result) == 10


def test_query_list_of_lists(db_uri):
    mylist = [[i for i in range(10)], [i for i in range(10)]]

    result = sqldf("SELECT * FROM mylist", locals(), db_uri)
    assert len(result) == 2


def test_query_list_of_tuples(db_uri):
    mylist = [tuple([i for i in range(10)]), tuple([i for i in range(10)])]

    result = sqldf("SELECT * FROM mylist", locals(), db_uri)
    assert len(result) == 2


def test_subquery(db_uri):
    kermit = pd.DataFrame({"x": range(10)})
    q = "SELECT * FROM (SELECT * FROM kermit) tbl LIMIT 2;"
    result = sqldf(q, locals(), db_uri)
    assert len(result) == 2


def test_in(db_uri):
    course_data = {
        'coursecode': ['TM351', 'TU100', 'M269'],
        'points': [30, 60, 30],
        'level': ['3', '1', '2']
    }
    course_df = pd.DataFrame(course_data)
    q = "SELECT * FROM course_df WHERE coursecode IN ( 'TM351', 'TU100' );"
    result = sqldf(q, locals(), db_uri)
    assert len(result) == 2


def test_in_with_subquery(db_uri):
    program_data = {
        'coursecode': ['TM351', 'TM351', 'TM351', 'TU100', 'TU100', 'TU100', 'M269', 'M269', 'M269'],
        'programCode': ['AB1', 'AB2', 'AB3', 'AB1', 'AB3', 'AB4', 'AB3', 'AB4', 'AB5']
    }
    program_df = pd.DataFrame(program_data)

    courseData = {
        'coursecode': ['TM351', 'TU100', 'M269'],
        'points': [30, 60, 30],
        'level': ['3', '1', '2']
    }
    course_df = pd.DataFrame(courseData)

    q = '''
        SELECT * FROM course_df WHERE coursecode IN ( SELECT DISTINCT coursecode FROM program_df ) ;
      '''
    result = sqldf(q, locals(), db_uri)
    assert len(result) == 3


def test_datetime_query(db_uri):
    meat = load_meat()
    result = sqldf("SELECT * FROM meat LIMIT 10;", locals(), db_uri)
    assert len(result) == 10


def test_returning_none(db_uri):
    meat = load_meat()
    result = sqldf("SELECT beef FROM meat LIMIT 10;", locals(), db_uri)
    assert len(result) == 10
