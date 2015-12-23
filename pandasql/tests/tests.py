import pandas as pd
from pandasql import PandaSQL, PandaSQLException, sqldf, load_meat
import string
import pytest
import pandas.util.testing as pdtest


@pytest.fixture()
def db_uris():
    return {
        'sqlite': 'sqlite:///:memory:',
        'postgres': 'postgresql://postgres@localhost/'
    }


@pytest.fixture(params=['sqlite', 'postgres'])
def db_flavor(request):
    return request.param


@pytest.fixture()
def db_uri(db_uris, db_flavor):
    return db_uris[db_flavor]


@pytest.fixture()
def pandasql(db_uri):
    return PandaSQL(db_uri)


def test_select(db_uri):
    df = pd.DataFrame({
        "letter_pos": [i for i in range(len(string.ascii_letters))],
        "l2": list(string.ascii_letters)
    })
    result = sqldf("SELECT * FROM df LIMIT 10", db_uri=db_uri)

    assert len(result) == 10
    pdtest.assert_frame_equal(df.head(10), result)


def test_select_using_class(pandasql):
    df = pd.DataFrame({
        "letter_pos": [i for i in range(len(string.ascii_letters))],
        "l2": list(string.ascii_letters)
    })
    result = pandasql("SELECT * FROM df LIMIT 10")

    assert len(result) == 10
    pdtest.assert_frame_equal(df.head(10), result)


def test_join(db_uri):
    df = pd.DataFrame({
        "letter_pos": [i for i in range(len(string.ascii_letters))],
        "l2": list(string.ascii_letters)
    })

    df2 = pd.DataFrame({
        "letter_pos": [i for i in range(len(string.ascii_letters))],
        "letter": list(string.ascii_letters)
    })

    result = sqldf("SELECT a.*, b.letter FROM df a INNER JOIN df2 b ON a.l2 = b.letter LIMIT 20", db_uri=db_uri)

    assert len(result) == 20
    pdtest.assert_frame_equal(df[['letter_pos', 'l2']].head(20), result[['letter_pos', 'l2']])
    pdtest.assert_frame_equal(df2[['letter']].head(20), result[['letter']])


def test_query_with_spacing(db_uri):
    df = pd.DataFrame({
        "letter_pos": [i for i in range(len(string.ascii_letters))],
        "l2": list(string.ascii_letters)
    })

    df2 = pd.DataFrame({
        "letter_pos": [i for i in range(len(string.ascii_letters))],
        "letter": list(string.ascii_letters)
    })

    expected = sqldf("SELECT a.*, b.letter FROM df a INNER JOIN df2 b ON a.l2 = b.letter LIMIT 20", db_uri=db_uri)

    q = """
        SELECT
        a.*
    , b.letter
    FROM
        df a
    INNER JOIN
        df2 b
    on a.l2 = b.letter
    LIMIT 20
    """
    result = sqldf(q, db_uri=db_uri)
    assert len(result) == 20
    pdtest.assert_frame_equal(expected, result)


def test_subquery(db_uri):
    kermit = pd.DataFrame({"x": range(10)})
    result = sqldf("SELECT * FROM (SELECT * FROM kermit) tbl LIMIT 2", db_uri=db_uri)
    pdtest.assert_frame_equal(kermit.head(2), result)
    assert len(result) == 2


def test_in(db_uri):
    course_data = {
        'coursecode': ['TM351', 'TU100', 'M269'],
        'points': [30, 60, 30],
        'level': ['3', '1', '2']
    }
    course_df = pd.DataFrame(course_data)
    result = sqldf("SELECT * FROM course_df WHERE coursecode IN ( 'TM351', 'TU100' )", db_uri=db_uri)
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

    result = sqldf("SELECT * FROM course_df WHERE coursecode IN ( SELECT DISTINCT coursecode FROM program_df )", db_uri=db_uri)
    assert len(result) == 3


def test_datetime_query(db_uri, db_flavor):
    meat = load_meat()
    expected = meat[meat['date'] >= '2012-01-01'].reset_index(drop=True)
    result = sqldf("SELECT * FROM meat WHERE date >= '2012-01-01'", db_uri=db_uri)
    if db_flavor == 'sqlite':
        # sqlite uses strings instead of datetimes
        pdtest.assert_frame_equal(expected.drop('date', 1), result.drop('date', 1))
    else:
        pdtest.assert_frame_equal(expected, result)


def test_returning_single(db_uri):
    meat = load_meat()
    result = sqldf("SELECT beef FROM meat LIMIT 10", db_uri=db_uri)
    assert len(result) == 10
    pdtest.assert_frame_equal(meat[['beef']].head(10), result)


def test_name_index(db_uri):
    df = pd.DataFrame({
        "index": [i for i in range(len(string.ascii_letters))],
        "level_0": [i for i in range(len(string.ascii_letters))],
        "level_1": [i for i in range(len(string.ascii_letters))],
        "letter": list(string.ascii_letters)
    })
    result = sqldf("SELECT * FROM df", db_uri=db_uri)
    pdtest.assert_frame_equal(df, result)


def test_nonexistent_table(db_uri):
    with pytest.raises(PandaSQLException):
        sqldf("SELECT * FROM nosuchtablereally")


def test_system_tables(db_uri, db_flavor):
    if db_flavor == 'sqlite':
        # sqlite doesn't have information_schema
        result = sqldf("SELECT * FROM sqlite_master", db_uri=db_uri)
    else:
        result = sqldf("SELECT * FROM information_schema.tables", db_uri=db_uri)
    assert len(result.columns) > 1


@pytest.mark.parametrize('db_flavor', ['postgres'])  # sqlite doesn't support tables with no columns
def test_no_columns(db_uri):
    df = pd.DataFrame()
    result = sqldf("SELECT * FROM df", db_uri=db_uri)
    pdtest.assert_frame_equal(df, result)


def test_empty(db_uri):
    df = pd.DataFrame({'x': []})
    result = sqldf("SELECT * FROM df", db_uri=db_uri)
    assert result.empty
    pdtest.assert_index_equal(df.columns, result.columns)


def test_noleak(db_uri):
    df = pd.DataFrame({'x': [1]})
    result = sqldf("SELECT * FROM df", db_uri=db_uri)
    pdtest.assert_frame_equal(df, result)
    with pytest.raises(PandaSQLException):
        result = sqldf("SELECT * FROM df", {}, db_uri=db_uri)


def test_noleak_class(pandasql):
    df = pd.DataFrame({'x': [1]})
    result = pandasql("SELECT * FROM df")
    pdtest.assert_frame_equal(df, result)
    with pytest.raises(PandaSQLException):
        result = pandasql("SELECT * FROM df", {})