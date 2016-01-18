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


@pytest.fixture(params=[False, True])
def pdsql(db_uri, request):
    return PandaSQL(db_uri, persist=request.param)


def test_select_legacy(db_uri):
    df = pd.DataFrame({
        "letter_pos": range(len(string.ascii_letters)),
        "l2": list(string.ascii_letters)
    })
    result = sqldf("SELECT * FROM df LIMIT 10", db_uri=db_uri)

    assert len(result) == 10
    pdtest.assert_frame_equal(df.head(10), result)


def test_select(pdsql):
    df = pd.DataFrame({
        "letter_pos": range(len(string.ascii_letters)),
        "l2": list(string.ascii_letters)
    })
    result = pdsql("SELECT * FROM df LIMIT 10")

    assert len(result) == 10
    pdtest.assert_frame_equal(df.head(10), result)


def test_join(pdsql):
    df = pd.DataFrame({
        "letter_pos": range(len(string.ascii_letters)),
        "l2": list(string.ascii_letters)
    })

    df2 = pd.DataFrame({
        "letter_pos": range(len(string.ascii_letters)),
        "letter": list(string.ascii_letters)
    })

    result = pdsql("SELECT a.*, b.letter FROM df a INNER JOIN df2 b ON a.l2 = b.letter LIMIT 20")

    assert len(result) == 20
    pdtest.assert_frame_equal(df[['letter_pos', 'l2']].head(20), result[['letter_pos', 'l2']])
    pdtest.assert_frame_equal(df2[['letter']].head(20), result[['letter']])


def test_query_with_spacing(pdsql):
    df = pd.DataFrame({
        "letter_pos": range(len(string.ascii_letters)),
        "l2": list(string.ascii_letters)
    })

    df2 = pd.DataFrame({
        "letter_pos": range(len(string.ascii_letters)),
        "letter": list(string.ascii_letters)
    })

    expected = pdsql("SELECT a.*, b.letter FROM df a INNER JOIN df2 b ON a.l2 = b.letter LIMIT 20")

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
    result = pdsql(q)
    assert len(result) == 20
    pdtest.assert_frame_equal(expected, result)


def test_subquery(pdsql):
    kermit = pd.DataFrame({"x": range(10)})
    result = pdsql("SELECT * FROM (SELECT * FROM kermit) tbl LIMIT 2")
    pdtest.assert_frame_equal(kermit.head(2), result)
    assert len(result) == 2


def test_in(pdsql):
    course_data = {
        'coursecode': ['TM351', 'TU100', 'M269'],
        'points': [30, 60, 30],
        'level': ['3', '1', '2']
    }
    course_df = pd.DataFrame(course_data)
    result = pdsql("SELECT * FROM course_df WHERE coursecode IN ( 'TM351', 'TU100' )")
    assert len(result) == 2


def test_in_with_subquery(pdsql):
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

    result = pdsql("SELECT * FROM course_df WHERE coursecode IN ( SELECT DISTINCT coursecode FROM program_df )")
    assert len(result) == 3


def test_datetime_query(pdsql, db_flavor):
    meat = load_meat()
    expected = meat[meat['date'] >= '2012-01-01'].reset_index(drop=True)
    result = pdsql("SELECT * FROM meat WHERE date >= '2012-01-01'")
    if db_flavor == 'sqlite':
        # sqlite uses strings instead of datetimes
        pdtest.assert_frame_equal(expected.drop('date', 1), result.drop('date', 1))
    else:
        pdtest.assert_frame_equal(expected, result)


def test_returning_single(pdsql):
    meat = load_meat()
    result = pdsql("SELECT beef FROM meat LIMIT 10")
    assert len(result) == 10
    pdtest.assert_frame_equal(meat[['beef']].head(10), result)


def test_name_index(pdsql):
    df = pd.DataFrame({
        "index": range(len(string.ascii_letters)),
        "level_0": range(len(string.ascii_letters)),
        "level_1": range(len(string.ascii_letters)),
        "letter": list(string.ascii_letters)
    })
    result = pdsql("SELECT * FROM df")
    pdtest.assert_frame_equal(df, result)


def test_nonexistent_table(pdsql):
    with pytest.raises(PandaSQLException):
        pdsql("SELECT * FROM nosuchtablereally")


def test_system_tables(pdsql, db_flavor):
    if db_flavor == 'sqlite':
        # sqlite doesn't have information_schema
        result = pdsql("SELECT * FROM sqlite_master")
    else:
        result = pdsql("SELECT * FROM information_schema.tables")
    assert len(result.columns) > 1


@pytest.mark.parametrize('db_flavor', ['postgres'])  # sqlite doesn't support tables with no columns
def test_no_columns(pdsql):
    df = pd.DataFrame()
    result = pdsql("SELECT * FROM df")
    pdtest.assert_frame_equal(df, result)


def test_empty(pdsql):
    df = pd.DataFrame({'x': []})
    result = pdsql("SELECT * FROM df")
    assert result.empty
    pdtest.assert_index_equal(df.columns, result.columns)


def test_noleak_legacy(db_uri):
    df = pd.DataFrame({'x': [1]})
    result = sqldf("SELECT * FROM df", db_uri=db_uri)
    pdtest.assert_frame_equal(df, result)
    del df
    with pytest.raises(PandaSQLException):
        result = sqldf("SELECT * FROM df", db_uri=db_uri)


@pytest.mark.parametrize('pdsql', [False], indirect=True)
def test_noleak_class(pdsql):
    df = pd.DataFrame({'x': [1]})
    result = pdsql("SELECT * FROM df")
    pdtest.assert_frame_equal(df, result)
    del df
    with pytest.raises(PandaSQLException):
        result = pdsql("SELECT * FROM df")


def test_same_query_noerr(pdsql):
    df = pd.DataFrame({'x': [1]})
    result1 = pdsql("SELECT * FROM df")
    pdtest.assert_frame_equal(df, result1)
    result2 = pdsql("SELECT * FROM df")
    pdtest.assert_frame_equal(result1, result2)


@pytest.mark.parametrize('pdsql', [True], indirect=True)
def test_persistent(pdsql):
    df = pd.DataFrame({'x': [1]})
    result1 = pdsql("SELECT * FROM df")
    pdtest.assert_frame_equal(df, result1)

    del df
    result2 = pdsql("SELECT * FROM df")
    pdtest.assert_frame_equal(result1, result2)

    df = pd.DataFrame({'x': [1, 2]})  # will not have any effect
    result3 = pdsql("SELECT * FROM df")
    pdtest.assert_frame_equal(result1, result3)

    df1 = pd.DataFrame({'x': [1, 2, 3]})
    result4 = pdsql("SELECT * FROM df1")
    pdtest.assert_frame_equal(df1, result4)


def test_noreturn_query(pdsql):
    assert pdsql("CREATE TABLE tbl (col INTEGER)") is None


@pytest.mark.parametrize('pdsql', [False], indirect=True)
def test_no_sideeffect_leak(pdsql):
    pdsql("CREATE TABLE tbl (col INTEGER)")
    with pytest.raises(PandaSQLException):
        result = pdsql("SELECT * FROM tbl")


@pytest.mark.parametrize('pdsql', [True], indirect=True)
def test_sideeffect_persist(pdsql):
    pdsql("CREATE TABLE tbl (col INTEGER)")
    result = pdsql("SELECT * FROM tbl")
    assert list(result.columns) == ['col']
