from shared import add_mypandas_to_path  # isort: ignore

add_mypandas_to_path()
import pytest

from mypandas.sqldf import extract_table_names, get_outer_frame_variables


def test_get_vars():
    var_a = 123
    variable = {"a": "b", "c": "d"}
    assert get_outer_frame_variables()["var_a"] == var_a
    assert get_outer_frame_variables()["variable"] == variable


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        ("", set()),
        ("SELECT * FROM dbtbl LIMIT 10", {"dbtbl"}),
        ("SELECT * from dbtbl LIMIT 10", {"dbtbl"}),
        (
            "SELECT * FROM dbtbl JOIN table_2 USING (a) LEFT JOIN mytable USING (b)",
            {"dbtbl", "table_2", "mytable"},
        ),
        (
            "SELECT * FROM dbtbl, table_2, mytable JOIN another USING (a)",
            {"dbtbl", "table_2", "mytable", "another"},
        ),
        ("SELECT * FROM dbtbl, table_2 JOIN dbtbl USING (a)", {"dbtbl", "table_2"}),
        pytest.mark.xfail(
            ("SELECT * FROM dbtbl WHERE col = 'Go and join us!'", {"dbtbl"})
        )(  # type: ignore,
            "SELECT * FROM course_df WHERE coursecode IN ( SELECT DISTINCT coursecode FROM program_df )",
            {"course_df", "program_df"},
        ),
        (
            "SELECT * FROM course_df, (SELECT coursecode FROM program_df) ssel",
            {"course_df", "program_df"},
        ),
    ],
)
def test_extract_table_names(query, expected):
    assert extract_table_names(query) == expected
