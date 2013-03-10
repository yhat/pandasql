import pandas as pd
from pandasql import sqldf
import string
import unittest


class PandaSQLTest(unittest.TestCase):

    def setUp(self):
        return

    def test_select(self):
        df = pd.DataFrame({
                 "letter_pos": [i for i in range(len(string.letters))],
                 "l2": list(string.letters)
        })
        result = sqldf("select * from df LIMIT 10;", locals())
        self.assertEquals(len(result), 10)

    def test_join(self):

        df = pd.DataFrame({
            "letter_pos": [i for i in range(len(string.letters))],
            "l2": list(string.letters)
        })

        df2 = pd.DataFrame({
            "letter_pos": [i for i in range(len(string.letters))],
            "letter": list(string.letters)
        })

        result = sqldf("SELECT a.*, b.letter FROM df a INNER JOIN df2 b ON a.l2 = b.letter LIMIT 20;", locals())
        self.assertEquals(len(result), 20)

    def test_query_with_spacing(self):

        df = pd.DataFrame({
            "letter_pos": [i for i in range(len(string.letters))],
            "l2": list(string.letters)
        })

        df2 = pd.DataFrame({
            "letter_pos": [i for i in range(len(string.letters))],
            "letter": list(string.letters)
        })
        
        result = sqldf("SELECT a.*, b.letter FROM df a INNER JOIN df2 b ON a.l2 = b.letter LIMIT 20;", locals())
        self.assertEquals(len(result), 20)

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
        result = sqldf(q, locals())
        self.assertEquals(len(result), 20)


if __name__=="__main__":
    unittest.main()

