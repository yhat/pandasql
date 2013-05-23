import pandas as pd
from pandasql import sqldf
import string
import unittest


class PandaSQLTest(unittest.TestCase):

    def setUp(self):
        return

    def test_select(self):
        df = pd.DataFrame({
                 "letter_pos": [i for i in range(len(string.ascii_letters))],
                 "l2": list(string.ascii_letters)
        })
        result = sqldf("select * from df LIMIT 10;", locals())
        self.assertEqual(len(result), 10)

    def test_join(self):

        df = pd.DataFrame({
            "letter_pos": [i for i in range(len(string.ascii_letters))],
            "l2": list(string.ascii_letters)
        })

        df2 = pd.DataFrame({
            "letter_pos": [i for i in range(len(string.ascii_letters))],
            "letter": list(string.ascii_letters)
        })

        result = sqldf("SELECT a.*, b.letter FROM df a INNER JOIN df2 b ON a.l2 = b.letter LIMIT 20;", locals())
        self.assertEqual(len(result), 20)

    def test_query_with_spacing(self):

        df = pd.DataFrame({
            "letter_pos": [i for i in range(len(string.ascii_letters))],
            "l2": list(string.ascii_letters)
        })

        df2 = pd.DataFrame({
            "letter_pos": [i for i in range(len(string.ascii_letters))],
            "letter": list(string.ascii_letters)
        })
        
        result = sqldf("SELECT a.*, b.letter FROM df a INNER JOIN df2 b ON a.l2 = b.letter LIMIT 20;", locals())
        self.assertEqual(len(result), 20)

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
        self.assertEqual(len(result), 20)

    def test_query_single_list(self):

        mylist = [i for i in range(10)]

        result = sqldf("SELECT * FROM mylist", locals())
        self.assertEqual(len(result), 10)

    def test_query_list_of_lists(self):

        mylist = [[i for i in range(10)], [i for i in range(10)]]

        result = sqldf("SELECT * FROM mylist", locals())
        self.assertEqual(len(result), 2)

    def test_query_list_of_tuples(self):

        mylist = [tuple([i for i in range(10)]), tuple([i for i in range(10)])]

        result = sqldf("SELECT * FROM mylist", locals())
        self.assertEqual(len(result), 2)


if __name__=="__main__":
    unittest.main()

