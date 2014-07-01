import pandas as pd
from pandasql import sqldf, load_meat
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
    
    def test_subquery(self):
        kermit = pd.DataFrame({"x": range(10)})
        q = "select * from (select * from kermit) tbl limit 2;"
        result = sqldf(q, locals())
        self.assertEqual(len(result), 2)
    
    def test_in(self):
        courseData = {
            'courseCode': ['TM351','TU100','M269'],
            'points':[30,60,30],
            'level':['3','1','2']
        }
        course_df = pd.DataFrame(courseData)
        q = "SELECT * FROM course_df WHERE courseCode IN ( 'TM351', 'TU100' );"
        result = sqldf(q, locals())
        self.assertEqual(len(result), 2)

    def test_in_with_subquery(self):
        programData = {
            'courseCode': ['TM351','TM351','TM351','TU100','TU100','TU100','M269','M269','M269'],
            'programCode':['AB1','AB2','AB3','AB1','AB3','AB4','AB3','AB4','AB5']
             }
        program_df = pd.DataFrame(programData)
        
        courseData = {
            'courseCode': ['TM351','TU100','M269'],
            'points':[30,60,30],
            'level':['3','1','2']
        }
        course_df = pd.DataFrame(courseData)

        q = '''
            SELECT * FROM course_df WHERE courseCode IN ( SELECT DISTINCT courseCode FROM program_df ) ;
          '''
        result = sqldf(q, locals())
        self.assertEqual(len(result), 3)

    def test_datetime_query(self):
        pysqldf = lambda q: sqldf(q, globals())
        meat = load_meat()
        result = sqldf("SELECT * FROM meat LIMIT 10;", locals())
        self.assertEqual(len(result), 10)

if __name__=="__main__":
    unittest.main()

