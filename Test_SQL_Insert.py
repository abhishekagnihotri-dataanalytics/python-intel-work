__author__ = "Matt Davis"
__email__ = "matthew1.davis@intel.com"
__description__ = """This script is used to test functionality of the Helper Function uploadDFtoSQL()"""
__schedule__ = "N/A"

import os
import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
from datetime import datetime
import unittest
import pandas as pd
import contextlib
from Helper_Functions import uploadDFtoSQL, querySQL
from Project_params import params


# remove the current file's parent directory from sys.path
try:
    sys.path.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
except ValueError:  # Already removed
    pass


class TestSQLInsertErrors(unittest.TestCase):
    def __iter__(self):
        self.table = 'audit.PythonUnitTest'

    def test_successful_sql_insert_by_position(self) -> None:
        example = [{'RequestID': 1,
                    'EventName': 'Event 1',
                    'TimeStamp': datetime.today().timestamp(),
                    'LoadDtm': datetime.now().replace(microsecond=0),  # SQL Server only stores 3 digit microsecond in the [datetime] object
                    'LoadBy': 'AMR\\' + os.getlogin().upper()
                    },
                   {'RequestID': 2,
                    'EventName': 'Event 2',
                    'TimeStamp': datetime.today().timestamp(),
                    'LoadDtm': datetime.now().replace(microsecond=0),
                    'LoadBy': 'AMR\\' + os.getlogin().upper()
                    }
                   ]

        df_input = pd.DataFrame(example)

        # Write data to SQL Server table
        with open(os.devnull, 'w') as devnull:  # suppress print output by redirecting to dev/null
            with contextlib.redirect_stdout(devnull):
                insert_succeeded, error_msg = uploadDFtoSQL(self.table, data=df_input)
        self.assertTrue(insert_succeeded)
        self.assertIsNone(error_msg)

        # Check if data in SQL Server matches input
        query_succeeded, df_result, error_msg = querySQL("SELECT * FROM {0}".format(self.table))
        self.assertTrue(query_succeeded)
        self.assertIsNone(error_msg)
        self.assertEqual(df_result.shape[0], 2)  # Check that there are 2 rows in the table
        self.assertTrue(df_input.compare(df_result).empty)  # Check that all the data in the table matches the input

    def test_successful_sql_insert_by_name(self) -> None:
        # Mix up the insert order
        example = [{'TimeStamp': datetime.today().timestamp(),
                    'LoadBy': 'AMR\\' + os.getlogin().upper(),
                    'EventName': 'Event 3',
                    'RequestID': 3,
                    'LoadDtm': datetime.now().replace(microsecond=0),  # SQL Server only stores 3 digit microsecond in the [datetime] object

                    },
                   {
                    'TimeStamp': datetime.today().timestamp(),
                    'LoadBy': 'AMR\\' + os.getlogin().upper(),
                    'EventName': 'Event 4',
                    'RequestID': 4,
                    'LoadDtm': datetime.now().replace(microsecond=0)
                    }
                   ]

        df_input = pd.DataFrame(example)

        # Write data to SQL Server table
        with open(os.devnull, 'w') as devnull:  # suppress print output by redirecting to dev/null
            with contextlib.redirect_stdout(devnull):
                insert_succeeded, error_msg = uploadDFtoSQL(self.table, data=df_input, columns=list(df_input.columns))
        self.assertTrue(insert_succeeded)
        self.assertIsNone(error_msg)

        # Correct the order of the data that was inserted to match the table column order
        df_input = df_input[['RequestID', 'EventName', 'TimeStamp', 'LoadDtm', 'LoadBy']]

        # Check if data in SQL Server matches input
        query_succeeded, df_result, error_msg = querySQL("SELECT * FROM {0}".format(self.table))
        self.assertTrue(query_succeeded)
        self.assertIsNone(error_msg)
        self.assertEqual(df_result.shape[0], 2)  # Check that there are 2 rows in the table
        self.assertTrue(df_input.compare(df_result).empty)  # Check that all the data in the table matches the input

    def test_successful_sql_insert_by_name_ignore_case(self) -> None:
        # Mix up the insert order
        example = [{'TimeStamp': datetime.today().timestamp(),
                    'LoadBy': 'AMR\\' + os.getlogin().upper(),
                    'EventName': 'Event 3',
                    'RequestID': 5,
                    'LoadDtm': datetime.now().replace(microsecond=0)  # SQL Server only stores 3 digit microsecond in the [datetime] object
                    },
                   {
                    'TimeStamp': datetime.today().timestamp(),
                    'LoadBy': 'AMR\\' + os.getlogin().upper(),
                    'EventName': 'Event 4',
                    'RequestID': 6,
                    'LoadDtm': datetime.now().replace(microsecond=0)
                    }
                   ]

        df_input = pd.DataFrame(example)

        # Write data to SQL Server table
        with open(os.devnull, 'w') as devnull:  # suppress print output by redirecting to dev/null
           with contextlib.redirect_stdout(devnull):
                insert_succeeded, error_msg = uploadDFtoSQL(self.table, data=df_input, columns=['Timestamp', 'Loadby', 'Eventname', 'Requestid', 'Loaddtm'])
        self.assertTrue(insert_succeeded)
        self.assertIsNone(error_msg)

        # Correct the order of the data that was inserted to match the table column order
        df_input = df_input[['RequestID', 'EventName', 'TimeStamp', 'LoadDtm', 'LoadBy']]

        # Check if data in SQL Server matches input
        query_succeeded, df_result, error_msg = querySQL("SELECT * FROM {0}".format(self.table))
        self.assertTrue(query_succeeded)
        self.assertIsNone(error_msg)
        self.assertEqual(df_result.shape[0], 2)  # Check that there are 2 rows in the table
        self.assertTrue(df_input.compare(df_result).empty)  # Check that all the data in the table matches the input

    def test_truncation_error_handling(self) -> None:
        example = [{'RequestID': 1,
                    'EventName': 'This is my super long event name that is "The coolest party I ever attended happened at 25 W. Gilmore Rd in New Mexico, United States of America at 8:00PM MST." What do you think about that name? Is it long enough to require additional column size in the database? Who knows...',
                    },
                   {'RequestID': 2,
                    'EventName': 'Event 2',
                    }
                   ]

        with open(os.devnull, 'w') as devnull:  # suppress print output by redirecting to dev/null
            with contextlib.redirect_stdout(devnull):
                insert_succeeded, error_msg = uploadDFtoSQL(self.table, data=pd.DataFrame(example), columns=['RequestID', 'EventName'])
        self.assertFalse(insert_succeeded)
        self.assertEqual(error_msg, "Error: Unable to insert data into table \"{0}\" due to possible data truncation. "
                                    "Attempted to insert text value of length 277 into column \"EventName\" which is the type nvarchar(255).\n"
                                    "Original error: ('String data, right truncation: length 554 buffer 510', 'HY000')".format(self.table))

    def test_missing_sql_column_error_handling(self) -> None:
        example = [{'RequestID': 1,
                    'EventName': 'Event 1',
                    'TimeStamp': datetime.today().timestamp(),
                    'ExtraColumn': 'Why is this here? My SQL table only has 5 columns...',
                    'LoadDtm': datetime.now(),
                    'LoadBy': 'AMR\\' + os.getlogin().upper()
                    },
                   {'RequestID': 2,
                    'EventName': 'Event 2',
                    'TimeStamp': datetime.today().timestamp(),
                    'ExtraColumn': 'Most likely someone added this column to the data source (i.e. Excel) without notifying us.',
                    'LoadDtm': datetime.now(),
                    'LoadBy': 'AMR\\' + os.getlogin().upper()
                    }
                   ]

        with open(os.devnull, 'w') as devnull:  # suppress print output by redirecting to dev/null
            with contextlib.redirect_stdout(devnull):
                insert_succeeded, error_msg = uploadDFtoSQL(self.table, data=pd.DataFrame(example))
        self.assertFalse(insert_succeeded)
        self.assertEqual(error_msg, "[SQL Server]Unable to insert values into to table {0} in database {1}. "
                                    "There are more columns in the Pandas DataFrame than are available in the SQL table. "
                                    "Perhaps a column was added to your data source?".format(self.table, params['GSMDW_DB']))

    def test_extra_sql_column_error_handling(self) -> None:
        example = [{'RequestID': 1,
                    'EventName': 'Event 1',
                    # 'TimeStamp': datetime.today().timestamp(),  # Note this column is intentionally excluded from the dataset
                    'LoadDtm': datetime.now(),
                    'LoadBy': 'AMR\\' + os.getlogin().upper()
                    },
                   {'RequestID': 2,
                    'EventName': 'Event 2',
                    # 'TimeStamp': datetime.today().timestamp(),  # Without timestamp there will only be 4 columns provided even though there are 5 columns in the SQL table
                    'LoadDtm': datetime.now(),
                    'LoadBy': 'AMR\\' + os.getlogin().upper()
                    }
                   ]

        with open(os.devnull, 'w') as devnull:  # suppress print output by redirecting to dev/null
            with contextlib.redirect_stdout(devnull):
                insert_succeeded, error_msg = uploadDFtoSQL(self.table, data=pd.DataFrame(example))
        self.assertFalse(insert_succeeded)
        self.assertEqual(error_msg, "[SQL Server]Unable to insert values into to table {0} in database {1}. "
                                    "There are more columns in the SQL table than are available in the Pandas DataFrame. "
                                    "Perhaps a column was deleted from your data source?".format(self.table, params['GSMDW_DB']))

    def test_invalid_table_name_error_handling(self) -> None:
        example = [{'RequestID': 1, 'EventName': 'Event 1'}, {'RequestID': 2, 'EventName': 'Event 2'}]
        fake_table = 'audit.YpthonUnitTest'

        # Write data to SQL Server table
        with open(os.devnull, 'w') as devnull:  # suppress print output by redirecting to dev/null
            with contextlib.redirect_stdout(devnull):
                insert_succeeded, error_msg = uploadDFtoSQL(fake_table, data=pd.DataFrame(example), columns=['RequestID', 'EventName'], truncate=True)
        self.assertFalse(insert_succeeded)
        self.assertEqual(error_msg, "Table {0} not found in database {1}".format(fake_table, params['GSMDW_DB']))

    def test_invalid_column_name_error_handling(self) -> None:
        example = [{'RequestID': 1, 'EventName': 'Event 1'}, {'RequestID': 2, 'EventName': 'Event 2'}]

        # Write data to SQL Server table
        with open(os.devnull, 'w') as devnull:  # suppress print output by redirecting to dev/null
            with contextlib.redirect_stdout(devnull):
                insert_succeeded, error_msg = uploadDFtoSQL(self.table, data=pd.DataFrame(example), columns=['ID', 'EventName'])
        self.assertFalse(insert_succeeded)
        self.assertEqual(error_msg, "[SQL Server]Unable to insert values into to table {0} in database {1}. "
                                    "Unable to find one or more columns provided by the user in the SQL table. "
                                    "Perhaps a column name changed in the SQL database?".format(self.table, params['GSMDW_DB']))

    def test_wrong_number_of_sql_columns_error_handling(self) -> None:
        example = [{'RequestID': 1, 'EventName': 'Event 1'}, {'RequestID': 2, 'EventName': 'Event 2'}]

        # Write data to SQL Server table
        with open(os.devnull, 'w') as devnull:  # suppress print output by redirecting to dev/null
            with contextlib.redirect_stdout(devnull):
                insert_succeeded, error_msg = uploadDFtoSQL(self.table, data=pd.DataFrame(example), columns=['RequestID', 'EventName', 'ExtraColumn'])
        self.assertFalse(insert_succeeded)
        self.assertEqual(error_msg, "[SQL Server]Unable to insert values into to table {0} in database {1}. "
                                    "Number of columns provided by the user does not match the number of columns in the Pandas DataFrame. "
                                    "Perhaps a column was deleted from your data source?".format(self.table, params['GSMDW_DB']))

    def test_duplicate_column_name_error_handling(self) -> None:
        example = [{'RequestID': 1, 'EventName': 'Event 1', 'EventName2': 'Why is this column name duplicated?'},
                   {'RequestID': 2, 'EventName': 'Event 2', 'EventName2': 'Who knows...'}]

        df = pd.DataFrame(example)
        df.rename(columns={'EventName2': 'EventName'}, inplace=True)  # Change column name to exactly match the first column

        # Write data to SQL Server table
        with open(os.devnull, 'w') as devnull:  # suppress print output by redirecting to dev/null
            with contextlib.redirect_stdout(devnull):
                insert_succeeded, error_msg = uploadDFtoSQL(self.table, data=df, columns=list(df.columns))
        self.assertFalse(insert_succeeded)
        self.assertEqual(error_msg, "Duplicate column name in the Pandas DataFrame. "
                                    "This function will not attempt to load data now because of column mapping issue.")


if __name__ == "__main__":
    if len(sys.argv) > 1:  # remove command line arguments when testing different SQL Server environments
        sys.argv.pop()
    unittest.main()
