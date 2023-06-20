import unittest
import warnings
from datetime import datetime
from Helper_Functions import querySSAS

# import os
# import contextlib
# with open(os.devnull, 'w') as devnull:  # suppress print output by redirecting to dev/null
#    with contextlib.redirect_stdout(devnull):
#         from Helper_Functions import querySSAS


class TestConnection(unittest.TestCase):
    def setUp(self) -> None:
        warnings.simplefilter('ignore', category=DeprecationWarning)

    def test_spends_cube(self) -> None:
        query = """EVALUATE
                    FILTER (
                        SUMMARIZE (
                                'CPI',
                                'CPI'[Plant Name],
                                "Total Paid", [Paid Amt]
                        ),
                        'CPI'[Plant Name] = "Chandler-Campus CC100"
                    )
                """
        df = querySSAS(query, server='GSM_Spends.intel.com', model='GSM_Spends')
        self.assertEqual(df.shape[0], 1)  # Check if 1 row is returned as a DataFrame"
        self.assertEqual(df['CPI[Plant Name]'][0], 'Chandler-Campus CC100')  # Incorrect campus code
        self.assertIsInstance(df['[Total Paid]'][0], float)

    def test_skynet_cube(self) -> None:
        query = """
                SELECTCOLUMNS(
                    FILTER(
                        SUMMARIZE(
                            'JobProcessingLog',
                            'JobProcessingLog'[Script Type],
                            'JobProcessingLog'[Package Name],
                            "Latest Run", DATEVALUE(MAX('JobProcessingLog'[Last Refresh Date]))
                        ),
                        'JobProcessingLog'[Script Type] = "Python"
                    ),
                    "Script Type", 'JobProcessingLog'[Script Type],
                    "Package Name", 'JobProcessingLog'[Package Name],
                    "Latest Run", [Latest Run]
                )
                ORDER BY [Latest Run] DESC
                """
        df = querySSAS(query, server='GSM_Skynet.intel.com', model='GSM_Skynet')
        df['[Latest Run]'] = df['[Latest Run]'].apply(lambda x: x.to_pydatetime())  # convert Timestamp to Datetime object
        self.assertEqual(df['[Script Type]'][0], 'Python')
        self.assertIsInstance(df['[Latest Run]'][0], datetime)


if __name__ == "__main__":
    unittest.main()
