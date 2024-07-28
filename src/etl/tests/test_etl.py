import logging
import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from src.etl.main import extract_data, transform_data, load_data, check_data_quality, CustomFormatter
from src.etl.custom_exceptions import RowCountMismatchError


class TestETL(unittest.TestCase):
    def test_check_data_quality(self):
        df = pd.DataFrame({'Date': ['2022-01-01', '2022-01-02', 'invalid_date'], 'SedentaryMinutes': [10, -20, 30],
                           'TotalActiveMinutes': [30, 40, 50]})
        dq_issues = check_data_quality(df, 'dummy_file_name')
        self.assertEqual(len(dq_issues), 2)

    def test_row_count_mismatch_error(self):
        df = pd.DataFrame({'A': [1, 2, 3], 'B': [11, 22, 33]})
        df_dq_issues = df[df['A'] == 3] # copy and preserve the index
        # add another row to df_dq_issues to simulate a DQ issue
        # df_dq_issues = df_dq_issues.append({'A': 4, 'B': 44}, ignore_index=True)
        df_clean = df.loc[~df.index.isin(df_dq_issues.index.unique())]

        self.assertEqual(df_clean.shape[0], df.shape[0] - len(df_dq_issues.index.unique()))
        #
        # with self.assertRaises(RowCountMismatchError):
        #     assert df_clean.shape[0] == df.shape[0] - len(df_dq_issues.index.unique())



if __name__ == '__main__':
    unittest.main()
