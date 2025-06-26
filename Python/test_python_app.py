import unittest
import pandas as pd
from pandas.testing import assert_frame_equal

from myFirstPython import add_days_on_loan, clean_systembook

class TestOperations(unittest.TestCase):

    def test_add_days_on_loan_valid_dates(self):
        # Create a test DataFrame
        raw_df = pd.DataFrame({
            "Book checkout": ["02/01/2002", "10/03/2002"],
            "Book Returned": ["05/01/2020", "15/03/2020"]
        })

        # Clean the data using your function
        df = clean_systembook(raw_df)  # ✅ Pass the test DataFrame

        expected_data = {
            "Book checkout": [pd.Timestamp("2002-01-02"), pd.Timestamp("2002-03-10")],
            "Book Returned": [pd.Timestamp("2020-01-05"), pd.Timestamp("2020-03-15")],
            "DaysOnLoan": [4, 5]
        }

        expected_df = pd.DataFrame(expected_data)
        result_df = add_days_on_loan(df.copy())

        assert_frame_equal(result_df, expected_df)

if __name__ == '__main__':
    unittest.main()
