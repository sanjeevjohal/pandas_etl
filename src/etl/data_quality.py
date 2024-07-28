import pandas as pd


def check_date_cast(column: pd.Series):
    try:
        pd.to_datetime(column)
        return False
    except ValueError:
        return True
