import time

import pandas as pd

from src.etl.constants import DEBUG


def _timeit(func):
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        duration = round(end - start, 2)
        print(f'{duration} seconds')
        return result

    return wrapper


def debug_print(*args):
    if DEBUG:
        print(*args)



# DQ helper functions
def check_date_cast(column: pd.Series):
    """
    Check if a column can be cast to a date
    :param column:
    :return:
    """
    try:
        pd.to_datetime(column)
        return False
    except ValueError:
        return True
