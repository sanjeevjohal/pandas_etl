import os
from collections import namedtuple
from pathlib import Path

import pandas as pd
import logging

from sqlalchemy import create_engine

from data import input_data_dir, output_data_dir
from logs import log_dir
from src.etl.custom_exceptions import RowCountMismatchError
from src.etl.utils import check_date_cast


class CustomFormatter(logging.Formatter):
    def format(self, record):
        if record.msg.startswith('IMPORTANT'):
            return f"********** {super().format(record)} **********"
        else:
            return super().format(record)


logging.basicConfig(filename=os.path.join(log_dir, 'etl.log'), level=logging.INFO)
# Add a stream handler to the root logger to output to console
console_handler = logging.StreamHandler()
console_handler.setFormatter(CustomFormatter())
console_handler.setLevel(logging.INFO)
logging.getLogger().addHandler(console_handler)

logger = logging.getLogger()

# Define the named tuple type
ExtractedData = namedtuple('ExtractedData', ['df', 'input_file_name', 'input_file_stem'])


def extract_data(input_file: Path) -> ExtractedData:
    df = pd.read_csv(input_file, encoding_errors='strict', on_bad_lines='warn')
    input_file_name = input_file.name
    input_file_stem = input_file.stem

    return ExtractedData(df, input_file_name, input_file_stem)


def transform_data(df):
    df['Date'] = pd.to_datetime(df['Date'])
    df = df.astype({'SedentaryMinutes': 'int64', 'TotalActiveMinutes': 'int64'})
    return df


def load_data(df, db_path, table_name):
    engine = create_engine(f'sqlite:///{db_path}', echo=False)
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)


def check_data_quality(df, file_name):
    """
    Check the data quality of the dataframe
    :param df:
    :param file_name:
    :return:
    """
    df_dq_issues = df.head(0)
    df_dq_columns = ['_dq_reason', '_severity']
    # add NEW columns and default to None
    for col in df_dq_columns:
        df_dq_issues[col] = None

    # DQ checks is a list of tuples of the type (string, pd.Series, severity)
    checks = [('Invalid date', df['Date'].apply(check_date_cast), 'abort')
        , ('Missing value', df.isnull().any(axis=1), 'warning')
        , ('Negative SedentaryMinutes', df['SedentaryMinutes'] < 0, 'continue')
        , ('Duplicate records', df.duplicated(), 'abort')
              ]

    # iterate over the checks
    for issue, condition, severity in checks:
        if condition.any():
            logger.warning(f"{issue}s found in {file_name}")
            logger.warning(f"\t>>Number of records with {issue}s: {df[condition].count()['Date']}")
            issue_dict = dict(zip(df_dq_columns, [issue, severity]))
            # df_dq_issues = pd.concat([df_dq_issues, df[condition].assign(**{df_dq_column: issue})], ignore_index=True)
            df_dq_issues = pd.concat(
                [
                    df_dq_issues  # the existing dataframe
                    , df[condition].assign(**issue_dict)  # use assign NEW columns to the dataframe
                ]
                # , ignore_index=True # preserve the index
            )

    return df_dq_issues


def main():
    # re-generate the log file
    with open(os.path.join(log_dir, 'etl.log'), 'w'):
        pass

    # hyperparameters
    etl_start_time = pd.to_datetime('now', utc=True)
    IMPORTANT = 'IMPORTANT::'

    ## EXTRACT
    logger.info(f"{IMPORTANT}EXTRACT:: started at {etl_start_time}")

    input_file = Path(input_data_dir, 'activity_data.csv')
    data = extract_data(input_file)
    df = data.df
    file_name = data.input_file_name
    file_stem = data.input_file_stem
    logger.info(f"Extracted {df.shape[0]} rows and {df.shape[1]} columns from {file_name}")

    # EXPLORE THE DATA ??

    # DQ checks
    df_dq_issues = check_data_quality(df, file_name)
    # logger.info(df_dq_issues[['Id', '_dq_reason', '_severity','Date','SedentaryMinutes']].sort_values(by=['Id', '_dq_reason']))
    df_dq_issues_ids = df_dq_issues['Id'].unique()
    logger.info(f"Number of DQ issues found: {df_dq_issues.shape[0]} across {len(df_dq_issues_ids)} Ids")
    df_dq_issues_summary_df: pd.DataFrame = df_dq_issues.groupby(['_dq_reason', '_severity']).size().reset_index(
        name='count')
    # logger.info(df_dq_issues_summary_df)

    # Build clean DF by removing DQ issues using the index
    df_clean = df.loc[~df.index.isin(df_dq_issues.index)]

    # log
    logger.info(f"Cleaned dataframe has {df_clean.shape[0]} rows and {df_clean.shape[1]} columns")

    # use try-except to catch the exception
    # assert df_clean.shape[0] == df.shape[0] - len(df_dq_issues.index.unique())
    try:
        assert df_clean.shape[0] == df.shape[0] - len(df_dq_issues.index.unique())
    except AssertionError:
        logger.error(f"Row count mismatch between clean and original dataframes")
        raise RowCountMismatchError("Row count mismatch between clean and original dataframes")
    finally:
        logger.info(f"EXTRACT:: Row count check passed")

    extract_complete_time = pd.to_datetime('now', utc=True)
    logger.info(f"EXTRACT:: took {extract_complete_time - etl_start_time}")
    logger.info(f"{IMPORTANT}EXTRACT:: completed at {etl_start_time}")


if __name__ == "__main__":
    main()
