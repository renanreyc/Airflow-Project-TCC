import pyxlsb
import pandas as pd
import re
import numpy as np


def excel_to_date(d):
    """
    Convert numbers to dates. Numbers equal 0 (i.e. 01/01/1900) are considered as None
    """
    if isinstance(d, str):
        return None
    if d > 0:
        return pd.to_datetime(pyxlsb.convert_date(d))


def handle_df(df, file_type):
    df.columns = _alter_duplicate_columns(df.columns)

    return df


def _alter_duplicate_columns(columns):
    cols = pd.Series(columns)

    for dup in cols[cols.duplicated()].unique():
        cols[cols[cols == dup].index.values.tolist()] = [dup + '_' + str(i)
                                                         if i != 0 else dup for i in range(sum(cols == dup))]
    return cols
