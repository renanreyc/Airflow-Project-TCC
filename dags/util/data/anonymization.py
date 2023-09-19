import pandas as pd
import hashlib


def encode(df, column, encode_fun=hashlib.sha1):
    df[column] = [encode_fun(str(val).encode()).hexdigest()
                  for val in df[column]]
    return df


def encode_columns(df, columns, encode_fun=hashlib.sha1):
    for column in columns:
        df[column] = [encode_fun(str(val).encode()).hexdigest()
                      for val in df[column]]
    return df
