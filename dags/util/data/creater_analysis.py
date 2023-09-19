import pandas as pd
import re


def create_schema_analysis(df):
    percent_filling = 100 - (
        df.isnull().sum() * 100 / len(df)
    )

    column_type = []
    column_size = []
    column_percent_unique = []
    column_type_dataframe = []
    for column in df.columns:
        column_type.append(
            _identificationtypedata(
                df[column],
                df[column].dtypes,
                0,
            )
        )
        column_type_dataframe.append(
            _identificationtypedata(
                df[column],
                df[column].dtypes,
                1,
            )
        )
        column_size.append(
            _identify_data_size(
                df[column], df[column].dtypes
            )
        )

        column_percent_unique.append(
            _calculate_percent_unique(df[column])
        )

    data = {
        "column_name": df.columns,
        "column_name_formatted": _formate_column_name(
            df.columns
        ),
        "percent_filling": round(percent_filling, 2),
        "column_type_dataframe": column_type_dataframe,
        "column_type": column_type,
        "column_size": column_size,
        "column_percent_unique": column_percent_unique,
    }

    pd_data_analyze = pd.DataFrame(
        data,
        columns=[
            "column_name",
            "column_name_formatted",
            "percent_filling",
            "column_type_dataframe",
            "column_type",
            "column_size",
            "column_percent_unique",
        ],
    )

    pd_data_analyze["column_size"] = (
        pd_data_analyze["column_size"].fillna(
            -1).astype(int).replace(-1, None)
    )

    return pd_data_analyze.reset_index(drop=True)


def _identificationtypedata(column, typecolumn, type=0):
    typecolumn = str(typecolumn)
    if (column.empty):
        return None
    if typecolumn == "datetime64[ns]":
        return ["TIMESTAMP", "datetime64[ns]"][type]
    elif typecolumn == "int64":
        return ["INTEGER", "int64"][type]
    elif typecolumn == "float64":
        return ["DECIMAL", "float64"][type]
    elif typecolumn == "object":
        return ["VARCHAR", "object"][type]
    else:
        return "type data not identified."


def _identify_data_size(column, typecolumn):
    typecolumn = str(typecolumn)
    if typecolumn == "object":
        try:
            size_column = int(column.str.len().max())
            return size_column
        except:
            return -1  # where size not identificated error
    else:
        return None


def _calculate_percent_unique(column):
    return round((len(column.drop_duplicates()) / len(column)) * 100, 2)


def _formate_column_name(columns):
    map_symbos = {
        "Á": "A",
        "À": "A",
        "Ã": "A",
        "Â": "A",
        "É": "E",
        "Ê": "E",
        "Í": "I",
        "Ó": "O",
        "Õ": "O",
        "Ô": "O",
        "Ú": "U",
        "Ç": "C",
        "º": "",
        ".": "",
        " ": "_",
        "(": "",
        ")": "",
        ":": "",
        "?": ""
    }

    regex = re.compile("[%s]" % "".join(map_symbos.keys()))

    columns_regex = (
        pd.Series(columns)
        .astype(str)
        .str.upper()
        .str.strip()
        .map(lambda x: regex.sub(lambda match: map_symbos[match.group(0)], x))
    )

    columns_regex = columns_regex.replace('\n', '', regex=True)
    columns_regex = columns_regex.replace('__', '_', regex=True)
    columns_regex = columns_regex.replace('_-_', '_', regex=True)
    return columns_regex.to_list()
