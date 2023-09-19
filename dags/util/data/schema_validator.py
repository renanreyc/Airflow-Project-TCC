import pandas as pd
import re


class SchemaValidator:
    def __init__(self, analysis_dataframe, input_data_validator):
        """
        Initialize Validation Data

        validation = validator.Validator(dataframe, dataframe_validator)

        Parameters
        ----------
        dataframe: df
        dataframe_validator: df

        Returns
        ----------
        new Validator class

        Atributes
        ----------

        """
        self._validator_df = input_data_validator
        self._analysis_df = analysis_dataframe
        self._analysis_filted_df = self._analysis_df[self._analysis_df['column_name'].isin(self._validator_df['column_name'])]

    def get_analysis_df(self):
        return self._analysis_df

    def get_validator_df(self):
        return self._validator_df

    def get_analysis_filted_df(self):
        return self._analysis_filted_df

    def check_if_dataframe_is_empty(self):
        """
        test if the dataframe is empty

        Returns
        ----------
        test empty result : str
        """
        if self._analysis_df.empty:
            return "O arquivo está em branco."
        else:
            return None

    def validate_number_fields(self):
        """
        To check the expected number of fields in the input data frame

        Returns
        ----------
        test column validation result : str
        """
        if self._analysis_filted_df.shape[0] < self._validator_df.shape[0]:
            return "Número de campos é menor do que o mínimo esperado."
        else:
            return None

    def validate_matched_columns(self, namecolumn):
        """
        Validate the columns of the input dataframe
        with the validator dataframe.

        Parameters
        ----------
        namecolumn : str
            Name of the column to validate.

        Returns
        ----------
        List of columns without match: list[str]
        """
        validator_column = self._validator_df[namecolumn]
        analysis_column = self._analysis_filted_df[namecolumn]

        unmatched_columns = analysis_column[~analysis_column.isin(
            validator_column)]

        if not unmatched_columns.empty:
            return f"Há colunas diferentes do esperado: {str(unmatched_columns.tolist())}"

        return None

    def validate_data_types(self, column_index, column_match):
        """
        validate the types of the input dataframe
        with the validator dataframe

        Returns
        ----------
        test type validation result : str
        """
        comparedatatyperesult = self._analysis_filted_df[
            [column_index, column_match]
        ].equals(self._validator_df[[column_index, column_match]])

        if comparedatatyperesult:
            return None
        else:
            return "Os tipos das colunas do arquivo estão diferentes do esperado."

    def check_for_missing_values(self):
        """
            Validate if there are missing values in the input dataframe

            Returns
            -------
            missing_columns : list
                report error with list of column names with missing values in 'percent_filling' field
        """
        column_name = 'column_name'
        missing_columns = self._analysis_df.loc[self._analysis_df['percent_filling'] == 0, column_name].tolist()

        if missing_columns:
            return "O arquivo tem colunas importantes vazias: " + str(missing_columns)
        else:
            return None

    def check_unique_primary_key(self):
        """
        To check in only primary key fields if they were unique.

        Returns
        ----------
        test missing values result : str
        """
        primary_key_validator = self._validator_df.loc[
            self._validator_df["is_primary_key"] == True
        ]
        if primary_key_validator.empty:
            return None

        boolean_analyse_filted_validator = self._analysis_filted_df["column_name"].isin(
            primary_key_validator["column_name"]
        )

        df_analyse_filted_validator = self._analysis_df.loc[
            boolean_analyse_filted_validator
        ]

        duplicate_values = df_analyse_filted_validator[df_analyse_filted_validator.duplicated(subset='column_name')]['column_name'].tolist()


        if duplicate_values:
            return "A coluna de identificação única contém duplicidade: " + str(duplicate_values)
        else:
            return None
