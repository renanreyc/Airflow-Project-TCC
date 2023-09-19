import pandas as pd


class MergeValidator:
    def __init__(self, analysis_dataframe):
        """
        Initialize Validation Data

        validation = validator.Validator(analysis_dataframe)

        Parameters
        ----------
        analysis_dataframe: df

        Returns
        ----------
        new MergeValidator class

        Atributes
        ----------

        """
        self._analysis_dataframe = analysis_dataframe

    def get_analysis_dataframe(self):
        return self._analysis_dataframe

    def check_if_dataframe_is_empty(self):
        """
        test if the dataframe is empty

        Returns
        ----------
        test empty result : str
        """
        if self._analysis_dataframe.empty:
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
        return None
