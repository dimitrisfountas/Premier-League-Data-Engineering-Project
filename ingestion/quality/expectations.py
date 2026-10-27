import pandas as pd  # pandas for DataFrame operations

# ---------------------------
# Base QA class
# ---------------------------
class BaseQA:
    """
    Base class for QA expectations on a DataFrame.
    Stores the DataFrame internally and provides common expectation methods.
    """

    def __init__(self, df: pd.DataFrame, expected_columns: set):
        """
        Initialize the QA instance with a DataFrame.

        Parameters
        ----------
        df : pd.DataFrame
            The dataset on which expectations will be run.
        expected_columns : set
            The set of expected column names.
        """
        self.df = df  # Store the DataFrame as an instance attribute
        self.expected_columns = expected_columns  # Expected schema columns
        self.results = []  # Store results of expectations

    # -----------------------
    # Expectation helpers
    # -----------------------

    def check_columns(self, store_result: bool = True) -> dict:
        """
        Check that the DataFrame columns match the expected set exactly.

        Parameters
        ----------
        store_result : bool
            Whether to store the result in self.results.

        Returns
        -------
        dict
            Result dictionary for the expectation.
        """
        actual_columns = set(self.df.columns)
        missing = self.expected_columns - actual_columns
        extra = actual_columns - self.expected_columns

        result = {
            "expectation": "expect_table_columns_to_match_set",
            "success": actual_columns == self.expected_columns,
            "result": {
                "missing": list(missing),
                "missing_count": len(missing),
                "extra": list(extra),
                "extra_count": len(extra)
            },
            "checked_at": pd.Timestamp.utcnow()
        }

        if store_result:
            self.results.append(result)
        return result

    def check_nulls(self, store_result: bool = True) -> dict:
        """
        Check for null values in any column of the DataFrame.

        Returns
        -------
        dict
            Result dictionary indicating columns with nulls.
        """
        failed_cols = self.df.columns[self.df.isnull().any()].tolist()

        result = {
            "expectation": "expect_table_columns_to_not_be_null",
            "success": len(failed_cols) == 0,
            "result": {
                "failed_for": failed_cols,
                "failed_count": len(failed_cols)
            },
            "checked_at": pd.Timestamp.utcnow()
        }

        if store_result:
            self.results.append(result)
        return result

    def check_unique(self, col: str, store_result: bool = True) -> dict:
        """
        Check that a specific column contains unique values.

        Parameters
        ----------
        col : str
            Column to check uniqueness for.

        Returns
        -------
        dict
            Result dictionary indicating duplicates if any.
        """
        failed_values = self.df[col][self.df[col].duplicated(keep=False)].tolist()
        result = {
            "expectation": f"expect_column_values_to_be_unique:{col}",
            "success": len(failed_values) == 0,
            "result": {
                "failed_for": failed_values,
                "failed_count": len(failed_values)
            },
            "checked_at": pd.Timestamp.utcnow()
        }

        if store_result:
            self.results.append(result)
        return result

    def check_iso_timestamp(self, col: str, store_result: bool = True) -> dict:
        """
        Check that a column has ISO-8601 timestamps (YYYY-MM-DDTHH:MM:SSZ).

        Parameters
        ----------
        col : str
            Column name to check.

        Returns
        -------
        dict
            Result dictionary indicating success or failure.
        """
        try:
            pd.to_datetime(self.df[col], format="%Y-%m-%dT%H:%M:%SZ", errors="raise")
            success = True
        except Exception:
            success = False

        result = {
            "expectation": f"expect_column_values_to_match_strftime_format:{col}",
            "success": success,
            "result": {},
            "checked_at": pd.Timestamp.utcnow()
        }

        if store_result:
            self.results.append(result)
        return result

    def check_between(self, col: str, min_value, max_value, store_result: bool = True) -> dict:
        """
        Check that values in a column fall between min_value and max_value (inclusive).

        Parameters
        ----------
        col : str
            Column name to check.
        min_value
            Minimum allowed value.
        max_value
            Maximum allowed value.

        Returns
        -------
        dict
            Result dictionary with failed values if any.
        """
        if not pd.api.types.is_numeric_dtype(self.df[col]):
            raise TypeError(f"Column '{col}' must be numeric to check between values.")

        failed_values = self.df.loc[~self.df[col].between(min_value, max_value), col].tolist()

        result = {
            "expectation": f"expect_column_values_to_be_between:{col}",
            "success": len(failed_values) == 0,
            "result": {"failed_for": failed_values},
            "checked_at": pd.Timestamp.utcnow()
        }

        if store_result:
            self.results.append(result)
        return result

    def check_min_value(self, col: str, min_value, store_result: bool = True) -> dict:
        """
        Check that all values in a column are >= min_value.

        Parameters
        ----------
        col : str
            Column name to check.
        min_value
            Minimum allowed value.

        Returns
        -------
        dict
            Result dictionary with failed values if any.
        """
        if not pd.api.types.is_numeric_dtype(self.df[col]):
            raise TypeError(f"Column '{col}' must be numeric to check min_value.")

        failed_values = self.df.loc[self.df[col] < min_value, col].tolist()

        result = {
            "expectation": f"expect_column_values_to_be_min:{col}",
            "success": len(failed_values) == 0,
            "result": {"failed_for": failed_values},
            "checked_at": pd.Timestamp.utcnow()
        }

        if store_result:
            self.results.append(result)
        return result

    # -----------------------
    # Helper methods
    # -----------------------

    def run_all_checks(self):
        """
        Example method to run all QA checks and store results.
        Call this after defining expected columns.
        """
        self.check_columns()
        self.check_nulls()
        # Add more checks as needed

    def get_results(self) -> list:
        """
        Return all stored QA results.

        Returns
        -------
        list
            List of all expectation result dictionaries.
        """
        return self.results
