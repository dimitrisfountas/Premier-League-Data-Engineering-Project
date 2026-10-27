import pandas as pd
from .expectations import BaseQA

class ScoresQA(BaseQA):
    """QA class for Scores dataset, includes scores-specific checks."""

    def __init__(self, df: pd.DataFrame, expected_columns: set):
        """
        Parameters
        ----------
        df : pd.DataFrame
            Scores dataset
        expected_columns : set
            Columns that should exist in scores dataset
        """
        super().__init__(df, expected_columns)  # Initialize BaseQA

    def run_all_checks(self):
        """Run all checks for the scores dataset."""

        # Base checks
        self.results.append(self.check_columns(store_result=False))
        self.results.append(self.check_nulls(store_result=False))
        self.results.append(self.check_unique("game_id", store_result=False))
        

        # Scores-specific checks
        self.results.append(self.check_min_value("home_goals", 0, store_result=False))
        self.results.append(self.check_min_value("away_goals", 0, store_result=False))

        # ISO timestamp for ingestion_time
        self.results.append(self.check_iso_timestamp("ingestion_time", store_result=False))

        return self.results
