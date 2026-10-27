import pandas as pd
from .expectations import BaseQA

class ScheduleQA(BaseQA):
    """QA class for Schedule dataset, includes schedule-specific checks."""

    def __init__(self, df: pd.DataFrame, expected_columns: set, valid_teams: set):
        """
        Parameters
        ----------
        df : pd.DataFrame
            Schedule DataFrame
        expected_columns : set
            Columns that should exist in schedule dataset
        valid_teams : set
            Set of valid team names
        """
        super().__init__(df, expected_columns)  # Initialize BaseQA
        self.valid_teams = valid_teams  # Store valid teams

    def team_must_be_in_set(self, col: str):
        """
        Check that all values in a team column are in the valid team set.

        Parameters
        ----------
        col : str
            Column name (home_team or away_team)
        """
        failed_values = self.df.loc[~self.df[col].isin(self.valid_teams), col].tolist()
        return {
            "expectation": f"expect_column_values_to_be_in_set:{col}",
            "success": len(failed_values) == 0,
            "result": {"failed_for": failed_values, "failed_count": len(failed_values)}
        }

    def expect_one_match_per_team_per_round(self):
        """
        Each team must play at most once per round.
        """
        all_matches = pd.concat([
            self.df[["round", "home_team"]].rename(columns={"home_team": "team"}),
            self.df[["round", "away_team"]].rename(columns={"away_team": "team"})
        ])
        duplicates = all_matches.duplicated(subset=["round", "team"], keep=False)
        return {
            "expectation": "one_match_per_team_per_round",
            "success": not duplicates.any(),
            "result": {
                "failed_for": all_matches.loc[duplicates].to_dict(orient="records"),
                "failed_count": int(duplicates.sum() / 2)
            }
        }

    def run_all_checks(self):
        """Run all checks for the schedule dataset."""
        # Base checks
        self.results.append(self.check_columns(store_result=False))
        self.results.append(self.check_nulls(store_result=False))
        self.results.append(self.check_unique("game_id", store_result=False))
        self.results.append(self.check_iso_timestamp("game_start_time", store_result=False))
        self.results.append(self.check_between("round", 1, 38, store_result=False))

        # Schedule-specific checks
        for col in ["home_team", "away_team"]:
            self.results.append(self.team_must_be_in_set(col))
        self.results.append(self.expect_one_match_per_team_per_round())
        return self.results
