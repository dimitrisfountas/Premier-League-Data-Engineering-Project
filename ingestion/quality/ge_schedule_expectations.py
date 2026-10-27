from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
from great_expectations import expectations as gxe

from ingestion.quality.expectations import BaseQA


class ScheduleQA(BaseQA):
    """Great Expectations suite for schedule records and team-round integrity."""

    def __init__(
        self,
        df: pd.DataFrame,
        expected_columns: set[str],
        valid_teams: set[str],
        data_docs_dir: str | Path | None = None,
    ):
        super().__init__(df, expected_columns, data_docs_dir=data_docs_dir)
        self.valid_teams = valid_teams

    def run_all_checks(self) -> list[dict[str, Any]]:
        expectations = self.column_expectations()
        columns = set(self.df.columns)

        if "game_id" in columns:
            expectations.append(
                gxe.ExpectColumnValuesToBeUnique(column="game_id")
            )
        if "round" in columns:
            expectations.append(
                gxe.ExpectColumnValuesToBeBetween(
                    column="round",
                    min_value=1,
                    max_value=38,
                )
            )
        for column in ("home_team", "away_team"):
            if column in columns:
                expectations.append(
                    gxe.ExpectColumnValuesToBeInSet(
                        column=column,
                        value_set=sorted(self.valid_teams),
                    )
                )
        if "game_start_time" in columns:
            expectations.append(
                gxe.ExpectColumnValuesToMatchStrftimeFormat(
                    column="game_start_time",
                    strftime_format="%Y-%m-%dT%H:%M:%SZ",
                )
            )

        results = self.validate(
            suite_name="premier_league_schedule",
            dataframe=self.df,
            expectations=expectations,
        )

        if {"round", "home_team", "away_team"}.issubset(columns):
            # Convert home/away sides into one team-per-round table so a team
            # appearing once on each side still violates compound uniqueness.
            appearances = pd.concat(
                [
                    self.df[["round", "home_team"]].rename(
                        columns={"home_team": "team"}
                    ),
                    self.df[["round", "away_team"]].rename(
                        columns={"away_team": "team"}
                    ),
                ],
                ignore_index=True,
            )
            appearance_results = self.validate(
                suite_name="premier_league_team_round_appearances",
                dataframe=appearances,
                expectations=[
                    gxe.ExpectCompoundColumnsToBeUnique(
                        column_list=["round", "team"]
                    )
                ],
            )
            results.extend(appearance_results)
        return results
