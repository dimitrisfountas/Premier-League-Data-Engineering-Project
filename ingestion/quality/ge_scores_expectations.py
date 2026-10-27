from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
from great_expectations import expectations as gxe

from ingestion.quality.expectations import BaseQA


class ScoresQA(BaseQA):
    """Great Expectations suite for score records and fixture referential integrity."""

    def __init__(
        self,
        df: pd.DataFrame,
        expected_columns: set[str],
        schedule_game_ids: set[str],
        data_docs_dir: str | Path | None = None,
    ):
        super().__init__(df, expected_columns, data_docs_dir=data_docs_dir)
        self.schedule_game_ids = schedule_game_ids

    def run_all_checks(self) -> list[dict[str, Any]]:
        expectations = self.column_expectations()
        columns = set(self.df.columns)

        if "game_id" in columns:
            expectations.append(
                gxe.ExpectColumnValuesToBeUnique(column="game_id")
            )
            if self.schedule_game_ids:
                # This is the score-to-schedule foreign-key check: an orphan
                # score ID fails before any score data reaches analytics.
                expectations.append(
                    gxe.ExpectColumnValuesToBeInSet(
                        column="game_id",
                        value_set=sorted(self.schedule_game_ids),
                    )
                )
        for column in ("home_goals", "away_goals"):
            if column in columns:
                expectations.append(
                    gxe.ExpectColumnValuesToBeBetween(
                        column=column,
                        min_value=0,
                    )
                )
        if "ingestion_time" in columns:
            expectations.append(
                gxe.ExpectColumnValuesToMatchStrftimeFormat(
                    column="ingestion_time",
                    strftime_format="%Y-%m-%dT%H:%M:%SZ",
                )
            )
        if self.schedule_game_ids:
            # With unique, non-null IDs restricted to the schedule set, matching
            # this count proves every scheduled fixture has exactly one score.
            expectations.append(
                gxe.ExpectTableRowCountToEqual(
                    value=len(self.schedule_game_ids)
                )
            )

        return self.validate(
            suite_name="premier_league_scores",
            dataframe=self.df,
            expectations=expectations,
        )
