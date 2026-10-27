from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from constants import expected_columns, valid_teams
from ingestion.audit.validation_csv_logger import ValidationCSVLogger
from ingestion.csv_ingestor import CSVIngestor
from ingestion.quality.ge_schedule_expectations import ScheduleQA
from ingestion.quality.ge_scores_expectations import ScoresQA


class DataQualityError(RuntimeError):
    """Raised when critical Great Expectations checks remain unsuccessful."""


class DataPipelineCSV:
    """Ingest, validate, remediate known score issues, and audit the result."""

    def __init__(
        self,
        dataset_name: str,
        enable_remediation: bool = False,
        schedule_game_ids: set[str] | None = None,
        log_dir: str | Path = "logs/validation",
    ):
        self.dataset_name = dataset_name
        self.enable_remediation = enable_remediation
        self.schedule_game_ids = schedule_game_ids
        self.df: pd.DataFrame | None = None
        self.results: list[dict[str, Any]] = []
        self.run_ts = pd.Timestamp.now(tz="UTC")
        self.logger = ValidationCSVLogger(dataset_name, log_dir=str(log_dir))

    def ingest_and_normalize(self) -> pd.DataFrame:
        ingestor = CSVIngestor(self.dataset_name)
        ingestor.read_csv()
        ingestor.normalize()
        ingestor.assign_batch_metadata()
        self.df = ingestor.get_dataframe()
        return self.df

    def run_validations(
        self, validation_stage: str = "validation"
    ) -> list[dict[str, Any]]:
        if self.df is None:
            raise ValueError("DataFrame not loaded. Run ingest_and_normalize first.")

        docs_dir = (
            self.logger.log_dir
            / "data_docs"
            / self.dataset_name
            / f"{validation_stage}_{self.run_ts.strftime('%Y%m%dT%H%M%SZ')}"
        )
        if self.dataset_name == "schedule":
            validator = ScheduleQA(
                self.df,
                expected_columns=expected_columns["schedule"],
                valid_teams=valid_teams,
                data_docs_dir=docs_dir,
            )
        elif self.dataset_name == "scores":
            if self.schedule_game_ids is None:
                raise ValueError(
                    "schedule_game_ids are required to validate score references."
                )
            validator = ScoresQA(
                self.df,
                expected_columns=expected_columns["scores"],
                schedule_game_ids=self.schedule_game_ids,
                data_docs_dir=docs_dir,
            )
        else:
            raise ValueError(f"Unknown dataset: {self.dataset_name}")

        self.results = validator.run_all_checks()
        return self.results

    def handle_failed_expectations(self) -> pd.DataFrame:
        """Apply score-specific policies and append removed rows to the audit log."""
        if self.df is None:
            raise ValueError("DataFrame not loaded. Run ingest_and_normalize first.")
        if self.dataset_name != "scores":
            return self.df
        if self.schedule_game_ids is None:
            raise ValueError(
                "schedule_game_ids are required before score remediation."
            )

        score_columns = ["home_goals", "away_goals"]
        required_columns = {"game_id", *score_columns}
        missing_columns = required_columns - set(self.df.columns)
        if missing_columns:
            raise DataQualityError(
                "Cannot remediate scores with missing required columns: "
                + ", ".join(sorted(missing_columns))
            )

        quarantine: dict[Any, str] = {}
        # A duplicate is only safe to collapse if its non-null goal values agree.
        # Conflicting outcomes quarantine the whole fixture group for review.
        for _, group in self.df.groupby("game_id", dropna=False, sort=False):
            if len(group) < 2:
                continue

            complete = group[score_columns].notna().all(axis=1)
            complete_rows = group.loc[complete]
            complete_pairs = {
                tuple(row)
                for row in complete_rows[score_columns].itertuples(
                    index=False, name=None
                )
            }

            if len(complete_pairs) > 1:
                for index in group.index:
                    quarantine[index] = "conflicting_duplicate_game_id"
                continue
            if complete_rows.empty:
                continue

            canonical_score = next(iter(complete_pairs))
            partial_conflict = any(
                pd.notna(row[column]) and row[column] != canonical_score[position]
                for _, row in group[score_columns].iterrows()
                for position, column in enumerate(score_columns)
            )
            if partial_conflict:
                for index in group.index:
                    quarantine[index] = "conflicting_duplicate_game_id"
                continue

            winner_index = complete_rows.index[0]
            for index in group.index:
                if index == winner_index:
                    continue
                # Prefer the complete record only when partial duplicate values
                # agree with it; otherwise the group was quarantined above.
                quarantine[index] = (
                    "duplicate_game_id_same_score"
                    if bool(complete.loc[index])
                    else "incomplete_duplicate_superseded_by_complete_score"
                )

        remaining = self.df.drop(index=list(quarantine))
        # The schedule is authoritative: scores without a fixture are excluded,
        # and missing scores are quarantined rather than silently dropped.
        for index, row in remaining.iterrows():
            game_id = row["game_id"]
            if pd.isna(game_id):
                quarantine[index] = "missing_game_id"
            elif str(game_id) not in self.schedule_game_ids:
                quarantine[index] = "orphan_score_game_id"
            elif row[score_columns].isna().any():
                quarantine[index] = "missing_required_score"

        if quarantine:
            quarantined = self.df.loc[list(quarantine)].copy()
            quarantined["quarantine_reason"] = [
                quarantine[index] for index in quarantined.index
            ]
            self.logger.log_quarantine(quarantined, run_ts=self.run_ts)
            self.df = self.df.drop(index=list(quarantine)).copy()
        return self.df

    def run_pipeline(
        self,
    ) -> tuple[pd.DataFrame, list[dict[str, Any]], pd.DataFrame]:
        self.ingest_and_normalize()
        initial_stage = "pre-remediation" if self.enable_remediation else "validation"
        self.run_validations(validation_stage=initial_stage)
        results_before = self.results.copy()
        summary_df = self.logger.log(
            results=results_before,
            run_ts=self.run_ts,
            validation_stage=initial_stage,
        )

        has_failures = any(not result["success"] for result in results_before)
        if self.enable_remediation and has_failures:
            self.handle_failed_expectations()
            self.run_validations(validation_stage="post-remediation")
            results_after = self.results.copy()
            summary_df = self.logger.log(
                results=results_after,
                run_ts=self.run_ts,
                validation_stage="post-remediation",
            )
            failed_before = sum(not result["success"] for result in results_before)
            failed_after = sum(not result["success"] for result in results_after)
            print(f"Failed expectations before remediation: {failed_before}")
            print(f"Failed expectations after remediation: {failed_after}")
            results_to_enforce = results_after
        else:
            results_to_enforce = results_before

        # Do not let downstream analytics consume a dataset with unresolved
        # critical GX failures, even if a remediation pass was requested.
        critical_failures = [
            result["expectation"]
            for result in results_to_enforce
            if not result["success"]
            and result.get("severity", "critical") == "critical"
        ]
        if critical_failures:
            raise DataQualityError(
                f"Critical {self.dataset_name} validation failures remain: "
                + ", ".join(critical_failures)
            )

        if self.df is None:
            raise RuntimeError("The pipeline completed without an ingested DataFrame.")
        return self.df, self.results, summary_df
