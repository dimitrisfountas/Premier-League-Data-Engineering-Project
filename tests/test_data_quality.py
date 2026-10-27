from __future__ import annotations

import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from constants import expected_columns
from ingestion.pipeline.pipeline import DataPipelineCSV, DataQualityError
from ingestion.quality.ge_scores_expectations import ScoresQA


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def make_scores(rows: list[tuple[str, float | None, float | None]]) -> pd.DataFrame:
    """Build the enriched score schema expected by the production validator."""
    df = pd.DataFrame(
        rows,
        columns=["game_id", "home_goals", "away_goals"],
    )
    df["ingestion_time"] = "2025-08-22T17:47:56Z"
    df["updated_at"] = pd.Timestamp("2025-08-22T17:47:56Z")
    df["source"] = "scores"
    df["ingestion_path"] = "tests/scores.csv"
    df["last_updated_at"] = pd.Timestamp("2025-08-22T17:47:56Z")
    return df


class GreatExpectationsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)

    def make_pipeline(
        self,
        scores: pd.DataFrame,
        schedule_game_ids: set[str],
    ) -> DataPipelineCSV:
        pipeline = DataPipelineCSV(
            "scores",
            enable_remediation=True,
            schedule_game_ids=schedule_game_ids,
            log_dir=self.temp_dir.name,
        )
        pipeline.df = scores.copy()
        return pipeline

    def run_fixture_pipeline(self, pipeline: DataPipelineCSV):
        # Keep tests focused on validation/remediation while avoiding disk input.
        with patch.object(pipeline, "ingest_and_normalize"):
            return pipeline.run_pipeline()

    def test_missing_columns_fail_the_gx_schema_expectation(self) -> None:
        frame = make_scores([("fixture-1", 1, 0)]).drop(columns="away_goals")
        results = ScoresQA(
            frame,
            expected_columns=expected_columns["scores"],
            schedule_game_ids={"fixture-1"},
            data_docs_dir=self.temp_dir.name,
        ).run_all_checks()

        schema_result = next(
            result
            for result in results
            if result["expectation"]
            == "expect_table_columns_to_match_ordered_list"
        )
        self.assertFalse(schema_result["success"])
        self.assertEqual(schema_result["severity"], "critical")

    def test_column_order_is_primary_key_then_related_fields(self) -> None:
        frame = make_scores([("fixture-1", 1, 0)])
        results = ScoresQA(
            frame,
            expected_columns=expected_columns["scores"],
            schedule_game_ids={"fixture-1"},
        ).run_all_checks()

        schema_result = next(
            result
            for result in results
            if result["expectation"]
            == "expect_table_columns_to_match_ordered_list"
        )
        self.assertTrue(schema_result["success"])

    def test_null_goals_fail_gx_and_are_audited_when_quarantined(self) -> None:
        frame = make_scores([("fixture-1", None, 0)])
        pipeline = self.make_pipeline(frame, {"fixture-1"})
        initial = ScoresQA(
            frame,
            expected_columns=expected_columns["scores"],
            schedule_game_ids={"fixture-1"},
        ).run_all_checks()
        null_result = next(
            result
            for result in initial
            if result["expectation"] == "expect_column_values_to_not_be_null:home_goals"
        )
        self.assertFalse(null_result["success"])

        with self.assertRaises(DataQualityError):
            self.run_fixture_pipeline(pipeline)
        quarantine = pd.read_csv(
            Path(self.temp_dir.name) / "scores_quarantine.csv"
        )
        self.assertEqual(quarantine["quarantine_reason"].tolist(), [
            "missing_required_score"
        ])

    def test_identical_duplicate_scores_collapse_with_audit_reason(self) -> None:
        frame = make_scores(
            [
                ("fixture-1", 2, 1),
                ("fixture-1", 2, 1),
            ]
        )
        pipeline = self.make_pipeline(frame, {"fixture-1"})

        clean, results, _ = self.run_fixture_pipeline(pipeline)

        self.assertEqual(len(clean), 1)
        self.assertTrue(all(result["success"] for result in results))
        quarantine = pd.read_csv(
            Path(self.temp_dir.name) / "scores_quarantine.csv"
        )
        self.assertEqual(
            quarantine["quarantine_reason"].tolist(),
            ["duplicate_game_id_same_score"],
        )

    def test_compatible_incomplete_duplicate_uses_complete_score(self) -> None:
        frame = make_scores(
            [
                ("fixture-1", 2, 1),
                ("fixture-1", None, 1),
            ]
        )
        pipeline = self.make_pipeline(frame, {"fixture-1"})

        clean, results, _ = self.run_fixture_pipeline(pipeline)

        self.assertEqual(
            clean[["home_goals", "away_goals"]].iloc[0].tolist(),
            [2.0, 1.0],
        )
        self.assertTrue(all(result["success"] for result in results))
        quarantine = pd.read_csv(
            Path(self.temp_dir.name) / "scores_quarantine.csv"
        )
        self.assertEqual(
            quarantine["quarantine_reason"].tolist(),
            ["incomplete_duplicate_superseded_by_complete_score"],
        )

    def test_conflicting_duplicate_scores_are_quarantined_and_block_pipeline(
        self,
    ) -> None:
        frame = make_scores(
            [
                ("fixture-1", 2, 1),
                ("fixture-1", 0, 3),
            ]
        )
        pipeline = self.make_pipeline(frame, {"fixture-1"})

        with self.assertRaisesRegex(DataQualityError, "Critical"):
            self.run_fixture_pipeline(pipeline)
        quarantine = pd.read_csv(
            Path(self.temp_dir.name) / "scores_quarantine.csv"
        )
        self.assertEqual(len(quarantine), 2)
        self.assertEqual(
            set(quarantine["quarantine_reason"]),
            {"conflicting_duplicate_game_id"},
        )

    def test_orphan_score_is_quarantined_against_schedule_ids(self) -> None:
        frame = make_scores(
            [
                ("fixture-1", 1, 0),
                ("orphan-id", 4, 2),
            ]
        )
        initial = ScoresQA(
            frame,
            expected_columns=expected_columns["scores"],
            schedule_game_ids={"fixture-1"},
        ).run_all_checks()
        foreign_key_result = next(
            result
            for result in initial
            if result["expectation"]
            == "expect_column_values_to_be_in_set:game_id"
        )
        self.assertFalse(foreign_key_result["success"])

        pipeline = self.make_pipeline(frame, {"fixture-1"})

        clean, results, _ = self.run_fixture_pipeline(pipeline)

        self.assertEqual(clean["game_id"].tolist(), ["fixture-1"])
        self.assertTrue(all(result["success"] for result in results))
        quarantine = pd.read_csv(
            Path(self.temp_dir.name) / "scores_quarantine.csv"
        )
        self.assertEqual(quarantine["game_id"].tolist(), ["orphan-id"])
        self.assertEqual(
            quarantine["quarantine_reason"].tolist(),
            ["orphan_score_game_id"],
        )

    def test_missing_schedule_columns_stop_the_pipeline(self) -> None:
        frame = make_scores([("fixture-1", 1, 0)]).drop(columns="game_id")
        pipeline = DataPipelineCSV(
            "schedule",
            log_dir=self.temp_dir.name,
        )
        pipeline.df = frame

        with self.assertRaisesRegex(DataQualityError, "Critical"):
            self.run_fixture_pipeline(pipeline)


class PipelineOutputTests(unittest.TestCase):
    def test_pipeline_outputs_and_gx_html_data_docs(self) -> None:
        with tempfile.TemporaryDirectory() as working_dir:
            shutil.copytree(PROJECT_ROOT / "data", Path(working_dir) / "data")
            completed = subprocess.run(
                [sys.executable, str(PROJECT_ROOT / "main.py")],
                cwd=working_dir,
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(
                completed.returncode,
                0,
                msg=f"Pipeline failed:\n{completed.stdout}\n{completed.stderr}",
            )

            work = Path(working_dir)
            self.assertEqual(
                len(pd.read_csv(work / "outputs/standings_historical.csv")),
                760,
            )
            self.assertEqual(
                len(pd.read_csv(work / "outputs/standings_final.csv")),
                20,
            )
            self.assertEqual(
                len(pd.read_csv(work / "outputs/recent_form.csv")),
                114,
            )

            quarantine = pd.read_csv(
                work / "logs/validation/scores_quarantine.csv"
            )
            orphan = quarantine.loc[
                quarantine["quarantine_reason"] == "orphan_score_game_id"
            ]
            self.assertEqual(
                orphan["game_id"].tolist(),
                ["750ba4567999ebd97nv18424d81bf412lz"],
            )

            docs_index = work / "logs/validation/data_docs/index.html"
            self.assertTrue(docs_index.is_file())
            docs_html = docs_index.read_text(encoding="utf-8")
            for report in (
                "premier_league_schedule",
                "premier_league_team_round_appearances",
                "premier_league_scores_scores_pre_remediation",
                "premier_league_scores_scores_post_remediation",
            ):
                self.assertIn(report, docs_html)
            self.assertIn("Expectation Suites", docs_html)
            self.assertIn("Validation", docs_html)
            self.assertEqual(
                len(list((docs_index.parent / "validations").rglob("*.html"))),
                4,
            )
            self.assertEqual(
                len(list((docs_index.parent / "expectations").glob("*.html"))),
                4,
            )
            pre_remediation_report = next(
                path
                for path in (docs_index.parent / "validations").rglob("*.html")
                if any(
                    "scores_pre_remediation" in parent.name
                    for parent in path.parents
                )
            )
            report_html = pre_remediation_report.read_text(encoding="utf-8")
            toc_html = report_html.split(
                "<strong>Table of Contents</strong>", maxsplit=1
            )[1].split("</nav>", maxsplit=1)[0]
            ordered_columns = (
                "game_id",
                "home_goals",
                "away_goals",
                "ingestion_time",
            )
            self.assertEqual(
                sorted(ordered_columns, key=toc_html.index),
                list(ordered_columns),
            )


if __name__ == "__main__":
    unittest.main()
