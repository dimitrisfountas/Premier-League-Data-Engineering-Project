from pathlib import Path
import pandas as pd
from typing import List, Dict, Any


def make_json_serializable(obj):
    """Recursively convert non-CSV/JSON-safe objects."""
    if isinstance(obj, set):
        return list(obj)
    elif isinstance(obj, dict):
        return {k: make_json_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [make_json_serializable(x) for x in obj]
    else:
        return obj


class ValidationCSVLogger:
    """
    Handles persistence of Great Expectations validation results.

    Produces:
    - Summary CSV: one row per expectation per run
    - Full CSV: full GE result flattened (append-only)
    """

    def __init__(self, dataset_name: str, log_dir: str = "logs/validation"):
        self.dataset_name = dataset_name
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)

        self.summary_path = self.log_dir / f"{dataset_name}_validation_summary.csv"
        self.full_path = self.log_dir / f"{dataset_name}_validation_full.csv"
        self.quarantine_path = self.log_dir / f"{dataset_name}_quarantine.csv"

    # -----------------------------------------------------
    def log(self, results: List[Dict[str, Any]], run_ts: pd.Timestamp, validation_stage: str = "post-remediation"):
        self._write_full_logs(results, run_ts, validation_stage)
        summary_df = self._write_summary_logs(results, run_ts, validation_stage)
        return summary_df

    def log_quarantine(self, rows: pd.DataFrame, run_ts: pd.Timestamp) -> None:
        audit_rows = rows.copy()
        audit_rows["quarantined_at"] = run_ts
        audit_rows.to_csv(
            self.quarantine_path,
            mode="a",
            index=False,
            header=(
                not self.quarantine_path.exists()
                or self.quarantine_path.stat().st_size == 0
            ),
        )

    # -----------------------------------------------------
    def _write_full_logs(self, results, run_ts, validation_stage: str):
        
        rows = []

        for r in results:
            row = make_json_serializable(r)
            row["validation_run_at"] = run_ts
            row["validation_stage"] = validation_stage
            rows.append(row)

        df = pd.DataFrame(rows)

        df.to_csv(
            self.full_path,
            mode="a",
            index=False,
            header=not self.full_path.exists()
        )

    # -----------------------------------------------------
    def _write_summary_logs(self, results, run_ts, validation_stage: str) -> pd.DataFrame:
        df = pd.DataFrame([
            {
                "source": self.dataset_name,
                "expectation": r.get("expectation", "result"),
                "success": r["success"],
                "validation_stage": validation_stage,
                "validation_run_at": run_ts,
            }
            for r in results
        ])

        write_header = not self.summary_path.exists() or self.summary_path.stat().st_size == 0

        df.to_csv(
            self.summary_path,
            mode="a",
            index=False,
            header=write_header,
        )

        return df
