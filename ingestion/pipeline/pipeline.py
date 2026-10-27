import sys
from pathlib import Path

# Add parent directory to path to allow imports from root
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import pandas as pd
from ingestion.csv_ingestor import CSVIngestor
from ingestion.quality.ge_schedule_expectations import ScheduleQA
from ingestion.quality.ge_scores_expectations import ScoresQA
from ingestion.audit.validation_csv_logger import ValidationCSVLogger
from constants import expected_columns, valid_teams

# --------------------------------------------------------
# Data Pipeline Class (Read, Normalize, Validate, Log)
#--------------------------------------------------------

class DataPipelineCSV:
    """
    End-to-end pipeline for:
    - CSV ingestion
    - Light normalization
    - Data quality validation (Great Expectations)
    - Append-only validation logging (CSV)

    This pipeline is intentionally simple and explicit.
    """

    def __init__(self, dataset_name: str, enable_remediation: bool = False):
        self.dataset_name = dataset_name
        self.enable_remediation = enable_remediation

        # Runtime state
        self.df = None
        self.results = []

        # Single timestamp per pipeline run (lineage-safe)
        self.run_ts = pd.Timestamp.now(tz="UTC")

        # Validation logger (audit layer)
        self.logger = ValidationCSVLogger(dataset_name)

    # --------------------------------------------------------
    # Step 1: Ingest and normalize raw CSV data
    # --------------------------------------------------------
    def ingest_and_normalize(self):
        ingestor = CSVIngestor(self.dataset_name)

        # Raw CSV read + batch metadata capture
        ingestor.read_csv()
        
        # Light deterministic cleanup (no validation here)
        ingestor.normalize()

        # Attach batch-level metadata to dataframe
        ingestor.assign_batch_metadata()

        self.df = ingestor.get_dataframe()
        
        return self.df

    # --------------------------------------------------------
    # Step 2: Run data quality validations (bronze contract)
    # --------------------------------------------------------
    def run_validations(self):
        if self.df is None:
            raise ValueError("DataFrame not loaded. Run ingest_and_normalize first.")

        # Reset results each run to avoid duplicates
        self.results = []

        if self.dataset_name == "schedule":
            print("Running schedule validations...")
            self.results = ScheduleQA(
                self.df,
                expected_columns=expected_columns[self.dataset_name],
                valid_teams=valid_teams
            ).run_all_checks()
        elif self.dataset_name == "scores":
            print(f"Running scores validations...")
            self.results = ScoresQA(
                self.df,
                expected_columns=expected_columns[self.dataset_name]
            ).run_all_checks()
        else:
            raise ValueError(f"Unknown dataset: {self.dataset_name}")

        return self.results


    # --------------------------------------------------------
    # Step 3: Handle failed expectations (remediation)
    # --------------------------------------------------------
    def handle_failed_expectations(self):
        """
        Apply remediation logic based on failed validation results.
        - Drop rows with nulls
        - Remove duplicate game_ids (keep first occurrence)
        """
        if self.df is None:
            raise ValueError("DataFrame not loaded. Run ingest_and_normalize first.")
        
        initial_rows = len(self.df)
        
        # Check for null values and drop rows with any nulls
        nulls_check = next((r for r in self.results if "not_be_null" in r.get("expectation", "")), None)
        if nulls_check and not nulls_check["success"]:
            print(f"Dropping rows with null values...")
            self.df = self.df.dropna()
            rows_after_nulls = len(self.df)
            print(f"  Dropped {initial_rows - rows_after_nulls} rows with nulls")
        
        # Check for duplicate game_ids and keep only first occurrence
        duplicates_check = next((r for r in self.results if "game_id" in r.get("expectation", "") and "unique" in r.get("expectation", "")), None)
        if duplicates_check and not duplicates_check["success"]:
            print(f"Removing duplicate game_ids (keeping first occurrence)...")
            rows_before_dedup = len(self.df)
            self.df = self.df.drop_duplicates(subset=["game_id"], keep="first")
            print(f"  Removed {rows_before_dedup - len(self.df)} duplicate rows")
        
        return self.df

    # --------------------------------------------------------
    # Step 4: Persist validation logs (append-only)
    # --------------------------------------------------------
    def run_pipeline(self):
        # Ingestion + normalization
        self.ingest_and_normalize()
        
        # Data quality checks (first pass)
        print("\n" + "="*60)
        if self.enable_remediation:
            print("VALIDATION PASS 1: Before Remediation")
        else:
            print("VALIDATION: Data Quality Check")
        print("="*60)
        self.run_validations()
        results_before = self.results.copy()
        
        # Log pre-remediation results
        validation_stage = "pre-remediation" if self.enable_remediation else "validation"
        self.logger.log(
            results=results_before,
            run_ts=self.run_ts,
            validation_stage=validation_stage
        )
        
        # Check if there are actual failures to remediate
        has_failures = any(not r["success"] for r in results_before)
        
        # Handle failed expectations (remediation) - only if enabled AND failures exist
        if self.enable_remediation and has_failures:
            print("\n" + "="*60)
            print("REMEDIATION: Handling Failed Expectations")
            print("="*60)
            self.handle_failed_expectations()
            
            # Data quality checks (second pass - after remediation)
            print("\n" + "="*60)
            print("VALIDATION PASS 2: After Remediation")
            print("="*60)
            self.run_validations()
            results_after = self.results.copy()
            
            # Log post-remediation results
            summary_df = self.logger.log(
                results=results_after,
                run_ts=self.run_ts,
                validation_stage="post-remediation"
            )
            
            # Compare results
            print("\n" + "="*60)
            print("IMPROVEMENT SUMMARY")
            print("="*60)
            failed_before = sum(1 for r in results_before if not r["success"])
            failed_after = sum(1 for r in results_after if not r["success"])
            print(f"Failed expectations before remediation: {failed_before}")
            print(f"Failed expectations after remediation: {failed_after}")
            print(f"Improvements: {failed_before - failed_after}")
        else:
            if self.enable_remediation:
                print("\n" + "="*60)
                print("REMEDIATION: Not needed - all tests passed")
                print("="*60)
            summary_df = self.logger.log(
                results=results_before,
                run_ts=self.run_ts,
                validation_stage="validation"
            )

        # Return dataframe, raw results, and formatted summary
        return self.df, self.results, summary_df

# --------------------------------------------------------
