# ingestion/csv_ingestor.py

from pathlib import Path
from datetime import datetime, timezone
import pandas as pd
import os

class CSVIngestor:
    """
    STEP 1 - Ingestion Layer: Read and lightly normalize raw CSV data.
    
    ╔══════════════════════════════════════════════════════════════════════════════╗
    ║                    CRITICAL: INGESTION IS NOT VALIDATION                     ║
    ║                                                                              ║
    ║  This class reads and normalizes data. Validation happens DOWNSTREAM in      ║
    ║  DataPipelineCSV with Great Expectations. This separation ensures clean      ║
    ║  responsibilities: ingest data, then validate it.                           ║
    ╚══════════════════════════════════════════════════════════════════════════════╝
    
    ROLE IN PIPELINE:
    This is the first stage of the data pipeline, responsible for reading raw CSV files
    and preparing them for validation. It operates BEFORE validation/remediation.
    
    RESPONSIBILITIES:
    1. Read CSV files from data/ directory (schedule.csv, scores.csv)
    2. Capture batch-level metadata (ingestion timestamp, file path)
    3. Apply light normalization (team name trimming/title casing)
    4. Capture schema snapshot (column info, nullability, data types)
    5. Return normalized dataframe to DataPipelineCSV for validation
    
    
    WORKFLOW:
    1. Initialize with dataset name ("schedule" or "scores")
    2. read_csv() → Load from data/{name}.csv
    3. assign_batch_metadata() → Add ingestion timestamp, source, path
    4. normalize() → Trim/title-case team names (schedule only)
    5. capture_schema() → Record column schema snapshot
    6. Return to DataPipelineCSV for validation
    
    OUTPUT TO DOWNSTREAM:
    - Normalized DataFrame with batch metadata columns:
      * updated_at: Ingestion timestamp (UTC)
      * source: Dataset name (schedule/scores)
      * ingestion_path: Absolute file path
      * last_updated_at: When record was last modified
    - Schema snapshot: One row per column with dtype and nullability
    - Ready for validation and remediation
    
    EXAMPLES OF WHAT THIS CLASS DOES:
    - Normalize team names: "  manchester united  " → "Manchester United"
    - Add timestamp: updated_at = 2025-09-01 08:00:00 UTC
    - Record path: ingestion_path = /full/path/to/data/schedule.csv
    
    """

    def __init__(self, source: str):
        """
        Initialize the CSVIngestor.

        Parameters:
        ----------
        source : str
            Logical dataset name ("schedule" or "scores").

        base_path : str | Path
            Directory where CSV files live.
            Defaults to the current working directory + "/data".
        """

        # Logical source identifier
        self.source = source

        # Resolve expected file name
        self.csv_path = Path(os.path.join('data', f"{self.source}.csv"))

        # Batch metadata (one value per ingestion run)
        self.batch_metadata: dict | None = None

        # Placeholder for the dataframe
        self.df: pd.DataFrame | None = None

    # --------------------------------------------------------

    def read_csv(self) -> pd.DataFrame:
        """
        Reads raw CSV data from disk and stores it in self.df.
        
        ⚠️  NOTE: This is PURE READING only. No transformation, no cleaning.
        The returned DataFrame will be passed to DataPipelineCSV for validation.
        
        Validation checks will determine:
        - Whether null values are acceptable
        - Whether data types are correct
        - Whether duplicates should be removed
        
        This method only:
        1. Checks file exists
        2. Checks file is not empty
        3. Reads into pandas DataFrame

        Raises:
        -------
        FileNotFoundError : If the CSV file does not exist.
        ValueError : If the CSV file is empty.

        Returns:
        -------
        pd.DataFrame : Raw dataframe read from CSV.
        """
        if not self.csv_path.exists():
            raise FileNotFoundError(f"{self.csv_path} does not exist")

        self.df = pd.read_csv(self.csv_path)

        if self.df.empty:
            raise ValueError(f"{self.csv_path} is empty")

        return self.df

    # --------------------------------------------------------

    def _build_batch_metadata(self, path: Path) -> dict:
        """
        Creates batch-level technical metadata.

        Parameters:
        ----------
        path : Path
            Path to the ingested CSV file.

        Returns:
        -------
        dict : Metadata dictionary for the ingestion run.
        """
        return {
            "updated_at": datetime.now(timezone.utc),  # UTC ingestion timestamp
            "source_csv": self.source,                 # logical dataset name
            "ingestion_path": str(path.resolve()),     # absolute file path
        }

    # --------------------------------------------------------

    def assign_batch_metadata(self) -> None:
        """
        Assigns batch-level metadata columns to the dataframe.
        
        PURPOSE: Create an audit trail of when and how data entered the system.
        
        This adds 4 columns to every row:
        1. updated_at: UTC timestamp when ingestion ran (same for all rows in batch)
        2. source: Dataset name ("schedule" or "scores")
        3. ingestion_path: Full file path (proves which file was read)
        4. last_updated_at: Record-level timestamp (same as updated_at on ingestion)
        
        WHY THESE FIELDS MATTER:
        - updated_at + source allow grouping "this schedule load" vs "that scores load"
        - ingestion_path provides full traceability (which file? what location?)
        - last_updated_at will be updated during remediation if values change
        
        EXAMPLE:
        Before:
        | game_id | home_team | away_team |
        |---------|-----------|-----------|
        | 1       | arsenal   | villa     |
        
        After:
        | game_id | home_team | away_team | updated_at          | source   | ingestion_path            | last_updated_at     |
        |---------|-----------|-----------|---------------------|----------|---------------------------|---------------------|
        | 1       | arsenal   | villa     | 2025-09-01 08:00:00 | schedule | /full/path/data/schedule  | 2025-09-01 08:00:00 |

        Raises:
        -------
        ValueError : If the dataframe has not been loaded yet.
        """
        if self.df is None:
            raise ValueError("Dataframe not loaded. Call read_csv() first.")

        self.batch_metadata = self._build_batch_metadata(self.csv_path)
        self.df = self.df.assign(
            updated_at=self.batch_metadata["updated_at"],
            source=self.batch_metadata["source_csv"],
            ingestion_path=self.batch_metadata["ingestion_path"],
            last_updated_at=datetime.now(timezone.utc)
        )

    # --------------------------------------------------------

    def normalize(self) -> pd.DataFrame:
        """
        Applies light, deterministic normalization rules.
        
        PURPOSE: Standardize team names to prevent validation failures from minor formatting.
        
        This is MINIMAL normalization:
        - Only for 'schedule' dataset (team names need consistency)
        - Only trim whitespace and apply Title Case
        - NOT a data fix (this is normalization, not remediation)
        
        EXAMPLE:
        Input:                    Output:
        "  manchester united  " → "Manchester United"
        "ARSENAL"              → "Arsenal"
        "  west ham  "         → "West Ham"
        

        Returns:
        -------
        pd.DataFrame : Normalized dataframe.
        """
        if self.df is None:
            raise ValueError("Dataframe not loaded. Call read_csv() first.")

        if self.source == "schedule":
            self.df["home_team"] = self.df["home_team"].astype(str).str.strip().str.title()
            self.df["away_team"] = self.df["away_team"].astype(str).str.strip().str.title()

        return self.df

    # --------------------------------------------------------

    def capture_schema(self) -> pd.DataFrame:
        """
        Captures a technical snapshot of the dataframe schema.
        
        PURPOSE: Create audit trail showing the schema at ingestion time.
        
        This is important because:
        1. **Schema Drift Detection**: Compare schema at different times
           - Did a column disappear? New column added?
           - Did data types change unexpectedly?
        
        2. **Lineage Tracking**: Know what the data looked like when ingested
           - Useful for debugging issues found downstream
           - Helps reproduce issues with same data shape
        
        3. **Data Governance**: Document nullability at each ingestion
           - Which columns had nulls? How many?
           - Is this expected or a data quality issue?
        
        OUTPUT EXAMPLE:
        | source   | column_name | dtype   | nullable | captured_at         |
        |----------|-------------|---------|----------|---------------------|
        | schedule | game_id     | int64   | False    | 2025-09-01 08:00:00 |
        | schedule | home_team   | object  | True     | 2025-09-01 08:00:00 |
        | schedule | away_team   | object  | True     | 2025-09-01 08:00:00 |
        | schedule | game_start  | object  | False    | 2025-09-01 08:00:00 |
        
        HOW IT'S USED:
        - Validation checks reference this to know what to expect
        - Remediation uses this to understand data structure
        - Audit trails show what data looked like at each step

        Returns:
        -------
        pd.DataFrame : One row per column describing name, dtype, nullability, and capture timestamp.
        """
        if self.df is None:
            raise ValueError("Dataframe not loaded. Call read_csv() first.")

        captured_at = datetime.now(timezone.utc)
        return pd.DataFrame(
            {
                "source": self.source,
                "column_name": col,
                "dtype": str(self.df[col].dtype),
                "nullable": self.df[col].isna().any(),
                "captured_at": captured_at,
            }
            for col in self.df.columns
        )

    # --------------------------------------------------------

    def get_dataframe(self) -> pd.DataFrame:
        """
        Returns the current dataframe.

        Raises:
        -------
        ValueError : If the dataframe has not been loaded yet.

        Returns:
        -------
        pd.DataFrame : Current dataframe.
        """
        if self.df is None:
            raise ValueError("Dataframe not loaded. Call read_csv() first.")
        return self.df
