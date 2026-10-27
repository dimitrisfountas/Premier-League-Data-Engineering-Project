# Data Ingestion Module

## Overview

The ingestion module is the entry point for all data into the pipeline. It handles three critical responsibilities:

1. **Reading raw CSV files** from disk
2. **Validating data quality** against business rules
3. **Remediating issues** and tracking changes via audit logs

This is **STEP 1** of the master data engineering pipeline.

---

## Architecture

```
Raw CSV Files (data/)
    ↓
csv_ingestor.py
├─ Read CSV from disk
├─ Normalize data (trim whitespace, title case)
├─ Capture batch metadata (timestamp, source, path)
└─ Create schema snapshot
    ↓
Normalized DataFrame
    ↓
quality/ (Validation Rules)
├─ expectations.py (BaseQA class with common checks)
├─ ge_schedule_expectations.py (Schedule-specific validation)
└─ ge_scores_expectations.py (Scores-specific validation)
    ↓
Validation Results
    ↓
pipeline/ (Orchestration)
├─ pipeline.py (DataPipelineCSV class)
├─ Runs validation checks
├─ If failures: Apply remediation
├─ Re-validate after fixes
└─ Pass validated DataFrame downstream
    ↓
audit/ (Audit Trail)
├─ validation_csv_logger.py (ValidationCSVLogger)
└─ Log all validation results to CSV
    ↓
Validated DataFrame + Audit Logs
(Ready for STEP 2, 3, 4)
```

---

## Components

### 1. `csv_ingestor.py` - Raw Data Reading

**Class**: `CSVIngestor`

**Purpose**: Read and lightly normalize raw CSV files without validation or remediation.

**Responsibilities**:
- Load CSV file from `data/{name}.csv`
- Add batch metadata columns (timestamp, source, path)
- Apply light deterministic normalization (trim whitespace, title case team names)
- Capture schema snapshot (column names, data types, nullability)
- Return normalized DataFrame for validation layer

**Key Methods**:

| Method | Purpose |
|--------|---------|
| `read_csv()` | Load CSV from disk into pandas DataFrame |
| `assign_batch_metadata()` | Add 4 metadata columns to track ingestion context |
| `normalize()` | Trim/title-case for consistency (schedule only) |
| `capture_schema()` | Create audit trail of schema at ingestion time |
| `get_dataframe()` | Retrieve current DataFrame |

**Example Usage**:
```python
from ingestion.csv_ingestor import CSVIngestor

ingestor = CSVIngestor("schedule")
ingestor.read_csv()
ingestor.assign_batch_metadata()
ingestor.normalize()
df = ingestor.get_dataframe()
```
---

### 2. `quality/` - Data Quality Validation Rules

**Base Class**: `expectations.py` → `BaseQA`

Common validation methods available to all datasets:

| Method | Description | Example |
|--------|-------------|---------|
| `check_columns()` | Verify exact column set | Ensures no extra/missing columns |
| `check_nulls()` | Find columns with null values | Flags data completeness issues |
| `check_unique(col)` | Verify column has unique values | Validates `game_id` uniqueness |
| `check_iso_timestamp(col)` | Validate ISO-8601 format | Checks `game_start_time` |
| `check_between(col, min, max)` | Verify numeric range | Validates `round` is 1-38 |
| `check_min_value(col, min)` | Verify minimum threshold | Ensures goals >= 0 |

#### Schedule Validation: `ge_schedule_expectations.py` → `ScheduleQA`

Specific checks for schedule dataset:
- Expected columns: `game_id`, `round`, `home_team`, `away_team`, `game_start_time`
- Teams must be in valid set (20 Premier League teams)
- `game_id` must be unique
- `game_start_time` must be ISO-8601 format
- `round` must be between 1 and 38
- Business rule: Each team plays at most once per round

#### Scores Validation: `ge_scores_expectations.py` → `ScoresQA`

Specific checks for scores dataset:
- Expected columns: `game_id`, `home_goals`, `away_goals`, `ingestion_time`
- `game_id` must be unique
- `home_goals` and `away_goals` must be >= 0
- `ingestion_time` must be ISO-8601 format

**Usage**:
```python
from ingestion.quality.ge_schedule_expectations import ScheduleQA

qa = ScheduleQA(df, expected_columns, valid_teams)
results = qa.run_all_checks()
# Returns list of dicts with expectation results
```

---

### 3. `pipeline/` - Validation & Remediation Orchestration

**Class**: `pipeline.py` → `DataPipelineCSV`

**Purpose**: Orchestrate the complete ingestion workflow:
1. Ingest and normalize raw data
2. Validate against quality rules
3. If failures: Apply remediation rules
4. Re-validate after fixes
5. Log all results to audit trail

**Workflow**:

```
1. ingest_and_normalize()
   └─ Uses CSVIngestor to read, normalize, capture metadata
   
2. run_validations() [PASS 1: Pre-remediation]
   └─ Instantiates ScheduleQA or ScoresQA
   └─ Runs all_checks()
   └─ Stores results
   
3. Log pre-remediation results to audit trail
   
4. handle_failed_expectations() [IF failures exist]
   ├─ Drop rows with null values
   └─ Remove duplicate game_ids (keep first)
   
5. run_validations() [PASS 2: Post-remediation]
   └─ Re-validates to ensure fixes worked
   
6. Log post-remediation results to audit trail
```

**Key Method: `run_pipeline()`**

```python
from ingestion.pipeline.pipeline import DataPipelineCSV

pipeline = DataPipelineCSV("schedule", enable_remediation=True)
df, results, summary = pipeline.run_pipeline()
# Returns: validated DataFrame, raw results, formatted summary
```

**Parameters**:
- `dataset_name`: "schedule" or "scores"
- `enable_remediation`: True to fix issues, False for validation-only

**Output**:
- Validated DataFrame (in memory)
- List of validation results (dicts)
- Summary DataFrame (for logging)

---

### 4. `audit/` - Validation Result Logging

**Class**: `validation_csv_logger.py` → `ValidationCSVLogger`

**Purpose**: Persist validation results to CSV files for audit trail and debugging.

**Produces Two Output Files Per Dataset**:

#### 1. Summary CSV (`logs/validation/{dataset}_validation_summary.csv`)
**One row per expectation per run**

Columns:
- `source`: Dataset name (schedule/scores)
- `expectation`: Validation check name
- `success`: Boolean (True/False)
- `validation_stage`: "pre-remediation" or "post-remediation"
- `validation_run_at`: Timestamp of validation run

**Purpose**: Quick overview of pass/fail status at each stage

#### 2. Full CSV (`logs/validation/{dataset}_validation_full.csv`)
**Complete flattened results (append-only)**

Columns:
- All Great Expectations result details
- `validation_run_at`: Timestamp
- `validation_stage`: Stage identifier

**Purpose**: Detailed audit trail for investigation

**Usage**:
```python
from ingestion.audit.validation_csv_logger import ValidationCSVLogger

logger = ValidationCSVLogger("schedule")
logger.log(
    results=validation_results,
    run_ts=pd.Timestamp.now(),
    validation_stage="post-remediation"
)
```

**Key Methods**:

| Method | Purpose |
|--------|---------|
| `log(results, run_ts, stage)` | Main entry point - logs to both summary and full CSVs |
| `_write_full_logs()` | Appends complete results to full CSV (append-only) |
| `_write_summary_logs()` | Appends summary to summary CSV (append-only) |
| `make_json_serializable()` | Converts Python objects to CSV-safe formats |

**Features**:

1. **Append-Only Design**
   - Builds historical record of all validation runs
   - Useful for tracking data quality trends

2. **Timestamps**
   - Captures `validation_run_at` for each check
   - Enables trend analysis over time

3. **Stage Tracking**
   - Distinguishes "pre-remediation" vs "post-remediation"
   - Shows remediation effectiveness

4. **JSON Serialization**
   - Safely handles nested dicts, sets, lists
   - Converts to CSV-safe formats

---

## Complete Workflow Example

```python
from ingestion.pipeline.pipeline import DataPipelineCSV

# 1. Initialize pipeline for schedule dataset
pipeline = DataPipelineCSV("schedule", enable_remediation=True)

# 2. Run complete workflow:
#    - Ingestion + normalization
#    - Validation (PASS 1)
#    - Remediation (if failures)
#    - Validation (PASS 2)
#    - Audit logging
df, results, summary = pipeline.run_pipeline()

# 3. Returns:
#    - df: Validated DataFrame ready for downstream processing
#    - results: List of validation result dicts
#    - summary: Summary DataFrame logged to CSV
```

---

## Output Locations

### Validated Data (In Memory)
- DataFrames passed directly to downstream steps
- No intermediate files (efficiency)

### Validation Logs (On Disk)
```
logs/validation/
├── schedule_validation_summary.csv
├── schedule_validation_full.csv
├── scores_validation_summary.csv
└── scores_validation_full.csv
```

### Execution Logs
```
logs/pipeline/
└── pipeline_YYYYMMDD_HHMMSS.log
```

---

## Data Quality Assurance Levels

The ingestion module enforces quality at three levels:

| Level | Component | Responsibility |
|-------|-----------|-----------------|
| **L1: Normalization** | CSVIngestor | Trim whitespace, title case, metadata |
| **L2: Validation** | quality/* | Check business rules (nulls, uniqueness, ranges) |
| **L3: Remediation** | pipeline/* | Fix known issues (drop nulls, remove duplicates) |
| **L4: Audit** | audit/* | Log all results for traceability |

---

## Integration with Master Pipeline

The ingestion module is consumed by all downstream steps:

- **STEP 2** (Historical Standings): Uses validated schedule & scores DataFrames
- **STEP 3** (Final Standings): Uses validated schedule & scores DataFrames
- **STEP 4** (Form Analysis): Consumes output from STEP 2

```
[Ingestion Module] ← STEP 1
        ↓
   [Validated Data]
        ↓
   ┌────┴────┐
   ↓         ↓
STEP 2    STEP 3
   ↓         ↓
   └────┬────┘
        ↓
      STEP 4
```

---

## Error Handling

If validation fails and remediation is enabled:
1. Affected rows are logged to console/file
2. Specific remediation rules are applied
3. Re-validation confirms fixes
4. All changes are tracked in audit logs

If validation fails and remediation is disabled:
1. Pipeline continues with unmodified data
2. Failures are logged for manual review
3. Downstream steps receive unvalidated data (proceed with caution)

---

## Related Documentation

- [Main Pipeline](../main.py) - Master orchestration (all 4 steps)
- [Analytics - Standings](../analytics/standings/) - Final season rankings
- [Analytics - Historical Standings](../analytics/historical_standings/) - Weekly progression
- [Analytics - Form Analysis](../analytics/form/) - Top 3 teams per week
- [Validation & Ingestion README](../VALIDATION_INGESTION_README.md) - Detailed technical docs

---

## Key Concepts

### Separation of Concerns
- **Ingestion** ≠ Validation ≠ Remediation
- Each layer has clear responsibility
- Issues caught at appropriate layer

### Audit Trail
- Every validation run is logged
- Pre/post remediation results compared
- Can trace what changed and when

### Flexibility
- Remediation can be enabled/disabled
- Works with or without fixes
- Suitable for exploration and production

### Production Ready
- Batch metadata captures lineage
- Timestamps enable reproducibility
- Append-only logs prevent data loss
