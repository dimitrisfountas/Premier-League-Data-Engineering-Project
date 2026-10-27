# Master Data Engineering Pipeline

Complete end-to-end data engineering solution for Premier League football analytics.

---

## Overview

**main.py** is the master orchestrator that executes a 4-step pipeline to process, validate, and analyze Premier League match data.

```
Raw CSV Data → Validation → Historical Analysis → Form Analysis → Final Standings
(380 games)                (38 snapshots)       (SQL-based)     (Season totals)
```

**Execution Time**: ~5-10 seconds  
**Output Files**: Analytics, validation, and quarantine reports
**Processors**: Python, pandas, Great Expectations, SQLite

---

## Quick Start

### Run the Complete Pipeline

```bash
pip install -r requirements.txt
python main.py
```

**Output:**
- Console: Progress updates, status messages, sample results
- File: `logs/pipeline/pipeline_YYYYMMDD_HHMMSS.log` (detailed debug log)
- Deliverables: Analytics CSVs, validation/quarantine audit CSVs, and GX HTML Data Docs

### Run the Automated Checks

```bash
python -m unittest discover -s tests -v
```

The integration test runs the complete pipeline in a temporary working
directory and verifies output counts, orphan-score reconciliation, and GX HTML
Data Docs generation.

### Expected Console Output

```
================================================================================
DATA ENGINEERING PIPELINE - MASTER ORCHESTRATION
================================================================================

================================================================================
STEP 1: DATA VALIDATION & REMEDIATION
================================================================================
Processing schedule and scores datasets...
Status: Ingestion -> Validation -> Remediation

Validating SCHEDULE...
[OK] Data validation completed successfully

Validating SCORES...
[OK] Data validation completed successfully

================================================================================
STEP 2: HISTORICAL STANDINGS SIMULATION
================================================================================
Simulating weekly pipeline runs from Aug 25, 2025 to May 11, 2026
Status: 38 Monday snapshots x 20 teams = 760 historical records
[OK] Historical standings simulation completed successfully

================================================================================
STEP 3: FINAL STANDINGS CALCULATION
================================================================================
Calculating final season standings for all 20 teams

================================================================================
FINAL LEAGUE STANDINGS
================================================================================
   ranking         team  wins  draws  losses  goals_for  goals_against  points
        1     Manchester  ...
        2      Liverpool  ...
       ...
[OK] Final standings calculation completed successfully

================================================================================
STEP 4: RECENT FORM ANALYSIS
================================================================================
Identifying top 3 teams by recent performance at each Monday
Technology: SQLite in-memory database with SQL JOIN queries

Sample recent form results (first 15 rows):
form_ranking     team  goals_for  goals_against  points  updated_at
            1  Newcastle  ...
            2  Liverpool  ...
[OK] Form analysis completed successfully

================================================================================
PIPELINE EXECUTION COMPLETED
================================================================================
```

---

## 4-Step Pipeline Architecture

### STEP 1: Data Validation & Remediation

**Purpose**: Ensure input data quality before downstream processing

**Input**:
- `data/schedule.csv` - Match schedules (date, teams, kickoff time)
- `data/scores.csv` - Match results (goals scored by each team)

**Process**:
1. **Ingestion**: Load CSV files into pandas DataFrames
2. **Validation**: Run Great Expectations suites against the pandas DataFrames
   - Schema, nullability, unique IDs, timestamps, team membership, and ranges
   - Team-round uniqueness and score-to-schedule ID integrity
   - Verify exactly one score per scheduled fixture
3. **Remediation**: Apply documented score-only policies
   - Collapse duplicate fixture rows only when their scores agree
   - Prefer a complete score over a compatible incomplete duplicate
   - Quarantine conflicting, incomplete, and orphan records with reasons
   - Revalidate; unresolved critical failures stop the pipeline
4. **Storage**: Keep validated DataFrames in memory and render Great Expectations HTML Data Docs

**Output**:
```
logs/validation/
├── schedule_validation_summary.csv    (Passed/Failed counts)
├── schedule_validation_full.csv       (Detailed results)
├── scores_validation_summary.csv      (Passed/Failed counts)
├── scores_validation_full.csv         (Detailed results)
├── scores_quarantine.csv              (Rows excluded with remediation reasons)
└── data_docs/
    ├── index.html                     (Single native GX Data Docs site)
    ├── expectations/                   (Expectation suites)
    └── validations/                    (Successful and failed validation results)
```

**Sample Validation Summary**:
```
check_name                           total_tests  passed  failed
schedule.nulls_across_all_columns           1       1       0
schedule.duplicates_entire_row              1       1       0
schedule.expected_columns                   1       1       0
schedule.date_format_iso8601                1       1       0
...
```

**Files**:
- [ingestion/csv_ingestor.py](ingestion/csv_ingestor.py) - CSV loading
- [quality/expectations.py](quality/expectations.py) - Validation rules

---

### STEP 2: Historical Standings Simulation

**Purpose**: Calculate league standings at 38 key points in time (every Monday)

**Input**: Validated schedule and scores DataFrames from STEP 1

**Process**:
1. **Iterate through 38 Mondays** (Aug 25, 2025 → May 11, 2026)
2. **For each Monday**:
   - Filter matches where kickoff ≤ Monday 23:59:59 UTC
   - Calculate cumulative standings:
     - Wins, draws, losses (how many of each)
     - Goals for, goals against (totals)
     - Points (wins×3 + draws)
   - Rank teams by: Points DESC → Goal Diff DESC → Goals For DESC
3. **Aggregate**: Stack all 38 snapshots into single file

**Output**:
```
outputs/standings_historical.csv (760 rows)
```

**Schema**:
```
ranking,team,wins,draws,losses,goals_for,goals_against,points,updated_at
1,Manchester City,5,1,0,19,4,16,2025-08-25T08:00:00Z
2,Liverpool,5,0,1,18,5,15,2025-08-25T08:00:00Z
3,Newcastle,4,1,1,16,6,13,2025-08-25T08:00:00Z
...
1,Manchester City,38,10,0,111,33,124,2026-05-11T08:00:00Z
2,Liverpool,36,8,4,109,41,116,2026-05-11T08:00:00Z
...
```

**Key Features**:
- 38 weekly snapshots (38 Mondays)
- 20 teams per snapshot (760 total rows)
- Chronological progression from season start to end
- Cumulative metrics (totals increase over time)

**Use Cases**:
- Track league position changes week-by-week
- Analyze promotion/relegation battles
- Study consistency of top teams
- Compare performance at different season phases

**Files**:
- [analytics/historical_standings_pipeline.py](analytics/historical_standings_pipeline.py)
- [analytics/historical_standings_calculator.py](analytics/historical_standings_calculator.py)

---

### STEP 3: Final Standings Calculation

**Purpose**: Calculate final season standings using all 380 matches

**Input**: Validated schedule and scores DataFrames from STEP 1

**Process**:
1. **Merge** schedule with scores on game_id
2. **Aggregate** for each team:
   - Wins, draws, losses (entire season, 38 matches per team)
   - Goals for, goals against (season totals)
   - Points (entire season)
3. **Rank** all 20 teams by: Points DESC → Goal Diff DESC → Goals For DESC → Team Name ASC
4. **Output** to CSV

**Output**:
```
outputs/standings_final.csv (20 rows)
```

**Schema**:
```
ranking,team,wins,draws,losses,goals_for,goals_against,points
1,Manchester City,28,5,5,97,34,89
2,Liverpool,26,7,5,95,43,85
3,Newcastle,25,8,5,88,41,83
4,Arsenal,24,6,8,86,52,78
...
20,Struggling Team,10,3,25,45,82,33
```

**Key Metrics**:
- **Points**: Win=3, Draw=1, Loss=0
- **Goal Difference**: Goals For - Goals Against
- **Win Percentage**: (Wins / 38) × 100

**Files**:
- [standings/standings_pipeline.py](standings/standings_pipeline.py)
- [standings/standings_calculator.py](standings/standings_calculator.py)

---

### STEP 4: Recent Form Analysis

**Purpose**: Identify top 3 teams based on their **last 5 matches** performance

**Input**: `outputs/standings_historical.csv` from STEP 2

**Technology**: SQLite in-memory database with SQL window functions

**Process**:
1. **Load** standings_historical.csv into SQLite table
2. **For each Monday**:
   - Find each team's standing from 5 matches ago (using game count delta)
   - Calculate last 5 match stats via delta: current - 5games_ago
   - Reconstruct form string: "WDWWL" (from W/D/L deltas)
   - Rank: Points DESC → Goal Diff DESC → Goals Against ASC → Team Name ASC
   - Extract top 3 teams
3. **Aggregate** results across all Mondays
4. **Output** to CSV

**Output**:
```
outputs/recent_form.csv (~114 rows)
```

**Schema**:
```
form_ranking,team,goals_for,goals_against,points,updated_at
1,Newcastle,7,0,15,2025-08-25T08:00:00Z
2,Liverpool,6,1,14,2025-08-25T08:00:00Z
3,Manchester City,5,2,13,2025-08-25T08:00:00Z
1,Manchester City,9,1,15,2025-09-01T08:00:00Z
2,Liverpool,8,2,14,2025-09-01T08:00:00Z
...
```

**Key Concepts**:

**Form Points** (Last 5 Matches):
- Range: 0-15 (0 losses to 5 wins)
- 15 = 5 wins (best)
- 13-14 = 4+ wins or 3 wins + 1+ draw
- 0 = 5 losses (worst)

**Form String** Examples:
- `WWWWW` → 5 wins (15 pts, elite form)
- `LDWWW` → Loss, Draw, 3 wins (11 pts, improving)
- `DDDDD` → 5 draws (5 pts, consistent but not winning)
- `LLLLL` → 5 losses (0 pts, crisis form)

**Ranking Logic** (Priority):
1. **Points** (DESC) - Most important
2. **Goal Difference** (DESC) - Quality of wins
3. **Goals Against** (ASC) - Defensive strength
4. **Team Name** (ASC) - Alphabetical tie-breaker

**Interesting Insights**:
- Top 3 rotates based on recent performance
- Shows form momentum (improving/declining)
- Identifies surprise performers mid-season
- Highlights defensive struggles (high goals against)

**Files**:
- [analytics/form/form_analysis.py](analytics/form/form_analysis.py)
- [analytics/form/README_NEW.md](analytics/form/README_NEW.md)

---

## Complete Data Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│                         RAW DATA FILES                              │
│  data/schedule.csv (380 games)  │  data/scores.csv (380 games)      │
└──────────────────────┬──────────────────────┬──────────────────────┘
                       │                      │
                       v                      v
        ┌──────────────────────────────────────────────┐
        │  STEP 1: Validation & Remediation            │
        │  (12+ checks, fix nulls/duplicates)          │
        │  OUTPUT: logs/validation/*.csv               │
        └──────────┬───────────────────────────────────┘
                   │
      validated_data (DataFrame dict)
      ├─ schedule: 380 rows ✓
      ├─ scores: 380 rows ✓
      └─ metadata: validated ✓
         │                           │
         ├─────────────────┐         └────────────────────┐
         │                 │                              │
         v                 v                              v
    ┌─────────┐        ┌──────────┐              ┌──────────────┐
    │ STEP 2  │        │ STEP 3   │              │  STEP 4      │
    │History │        │ Final    │              │  Form        │
    │Standings│        │Standings │              │  Analysis    │
    │ (38 wks)│        │(380 games)│             │ (SQLite)     │
    │         │        │          │              │              │
    │760 rows │        │20 rows   │              │114 rows      │
    │         │        │          │              │              │
    └────┬────┘        └────┬─────┘              └──────┬───────┘
         │                  │                          │
         v                  v                          v
    standings_         standings_            recent_form.csv
    historical.csv    final.csv             (Top 3 teams/week)
         │
         │ INPUT TO STEP 4
         └──────────────────────────────────────>
```

---

## File Structure

```
data-engineer-assessment/
├── main.py                              ← YOU ARE HERE (Master Orchestrator)
├── data/
│   ├── schedule.csv                     (Input: 380 matches)
│   └── scores.csv                       (Input: 380 match results)
├── outputs/
│   ├── standings_historical.csv         (STEP 2: 760 rows)
│   ├── standings_final.csv              (STEP 3: 20 rows)
│   └── recent_form.csv                  (STEP 4: ~114 rows)
├── logs/
│   ├── validation/
│   │   ├── schedule_validation_summary.csv
│   │   ├── schedule_validation_full.csv
│   │   ├── scores_validation_summary.csv
│   │   └── scores_validation_full.csv
│   ├── standings/
│   │   ├── standings_historical.log
│   │   └── standings_final.log
│   └── pipeline/
│       └── pipeline_YYYYMMDD_HHMMSS.log (← Current run logs)
└── analytics/
    ├── historical_standings/
    │   ├── historical_standings_pipeline.py
    │   └── historical_standings_calculator.py
    ├── standings/
    │   ├── standings_pipeline.py
    │   └── standings_calculator.py
    └── form/
        ├── form_analysis.py
        └── README_NEW.md
```

---

## How to Use Outputs

### 1. standings_historical.csv - Track League Evolution

**Use For**:
- Timeline analysis (how teams moved up/down)
- Trend analysis (consistent improvers vs. inconsistent teams)
- Historical reports (performance at specific dates)

**Example Query**:
```sql
-- Who was 1st at Christmas (Jan 1)?
SELECT * FROM standings_historical 
WHERE ranking = 1 AND updated_at = '2025-12-29T08:00:00Z';

-- How did Liverpool's points progression look?
SELECT updated_at, points FROM standings_historical
WHERE team = 'Liverpool'
ORDER BY updated_at;
```

### 2. standings_final.csv - Season Champion & Final Rankings

**Use For**:
- Season review
- Awards (best offense, best defense)
- Playoff/European qualification positions
- League table for reports

**Key Findings**:
```
1st Place: 89 points → Premier League Champion
2nd-4th: 85-83 points → UEFA Champions League qualification
5th-7th: 74-72 points → UEFA Europa League qualification
18th-20th: < 45 points → Relegated (Championship next season)
```

### 3. recent_form.csv - In-Season Performance Tracker

**Use For**:
- Form analysis for upcoming matches
- Momentum tracking (is team improving?)
- Injury crisis detection (sudden form drop)
- Confidence building (on a run of wins)

**Example Insights**:
```
Week of Jan 13:
1. Team A: WWWWW (15 pts) - Perfect form, attacking threat
2. Team B: WDWDW (11 pts) - Solid but drawing too many
3. Team C: LDWWW (11 pts) - Improving trend

Week of Jan 20:
1. Team A: LWWWW (12 pts) - Lost one but still strong
2. Team D: WWWWW (15 pts) - NEW TOP FORM
3. Team B: DDDWW (7 pts)  - Form collapse from 11 to 7 pts!
```

---

## Logging & Debugging

### Log Files Location

```
logs/pipeline/pipeline_YYYYMMDD_HHMMSS.log
```

Contains:
- Complete execution trace (DEBUG level)
- All step outputs and results
- Error details if failures occur
- Performance metrics

### Log Levels

| Level | Purpose | When |
|-------|---------|------|
| DEBUG | Detailed trace | Internal processing steps |
| INFO | Status updates | Step completions, progress |
| WARNING | Issues fixed | Validation remediation |
| ERROR | Failures | Stops pipeline execution |

### Common Issues & Solutions

| Issue | Cause | Solution |
|-------|-------|----------|
| "File not found" | Missing `data/schedule.csv` | Check data/ directory has both CSVs |
| Validation fails | Nulls in data | Pipeline auto-remediates, check logs |
| Slow execution | Large dataset | Normal (380 games × processing) |
| Form CSV empty | No standings_historical.csv | Ensure STEP 2 completed |

---

## Dependencies

### Python Libraries

```
pandas          - DataFrames and CSV I/O
great-expectations - Data validation suites and results
sqlite3         - Built-in SQL engine (no install needed)
logging         - Built-in logging framework
pathlib         - File path utilities
datetime        - Timestamp handling
```

### Data Files

```
data/schedule.csv    - Match dates, teams, kickoff times
data/scores.csv      - Match goals, results
```

### Directory Structure

```
./data/               - Input CSVs (must exist)
./outputs/            - Output CSVs (auto-created)
./logs/               - Log files (auto-created)
```

---

## Performance Characteristics

| Metric | Value |
|--------|-------|
| Total Execution Time | 5-10 seconds |
| STEP 1 (Validation) | ~1 second |
| STEP 2 (Historical) | ~2-3 seconds (38 iterations) |
| STEP 3 (Final) | ~1 second |
| STEP 4 (Form) | ~2-3 seconds (SQLite queries) |

---

## Execution Modes

### Standard Execution
```bash
python main.py
```
Runs all 4 steps in sequence, displays progress.

### Check Logs
```bash
tail -f logs/pipeline/pipeline_*.log
```
Follow live log output during execution.

### Suppress Console Output
```bash
python main.py 2>/dev/null
```
Only file logs (no console spam).

---

## Deliverables Summary

### Generated After Running main.py

| File | Rows | Purpose |
|------|------|---------|
| standings_historical.csv | 760 | Week-by-week league progression |
| standings_final.csv | 20 | Final season standings & champion |
| recent_form.csv | ~114 | Top 3 teams by last 5 matches each week |
| Validation logs (4 files) | Varies | Great Expectations audit trail |
| scores_quarantine.csv | Varies | Rows excluded with auditable reasons |

Great Expectations Data Docs are GX Core's built-in HTML documentation. One
native site contains all expectation suites and validation results from the
pipeline run. To view it with working report links, serve the generated site
over localhost from the project root:

```powershell
.\.venv\Scripts\python.exe -m http.server 8765 --directory .\logs\validation\data_docs
```

Then open `http://localhost:8765/` in your browser. Press `Ctrl+C` in the
terminal to stop the local server. Serving over HTTP avoids browser restrictions
that can break links when opening nested reports directly with `file://`.

---

## Architecture Insights

### Why This 4-Step Design?

1. **Validation First** - Catch data quality issues before analysis
2. **Historical Simulation** - Enable time-series analysis and trend tracking
3. **Final Standings** - Season outcome and champion identification
4. **Form Analysis** - Real-time performance assessment (uses STEP 2 output)

### Data Independence

- **STEP 3 (Final Standings)**: Uses STEP 1 data directly (no dependency on STEP 2)
- **STEP 4 (Form Analysis)**: Depends on STEP 2 output (consumes standings_historical.csv)
- **STEP 2 (Historical)**: Independent, can run in parallel with STEP 3

### SQL vs. Python

- **STEP 1**: Python validation rules
- **STEP 2-3**: Python pandas for aggregation
- **STEP 4**: SQLite for window function analysis (LAG for match reconstruction)

---

## Extending the Pipeline

### Adding New Validation Rules

Edit: [quality/expectations.py](quality/expectations.py)

```python
def new_validation_rule(df):
    """Add custom check"""
    return df.isnull().sum().sum() == 0
```

### Adding New Analysis Steps

Edit: `main.py` in the `main()` function:

```python
# After STEP 4
logger.info("\n" + "="*80)
logger.info("STEP 5: YOUR NEW STEP")
logger.info("="*80)

from my_module import MyAnalyzer
analyzer = MyAnalyzer()
analyzer.run()
```

### Modifying Output Schema

Edit respective pipeline files:
- STEP 2: [analytics/historical_standings_calculator.py](analytics/historical_standings_calculator.py)
- STEP 3: [standings/standings_calculator.py](standings/standings_calculator.py)
- STEP 4: [analytics/form/form_analysis.py](analytics/form/form_analysis.py)

---

## Troubleshooting

### "ModuleNotFoundError"

**Cause**: Missing Python packages  
**Fix**: Install dependencies
```bash
pip install -r requirements.txt
```

### "FileNotFoundError: data/schedule.csv"

**Cause**: Input data missing  
**Fix**: Ensure CSV files exist in `data/` directory

### Validation Fails & Remediation Can't Fix

**Cause**: Severe data corruption  
**Fix**: Check logs, manually inspect `data/schedule.csv` and `data/scores.csv`

### Historical CSV Empty

**Cause**: STEP 2 didn't run or failed silently  
**Fix**: Check `logs/pipeline/pipeline_*.log` for errors

### Form CSV Empty

**Cause**: standings_historical.csv missing when STEP 4 runs  
**Fix**: Ensure STEP 2 completes successfully before STEP 4

---

## Next Steps

After running this pipeline:

1. **Review Deliverables**
   - Check `outputs/standings_final.csv` for champion
   - Analyze `outputs/recent_form.csv` for trends
   - Read validation logs for data quality

2. **Use Outputs for Reports**
   - Season review document
   - Form analysis for upcoming matches
   - Statistical analysis and rankings

3. **Schedule Regular Runs**
   - Run weekly as new matches complete
   - Update `standings_historical.csv`
   - Track form changes week-to-week

4. **Extend Analysis**
   - Add injury impact analysis
   - Home/away form tracking
   - Head-to-head records

---

## Related Documentation

- [README_NEW.md](analytics/form/README_NEW.md) - Form analysis algorithm deep dive
- [DOCUMENTATION_UPDATES.md](DOCUMENTATION_UPDATES.md) - Code documentation summary
- [ingestion/](ingestion/) - Data ingestion and validation
- [quality/](quality/) - Data quality expectations
- [analytics/](analytics/) - Analysis modules

---

## Support

For questions about:
- **Pipeline Structure**: Review "4-Step Pipeline Architecture" section
- **Output Schemas**: Check "Deliverables Summary" section
- **Validation Logic**: See [quality/expectations.py](quality/expectations.py)
- **SQL Queries**: Read [analytics/form/README_NEW.md](analytics/form/README_NEW.md)

---

## License

Part of Data Engineer Assessment Project