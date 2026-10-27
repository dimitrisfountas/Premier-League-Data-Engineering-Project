# Historical Standings Simulation

## Task
Calculate **weekly league standings** by processing matches incrementally in batches, maintaining accumulated state across 39 Monday checkpoints. Produces a complete history of how rankings evolved throughout the season.

## How It Works

### Purpose
This module simulates how standings would be updated each Monday during the season, processing new games/scores that arrive between consecutive weeks while maintaining running totals.

### Batch Processing Approach

The pipeline uses **stateful batch processing**:
- Starts with first Monday (Aug 25, 2025)
- For each subsequent Monday:
  1. **Extract NEW BATCH**: Games/scores between previous_monday and current_monday
  2. **Append to accumulated state**: Add batch to running totals
  3. **Calculate standings**: On TOTAL accumulated data (not just batch)
  4. **Store snapshot**: Save weekly standings
  5. **Advance**: Move to next Monday

**Key Insight**: Standing at Monday N is based on ALL games through Monday N, not just games from that week.

### Workflow

```
Validated schedule.csv + scores.csv
            ↓
For each Monday (Aug 25 → May 18):
  ├─ Extract NEW games up to Monday
  ├─ Extract NEW scores up to Monday
  ├─ Append to accumulated state
  ├─ Calculate standings on TOTAL accumulated
  └─ Store weekly snapshot
            ↓
Concatenate all 39 snapshots
            ↓
standings_historical.csv (780 rows: 20 teams × 39 Mondays)
```

## Components

### `historical_standings_calculator.py`
**HistoricalStandingsCalculator Class**

Performs incremental standings calculation with state management:

**State Variables**:
- `accumulated_schedule`: All games processed so far (grows each Monday)
- `accumulated_scores`: All scores processed so far (grows each Monday)
- `valid_teams`: Set of valid team names

**Key Method: `process_batch_at_date()`**

```python
standings = calculator.process_batch_at_date(
    schedule_df=full_schedule,
    scores_df=full_scores,
    cutoff_date=pd.Timestamp('2025-09-01 08:00:00')  # Monday cutoff
)
```

**Process**:
1. **Filter schedule**: Get games with `game_start_time <= cutoff_date`
2. **Extract NEW batch**: Compare to accumulated_schedule, find new game_ids
3. **Append to accumulated_schedule**: Grow the running total
4. **Filter scores**: Get scores with `ingestion_time <= cutoff_date`
5. **Extract NEW batch**: Compare to accumulated_scores, find new game_ids
6. **Append to accumulated_scores**: Grow the running total
7. **Calculate standings**: On total accumulated data (same as final standings calculator)
8. **Return standings snapshot**: DataFrame with 20 rows for this Monday

**Efficiency**:
- Only processes NEW games/scores each iteration
- Reuses accumulated data (no re-computation)
- Stateful approach matches production data arrival patterns

**Example Evolution**:
```
Monday 1 (Aug 25): 9 games processed → Calculate on 9 total
Monday 2 (Sep 01): 9 NEW games added → Calculate on 18 total
Monday 3 (Sep 08): 11 NEW games added → Calculate on 29 total
...
Monday 39 (May 18): 9 NEW games added → Calculate on 380 total
```

**Output Columns** (per weekly snapshot):
- `ranking`: Position 1-20 at that Monday
- `team`: Team name
- `wins`: Cumulative wins through that Monday
- `draws`: Cumulative draws through that Monday
- `losses`: Cumulative losses through that Monday
- `goals_for`: Cumulative goals scored through that Monday
- `goals_against`: Cumulative goals conceded through that Monday
- `points`: Cumulative league points through that Monday
- `updated_at`: Monday timestamp (ISO format)

### `historical_standings_pipeline.py`
**HistoricalStandingsPipeline Class**

Orchestrates the week-by-week standings simulation:

1. **Load & Validate Data** (one-time)
   - Loads schedule.csv through validation pipeline
   - Loads scores.csv through validation pipeline
   - Logging: Full DataFrame shapes

2. **Initialize Calculator**
   - Creates `HistoricalStandingsCalculator` with empty accumulated state
   - Valid teams set from constants

3. **Iterate Through Mondays**
   - Starts at Aug 25, 2025
   - Increments by 7 days each iteration
   - Calls `calculator.process_batch_at_date(cutoff_date)`
   - Collects standings snapshot
   - Stops when batch returns empty (no new data)

4. **Concatenate Results**
   - Combines all 39 weekly snapshots into single DataFrame
   - 780 rows total (20 teams × 39 weeks)

5. **Save Results**
   - Saves to `outputs/standings_historical.csv`
   - Saves execution log to `logs/standings/standings_historical.log`

**Key Method: `run()`**
```python
HistoricalStandingsPipeline().run(schedule_df, scores_df)
# Processes all Mondays and produces standings_historical.csv
```

## Output Schema

**standings_historical.csv**
```
ranking,team,wins,draws,losses,goals_for,goals_against,points,updated_at
1,Liverpool,1,0,0,5,1,3,2025-08-25T08:00:00Z
2,Manchester City,1,0,0,4,2,3,2025-08-25T08:00:00Z
...
20,Leicester City,0,0,1,0,5,0,2025-08-25T08:00:00Z
1,Manchester City,2,0,0,9,1,6,2025-09-01T08:00:00Z
2,Liverpool,2,0,0,10,2,6,2025-09-01T08:00:00Z
...
```

- **780 rows** (20 teams × 39 Mondays)
- **Sorted by updated_at first, then ranking**
- **Cumulative statistics** through each Monday

## Key Differences from Final Standings

| Aspect | Historical Standings | Final Standings |
|--------|----------------------|-----------------|
| **Snapshots** | 39 (weekly checkpoints) | 1 (complete season) |
| **Processing** | Incremental/batch | Single pass |
| **Purpose** | Track ranking evolution | Final season rankings |
| **Rows** | 780 (20 teams × 39 weeks) | 20 (one per team) |
| **Use Case** | Analyze form changes, upsets, trends | Report final league table |

## Monday Dates

The pipeline iterates through 39 Mondays during the 2025-26 season:
- **Week 1**: August 25, 2025 (season starts)
- **Week 2**: September 1, 2025
- **Week 3**: September 8, 2025
- ...
- **Week 39**: May 18, 2026 (final Monday)

**Total Games**: 380 (20 teams × 19 home matches + 19 away matches)

## Input Requirements

**schedule.csv (validated)**
- Columns: `game_id`, `round`, `home_team`, `away_team`, `game_start_time`
- All 380 games for the season

**scores.csv (validated)**
- Columns: `game_id`, `home_goals`, `away_goals`, `ingestion_time`
- Should have matching `game_id` for all scheduled games

## Execution

From main pipeline (STEP 2):
```python
from analytics.historical_standings.historical_standings_pipeline import HistoricalStandingsPipeline

pipeline = HistoricalStandingsPipeline()
pipeline.run(schedule_df, scores_df)
# Produces outputs/standings_historical.csv
```

Or standalone:
```python
pipeline = HistoricalStandingsPipeline()
pipeline.run()
# Loads and validates data from scratch
```

## Logging

**Log File**: `logs/standings/standings_historical.log`

Contains:
- Timestamp of execution
- Schedule and scores DataFrame shapes
- For each Monday:
  - New games in batch
  - New scores in batch
  - Batch processing details
  - Standings snapshot (20 rows)
- Total snapshots collected
- Execution status


## Related Modules

- **Ingestion**: `ingestion/csv_ingestor.py` - Reads raw CSV files
- **Validation**: `quality/` - Validates schedule and scores
- **Remediation**: `pipeline/pipeline.py` - Fixes data quality issues
- **Final Standings**: `analytics/standings/` - Complete season ranking
- **Form Analysis**: `analytics/form/` - Top 3 teams per week (uses this output)
