# Final Standings Calculation

## Task
Calculate the **final league standings** from complete validated schedule and scores data. Produces a single standings snapshot representing the complete Premier League season (38 rounds).

## How It Works

### Workflow

```
Validated schedule.csv + scores.csv
            ↓
      Merge schedule ← scores (LEFT join)
            ↓
      Filter to games with scores
            ↓
   StandingsCalculator
   - Initialize stats for all 20 teams
   - Process each match: update wins/draws/losses, goals, points
   - Sort teams by ranking criteria
            ↓
   Final standings.csv (20 rows, 1 per team)
            ↓
   Save to outputs/standings_final.csv
```

## Components

### `standings_calculator.py`
**StandingsCalculator Class**

Performs the actual standings computation:
- **Initialization**: Sets up empty stats dict for all valid teams
- **Processing**: Iterates through each completed match
  - Updates goals_for and goals_against
  - Calculates match result (Win/Draw/Loss)
  - Awards points (3 for win, 1 for draw, 0 for loss)
- **Ranking**: Sorts teams by comprehensive criteria

**Ranking Criteria (Priority Order):**
1. **Total Points** (descending) - Primary sorting criterion
2. **Goal Difference** (goals_for - goals_against) (descending)
3. **Goals Scored** (goals_for) (descending)
4. **Team Name** (alphabetically ascending) - Final tie-breaker

**Output Columns:**
- `ranking`: Position 1-20
- `team`: Team name
- `wins`: Total matches won
- `draws`: Total matches drawn
- `losses`: Total matches lost
- `goals_for`: Total goals scored across all 38 matches
- `goals_against`: Total goals conceded across all 38 matches
- `points`: Total league points earned

### `standings_pipeline.py`
**StandingsPipeline Class**

Orchestrates the entire final standings calculation:

1. **Load & Validate Data**
   - Loads schedule.csv through validation pipeline (quality checks + remediation)
   - Loads scores.csv through validation pipeline
   - Logging: Number of records loaded

2. **Merge Data**
   - Uses LEFT join to keep all scheduled matches
   - Merges on `game_id`

3. **Filter Complete Games**
   - Removes games where scores are missing (home_goals, away_goals are null)
   - Logging: Number of games with complete scores

4. **Calculate Standings**
   - Calls `StandingsCalculator.calculate_standings()`
   - Returns DataFrame with 20 rows (one per team)
   - Logging: Full standings table

5. **Save Results**
   - Saves to `outputs/standings_final.csv`
   - Saves execution log to `logs/standings/standings_final.log`

**Key Method: `run()`**
```python
standings_df = StandingsPipeline().run()
# Returns DataFrame with final league standings
```

## Key Differences from Historical Standings

| Aspect | Final Standings | Historical Standings |
|--------|-----------------|----------------------|
| **Snapshots** | 1 (complete season) | 39 (weekly checkpoints) |
| **Calculation** | Single pass on all data | Incremental/batch processing |
| **Purpose** | Final season rankings | Track ranking evolution |
| **Data** | All 380 matches | Cumulative matches up to each Monday |

## Input Requirements

**schedule.csv (validated)**
- Columns: `game_id`, `round`, `home_team`, `away_team`, `game_start_time`
- All rows with games up to final round (38)

**scores.csv (validated)**
- Columns: `game_id`, `home_goals`, `away_goals`
- Should have matching `game_id` for all scheduled games

## Output Schema

**standings_final.csv**
```
ranking,team,wins,draws,losses,goals_for,goals_against,points
1,Manchester City,28,5,5,93,43,89
2,Arsenal,28,5,5,88,44,89
...
20,Leicester City,5,4,29,40,96,19
```

- **20 rows** (one per team)
- **Sorted by ranking** (1-20)
- **Final season snapshot** (all 380 matches processed)

## Execution

From main pipeline (STEP 3):
```python
from analytics.standings.standings_pipeline import StandingsPipeline

standings_pipeline = StandingsPipeline(schedule_df, scores_df)
standings_df = standings_pipeline.run()
```

Or standalone:
```python
standings_pipeline = StandingsPipeline()
standings_df = standings_pipeline.run()
# Loads and validates data from scratch
```

## Logging

**Log File**: `logs/standings/standings_final.log`

Contains:
- Timestamp of execution
- Number of records loaded from schedule and scores
- Number of teams in final standings
- Full standings table output
- Execution status (success/failure)

## Related Modules

- **Ingestion**: `ingestion/csv_ingestor.py` - Reads raw CSV files
- **Validation**: `quality/ge_scores_expectations.py` - Validates scores
- **Remediation**: `pipeline/pipeline.py` - Fixes data quality issues
- **Historical Standings**: `analytics/historical_standings/` - Weekly snapshots
- **Form Analysis**: `analytics/form/` - Top 3 teams per week
