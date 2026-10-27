# Recent Form Analysis

## Overview

Identifies the **top 3 performing teams** each Monday based on their **last 5 matches**. Uses SQL window functions to reconstruct individual match results from cumulative standings data, calculates performance metrics, and ranks teams.

**Output**: `recent_form.csv` with 117 rows (3 teams × 39 Mondays)

---

## Problem Statement

Given only **cumulative weekly standings** (wins, draws, losses, goals cumulative totals), reconstruct:
1. The result of each individual match in the last 5-match window
2. Form string (e.g., "WDWWL")
3. Points, goals for/against in that window
4. Ranking by form metrics


---

## How It Works

### High-Level Algorithm

For **each Monday** in the season:

1. **Identify the 5-game window**
   - Current record: Team's cumulative stats as of this Monday
   - Reference record: Team's stats from when they had 5 fewer games
   - Window contains: Those 5 intermediate games

2. **Reconstruct match results**
   - Use cumulative delta to determine W/D/L for each game
   - Use LAG window function to compare consecutive game counts
   - Build result string: "WDWWL" (oldest to newest)

3. **Calculate metrics**
   - Points in last 5: (wins × 3) + draws
   - Goals for/against: Sum of deltas
   - Goal difference: Goals for - goals against

4. **Rank teams**
   - Primary: Points (descending)
   - Secondary: Goal difference (descending)
   - Tertiary: Goals against (ascending)
   - Quaternary: Team name (ascending tie-breaker)

5. **Extract top 3** for this Monday

6. **Repeat for all Mondays**, concatenate results

### Data Flow

```
standings_historical.csv
(20 teams × 39 weeks = 780 records)
         ↓
    Load into SQLite
         ↓
   For each Monday:
   ├─ Identify current and reference records
   ├─ Extract records in 5-game window
   ├─ Reconstruct individual match results
   ├─ Aggregate metrics
   ├─ Rank teams
   └─ Extract top 3
         ↓
  Concatenate all results
         ↓
recent_form.csv (117 records)
```

---

## SQL Query Breakdown

The analysis uses 4 CTEs (Common Table Expressions) in sequence:

### 1. `ranked_teams` CTE
**Purpose**: Find the reference point (5 games ago) for each team

- `last_game`: Total matches this team has played as of this Monday
- `start_game`: Total matches when they had played 5 fewer games (minimum 1 if <5 played)
- Used to slice the standings history to find only records in the 5-game window

### 2. `sliced_standings` CTE
**Purpose**: Get all historical records that fall in the 5-game window

- Joins current standings with reference records
- Filters to only records where cumulative game count is in range [start_game, last_game]
- Returns: 6 records per team (reference + 5 subsequent snapshots)

### 3. `per_game_stats` CTE
**Purpose**: Reconstruct individual match results using LAG and deltas

**Key Logic**:
- `LAG(...) OVER (PARTITION BY team ORDER BY total_games)` retrieves previous record for this team
- **Delta of 1 in wins column** → Win
- **Delta of 1 in draws column** → Draw
- **Delta of 1 in losses column** → Loss
- `game_result` becomes: 'W', 'D', 'L', or NULL (reference record has no previous)
- `gf`/`ga`: Goal deltas for this match
- `points`: Match points (3 for W, 1 for D, 0 for L)

**Example**: 
- Monday A: wins=2, Monday B: wins=3, delta=1 → One win occurred between these snapshots

### 4. `team_last_5` CTE
**Purpose**: Aggregate the 5 matches and rank teams

**Aggregation**:
- `SUM(points)`: Total points across 5 matches (0-15 range)
- `SUM(gf)` / `SUM(ga)`: Total goals scored/conceded
- `GROUP_CONCAT(game_result)`: String of match results ("WDWWL")

**Ranking**:
- `ROW_NUMBER() OVER (ORDER BY ...)` assigns rank 1, 2, 3
- Sort order: Points DESC → Goal Diff DESC → Goals Against ASC → Team ASC

**Final SELECT**: Returns top 3 teams for this Monday

---

## Implementation Details

### Class: `FormAnalyzer`

#### `__init__(input_path, output_path)`
- **input_path**: Path to `standings_historical.csv` (default: `outputs/standings_historical.csv`)
- **output_path**: Where to save results (default: `outputs/recent_form.csv`)

#### `load_data()`
1. Reads CSV into pandas DataFrame
2. Extracts unique dates (all 39 Mondays)
3. Creates SQLite in-memory database
4. Loads standings table
5. Prints schema for verification

**Database Schema**:
```
standings:
  - index (auto, not used)
  - ranking: Numerical position (1-20)
  - team: Team name
  - wins: Cumulative wins
  - draws: Cumulative draws
  - losses: Cumulative losses
  - goals_for: Cumulative GF
  - goals_against: Cumulative GA
  - points: Cumulative points (wins×3 + draws)
  - updated_at: Monday ISO timestamp
```

#### `run()`
1. Call `load_data()` to initialize database
2. For each date in `self.dates`:
   - Execute the 4-CTE SQL query
   - Append result DataFrame to `all_results` list
3. Concatenate all DataFrames
4. Reorder and rename columns
5. Save to CSV

---

## Output Schema

### `recent_form.csv`

| Column | Type | Description |
|--------|------|-------------|
| form_ranking | int | Rank 1, 2, or 3 for this Monday |
| team | str | Team name |
| goals_for | int | Goals scored in last 5 matches |
| goals_against | int | Goals conceded in last 5 matches |
| points | int | Points earned (0-15 range) |
| updated_at | str | Monday timestamp (ISO format) |

**Total Rows**: 117 (3 ranks × 39 Mondays)

---

## Form String Reference

The SQL includes `form` column showing sequence of match results:

| Result | Points | Notes |
|--------|--------|-------|
| W (Win) | 3 | Victory |
| D (Draw) | 1 | Tie |
| L (Loss) | 0 | Defeat |

**Example Interpretations**:
- `WWWWW` → 5 wins, 15 points (elite form)
- `WWWWD` → 4 wins + 1 draw, 13 points
- `WDWDW` → Mixed form, 11 points (inconsistent)
- `LLLLL` → 5 losses, 0 points (crisis)

---

## Usage

### Standalone Execution
```bash
python analytics/form/form_analysis.py
```

### From Python
```python
from analytics.form.form_analysis import FormAnalyzer

analyzer = FormAnalyzer()
analyzer.run()
# Creates outputs/recent_form.csv
```

### Custom Paths
```python
analyzer = FormAnalyzer(
    input_path="custom/standings.csv",
    output_path="custom/output.csv"
)
analyzer.run()
```

---

## Dependencies

- `sqlite3`: Built-in Python module
- `pandas`: For CSV I/O and DataFrame operations
- `logging`: Built-in Python module

---

## Related Components

- **Input**: `analytics/historical_standings/` - Generates `standings_historical.csv`
- **Pipeline**: `main.py` - Orchestrates all analysis steps
- **Validation**: `quality/` - Data quality checks
- **Ingestion**: `ingestion/` - Raw data loading