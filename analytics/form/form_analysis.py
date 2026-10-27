"""
Recent Form Analysis Module

Identifies the top 3 performing teams each Monday based on their last 5 matches.

APPROACH:
- Uses SQL window functions to reconstruct individual match results from cumulative standings data
- Cannot use match-level records (not available) so we reverse-engineer results from deltas
- For each team at each Monday: find 5 games ago snapshot, calculate deltas, derive W/D/L

ALGORITHM:
1. Load standings_historical.csv (780 rows: 20 teams × 39 Mondays)
2. For each Monday:
   a) Find "5 games ago" reference point for each team
   b) Extract records between reference and current
   c) Use LAG() to reconstruct match results from cumulative deltas
   d) Aggregate metrics: points, goals for/against
   e) Rank teams by points DESC, goal_diff DESC, goals_against ASC, team ASC
   f) Extract top 3
3. Concatenate results across all Mondays
4. Save to CSV

OUTPUT:
- recent_form.csv: 117 rows (3 teams × 39 Mondays)
- Columns: form_ranking, team, goals_for, goals_against, points, updated_at
"""

import sqlite3
import pandas as pd
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class FormAnalyzer:
    """
    SQL-based form analysis for top 3 teams per Monday.
    
    Uses window functions and cumulative standing deltas to reconstruct individual
    match results from weekly snapshots. Ranks teams by recent 5-match performance.
    """
    
    def __init__(self, input_path="outputs/standings_historical.csv", 
                 output_path="outputs/recent_form.csv"):
        """
        Initialize FormAnalyzer.
        
        Args:
            input_path: Path to standings_historical.csv
            output_path: Path where to save recent_form.csv
        """
        self.input_path = input_path
        self.output_path = output_path
        self.conn = None
        self.dates = []  # List of unique Mondays (will be sorted)

    def load_data(self):
        """
        Load standings into SQLite database.
        
        Process:
        1. Read standings_historical.csv (20 teams × 39 weeks = 780 rows)
        2. Extract unique dates (39 Mondays, sorted chronologically)
        3. Create in-memory SQLite database
        4. Load standings table with all columns
        5. Print schema for verification
        
        Database Schema:
        - ranking: Team position (1-20)
        - team: Team name
        - wins/draws/losses: Cumulative stats
        - goals_for/goals_against: Cumulative
        - points: Cumulative (wins×3 + draws)
        - updated_at: Monday ISO timestamp
        
        Note: total_games will be computed later via: wins + draws + losses
        """
        logger.info("Loading data into SQLite...")
        
        # Read CSV and extract unique Mondays
        standings_df = pd.read_csv(self.input_path)
        self.dates = sorted(standings_df['updated_at'].unique())
        
        # Create in-memory SQLite database
        self.conn = sqlite3.connect(':memory:')
        standings_df.to_sql('standings', self.conn, if_exists='replace', index=False)
        
        logger.info(f"Loaded {len(standings_df)} standings records")
        
        # Print table schema for verification
        cursor = self.conn.cursor()
        
        print("\n=== STANDINGS TABLE ===")
        cursor.execute("PRAGMA table_info(standings)")
        for row in cursor.fetchall():
            print(f"  {row[1]}: {row[2]}")
        
        print("\nTable ready for SQL queries.")
        return self.conn

    def run(self):
        """
        Load data and export results to CSV.
        
        Main orchestration:
        1. Load standings into SQLite
        2. For each Monday:
           - Execute 4-CTE SQL query to identify top 3 teams by recent form
           - Collect results
        3. Concatenate all results
        4. Save to CSV
        """
        self.load_data()
        
        all_results = []
        for date in self.dates:
            logger.info(f"Processing date {date}")
            
            # ===== SQL QUERY EXPLANATION =====
            # The query uses 4 CTEs (Common Table Expressions) to:
            # 1. Find reference points (5 games ago)
            # 2. Extract records in 5-game window
            # 3. Reconstruct individual match results
            # 4. Aggregate and rank teams
            
            query = f"""
                WITH ranked_teams AS (
                    -- CTE 1: Find the 5-game reference point
                    -- For each team as of this Monday, calculate:
                    -- - last_game: Total games played (wins + draws + losses)
                    -- - start_game: Total games when they had 5 fewer games
                    -- This defines the 5-game window we'll analyze
                    SELECT 
                        team,
                        wins + draws + losses AS last_game,
                        CASE 
                            WHEN wins + draws + losses - 5 < 1 THEN 1
                            ELSE wins + draws + losses - 5
                        END AS start_game
                    FROM standings
                    WHERE updated_at = '{date}'
                ),
                sliced_standings AS (
                    -- CTE 2: Get all historical records in the 5-game window
                    -- Join current standings with reference points
                    -- Filter to only records where total_games is in [start_game, last_game]
                    -- Result: ~6 records per team (reference + 5 subsequent snapshots)
                    SELECT
                        s.*,
                        s.wins + s.draws + s.losses AS total_games
                    FROM standings s
                    JOIN ranked_teams r
                        ON s.team = r.team
                    WHERE s.wins + s.draws + s.losses BETWEEN r.start_game AND r.last_game
                ),
                per_game_stats AS (
                    -- CTE 3: Reconstruct individual match results from cumulative deltas
                    -- Uses LAG() window function to compare consecutive records
                    -- Logic: If wins increased by 1 → Win; draws +1 → Draw; losses +1 → Loss
                    -- Example: Monday A (wins=2) → Monday B (wins=3) → Delta=1 Win
                    SELECT
                        team,
                        total_games,
                        CASE
                            -- If wins increased by 1 compared to previous record → Win
                            WHEN wins   - LAG(wins,   1, 0) OVER (PARTITION BY team ORDER BY total_games) = 1 THEN 'W'
                            -- If draws increased by 1 compared to previous record → Draw
                            WHEN draws  - LAG(draws,  1, 0) OVER (PARTITION BY team ORDER BY total_games) = 1 THEN 'D'
                            -- If losses increased by 1 compared to previous record → Loss
                            WHEN losses - LAG(losses, 1, 0) OVER (PARTITION BY team ORDER BY total_games) = 1 THEN 'L'
                        END AS game_result,
                        -- Goals scored/conceded in this individual match
                        goals_for     - LAG(goals_for,     1, 0) OVER (PARTITION BY team ORDER BY total_games) AS gf,
                        goals_against - LAG(goals_against, 1, 0) OVER (PARTITION BY team ORDER BY total_games) AS ga,
                        -- Points for this match: Win=3, Draw=1, Loss=0
                        (wins   - LAG(wins,   1, 0) OVER (PARTITION BY team ORDER BY total_games)) * 3 +
                        (draws  - LAG(draws,  1, 0) OVER (PARTITION BY team ORDER BY total_games))     AS points,
                        updated_at
                    FROM sliced_standings
                ),
                team_last_5 AS (
                    -- CTE 4: Aggregate the 5 matches and rank teams
                    -- GROUP BY team: Combines all 5 matches into single metrics
                    -- ROW_NUMBER: Assigns rank 1, 2, 3... based on form
                    SELECT
                        ROW_NUMBER() OVER (
                            ORDER BY
                                -- Rank by: Points DESC (most important)
                                SUM(points) DESC,
                                -- Then: Goal difference DESC
                                (SUM(gf) - SUM(ga)) DESC,
                                -- Then: Goals against ASC (defensive quality)
                                SUM(ga) ASC,
                                -- Finally: Team name ASC (alphabetical tie-breaker)
                                team ASC
                        ) AS form_ranking,
                        team,
                        -- Total goals scored in last 5 matches
                        SUM(gf) AS goals_for,
                        -- Total goals conceded in last 5 matches
                        SUM(ga) AS goals_against,
                        -- String of match results: "WDWWL" (oldest to newest)
                        GROUP_CONCAT(game_result, '' ORDER BY total_games) AS form,
                        -- Total points in last 5: 0-15 range
                        SUM(points) AS points,
                        -- Use most recent Monday date
                        MAX(updated_at) AS updated_at
                    FROM per_game_stats
                    GROUP BY team
                )
                -- Extract only top 3 teams for this Monday
                SELECT *
                FROM team_last_5
                LIMIT 3;
            """
            result = pd.read_sql_query(query, self.conn)
            all_results.append(result)
        
        # Concatenate results from all Mondays into single DataFrame
        output_df = pd.concat(all_results, ignore_index=True)
        
        # Reorder columns for final output
        output_df = output_df[['form_ranking', 'team', 'goals_for', 'goals_against', 'points', 'updated_at']]
        
        # Rename columns (remove "_last_5" suffix - already implied by context)
        output_df.columns = ['form_ranking', 'team', 'goals_for', 'goals_against', 'points', 'updated_at']
        
        # Save to CSV: 117 rows (3 teams × 39 Mondays)
        output_df.to_csv(self.output_path, index=False)
        logger.info(f"Saved {len(output_df)} results to {self.output_path}")


if __name__ == "__main__":
    # Configure logging to show what's happening
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Run the form analysis: loads standings_historical.csv and exports recent_form.csv
    analyzer = FormAnalyzer()
    analyzer.run()

