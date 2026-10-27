"""
╔══════════════════════════════════════════════════════════════════════════════╗
║              HISTORICAL STANDINGS CALCULATOR - BATCH PROCESSING              ║
║                        Incremental/Windowed Approach                         ║
╚══════════════════════════════════════════════════════════════════════════════╝

PURPOSE:
Calculate league standings incrementally by processing batches of games/scores
for each Monday, maintaining accumulated state across iterations.

APPROACH:
Pipeline handles batch extraction between consecutive Mondays:
  For each Monday:
        1. Pipeline extracts NEW games between previous_monday and current_monday
        2. Pipeline extracts NEW scores between previous_monday and current_monday
        3. Pipeline appends batch to calculator.accumulated_schedule/accumulated_scores
        4. Pipeline calls calculator._calculate_standings_from_accumulated()
        5. Calculator returns standings on TOTAL accumulated data (not just batch)

STATE MANAGEMENT:
The calculator maintains state between Monday calls:
  - accumulated_schedule: All games processed so far
  - accumulated_scores: All scores processed so far
  - This state is NOT reset between Monday calls
  - Pipeline appends NEW batch to existing state each iteration
  - Calculator uses TOTAL accumulated data for standings calculation

EXAMPLE WORKFLOW:
  Monday 1 (Aug 25): Pipeline adds 9 games → Calculate standings on 9 total
  Monday 2 (Sep 01): Pipeline adds 9 NEW games → Calculate standings on 18 total
  Monday 3 (Sep 08): Pipeline adds 11 NEW games → Calculate standings on 29 total
  ...
  Monday 39 (May 18): Pipeline adds 9 NEW games → Calculate standings on 380 total
"""

import pandas as pd
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class HistoricalStandingsCalculator:
    """
    Calculates league standings incrementally on accumulated data.
    
    Pipeline handles batch extraction (between consecutive Mondays).
    Calculator receives batch from pipeline, appends to accumulated state,
    and calculates standings on TOTAL accumulated data for the Monday snapshot.
    """
    
    def __init__(self, valid_teams: set):
        """
        Initialize calculator with state tracking for incremental processing.
        
        PARAMETERS:
        - valid_teams: Set of valid team names to validate teams in data
        
        STATE VARIABLES (initialized empty, grow with each Monday):
        - accumulated_schedule: All unique games processed so far (concatenated batches)
        - accumulated_scores: All unique scores processed so far (concatenated batches)
        
        STATE PERSISTENCE:
        - These DataFrames persist across multiple calls to process_batch_at_date()
        - Each Monday call appends NEW records only (duplicates avoided by game_id)
        - Standings calculated on TOTAL accumulated data, not just new batch
        - This creates the illusion of weekly updates while maintaining full history
        """
        self.valid_teams = valid_teams
        
        # Accumulated state (grows with each Monday)
        self.accumulated_schedule = pd.DataFrame()
        self.accumulated_scores = pd.DataFrame()
        
        logger.info("Initialized HistoricalStandingsCalculator with batch processing mode")
    
    def process_batch_at_date(
        self,
        schedule_df: pd.DataFrame,
        scores_df: pd.DataFrame,
        cutoff_date: pd.Timestamp
    ) -> pd.DataFrame:
        """
        Process NEW BATCH of games/scores up to cutoff date and calculate standings.
        
        ALGORITHM:
        1. Filter both DataFrames to cutoff date (game_start_time/ingestion_time <= cutoff)
        2. Identify NEW records by comparing game_ids to what's already accumulated
        3. Append NEW records to accumulated_schedule and accumulated_scores
        4. Merge accumulated data on game_id (LEFT join to keep unscored games)
        5. Calculate standings from TOTAL accumulated games (not just batch)
        6. Return standings snapshot with ranking, team stats, and timestamp
        
        PARAMETERS:
        - schedule_df: Full schedule DataFrame (contains all games, including future ones)
        - scores_df: Full scores DataFrame (contains all scores, including future ones)
        - cutoff_date: Monday cutoff time (e.g., 2025-09-01 08:00:00 UTC)
                      Only games/scores with timestamp <= cutoff_date are used
        
        RETURN:
        - pd.DataFrame: Standings snapshot for this Monday
                       Columns: ranking (1-20), team, wins, draws, losses,
                               goals_for, goals_against, points, updated_at (ISO timestamp)
        
        DUPLICATE HANDLING:
        - Compares game_ids to accumulated_schedule/accumulated_scores
        - Only adds game_ids that don't already exist (avoids double-counting)
        - If pipeline sends same game_id twice, it's ignored second time
        
        STATE MODIFICATION:
        - This method MODIFIES the calculator's state:
          * accumulated_schedule grows by new games
          * accumulated_scores grows by new scores
        - State persists across calls, enabling incremental processing
        
        EXAMPLE:
        --------
        # Monday 1: Aug 25, 2025 (9 games played)
        standings_1 = calculator.process_batch_at_date(full_schedule, full_scores,
                                                       pd.Timestamp('2025-08-25 08:00:00'))
        # Returns: 20 teams with stats for 9 games
        # Internal: accumulated_schedule now has 9 games
        
        # Monday 2: Sep 01, 2025 (18 games total - 9 new)
        standings_2 = calculator.process_batch_at_date(full_schedule, full_scores,
                                                       pd.Timestamp('2025-09-01 08:00:00'))
        # Returns: 20 teams with stats for 18 games
        # Internal: accumulated_schedule now has 18 games (9 old + 9 new)
        
        # Monday 3: Sep 08, 2025 (29 games total - 11 new)
        standings_3 = calculator.process_batch_at_date(full_schedule, full_scores,
                                                       pd.Timestamp('2025-09-08 08:00:00'))
        # Returns: 20 teams with stats for 29 games
        # Internal: accumulated_schedule now has 29 games (18 old + 11 new)
        """
        # ────────────────────────────────────────────────────────────────────────────
        # STEP 1: Filter new games up to cutoff date
        # ────────────────────────────────────────────────────────────────────────────
        schedule_to_cutoff = schedule_df[
            pd.to_datetime(schedule_df["game_start_time"]) <= cutoff_date
        ].copy()
        
        # Determine which rows are NEW (haven't been processed yet)
        if len(self.accumulated_schedule) == 0:
            # First iteration: all rows up to cutoff are new
            new_schedule = schedule_to_cutoff
            logger.info(f"First iteration - All {len(new_schedule)} games are new")
        else:
            # Subsequent iterations: find rows we haven't seen before
            # Compare by game_id to avoid duplicates
            processed_game_ids = set(self.accumulated_schedule["game_id"].unique())
            new_schedule = schedule_to_cutoff[
                ~schedule_to_cutoff["game_id"].isin(processed_game_ids)
            ]
            logger.info(f"New games in batch: {len(new_schedule)} out of {len(schedule_to_cutoff)} total")
        
        # ────────────────────────────────────────────────────────────────────────────
        # STEP 2: Filter new scores up to cutoff date
        # ────────────────────────────────────────────────────────────────────────────
        scores_to_cutoff = scores_df[
            pd.to_datetime(scores_df["ingestion_time"]) <= cutoff_date
        ].copy()
        
        # Determine which rows are NEW
        if len(self.accumulated_scores) == 0:
            # First iteration: all rows up to cutoff are new
            new_scores = scores_to_cutoff
            logger.info(f"First iteration - All {len(new_scores)} scores are new")
        else:
            # Subsequent iterations: find rows we haven't seen before
            processed_score_ids = set(self.accumulated_scores["game_id"].unique())
            new_scores = scores_to_cutoff[
                ~scores_to_cutoff["game_id"].isin(processed_score_ids)
            ]
            logger.info(f"New scores in batch: {len(new_scores)} out of {len(scores_to_cutoff)} total")
        
        # ────────────────────────────────────────────────────────────────────────────
        # STEP 3: Append NEW BATCH to accumulated state
        # ────────────────────────────────────────────────────────────────────────────
        if len(new_schedule) > 0:
            self.accumulated_schedule = pd.concat(
                [self.accumulated_schedule, new_schedule],
                ignore_index=True
            )
            logger.info(f"Accumulated schedule size: {len(self.accumulated_schedule)} games")
        
        if len(new_scores) > 0:
            self.accumulated_scores = pd.concat(
                [self.accumulated_scores, new_scores],
                ignore_index=True
            )
            logger.info(f"Accumulated scores size: {len(self.accumulated_scores)} scores")
        
        # ────────────────────────────────────────────────────────────────────────────
        # STEP 4: Calculate standings on ACCUMULATED state
        # ────────────────────────────────────────────────────────────────────────────
        standings = self._calculate_standings_from_accumulated(cutoff_date)
        
        return standings
    
    def _calculate_standings_from_accumulated(self, snapshot_date: pd.Timestamp) -> pd.DataFrame:
        """
        Calculate standings snapshot from accumulated schedule and scores.
        
        PROCESS:
        1. Merge accumulated_schedule with accumulated_scores on game_id (LEFT join)
           - LEFT join keeps games without scores (counts as 0 points for both teams)
        2. Initialize stats dict for all 20 valid teams (0 wins/draws/losses)
        3. Iterate through each merged row:
           a. Extract home_goals, away_goals (skip if missing)
           b. Validate teams are in valid_teams set
           c. Update goals_for and goals_against for both teams
           d. Determine match result and award points:
              - Win: 3 points
              - Draw: 1 point each
              - Loss: 0 points
           e. Update wins/draws/losses counters
        4. Convert stats dict to DataFrame (1 row per team)
        5. Sort by ranking criteria (in priority order):
           - Points (descending)
           - Goals For (descending)
           - Goals Against (ascending)
           - Team Name (alphabetically ascending - tie-breaker)
        6. Add ranking column (1-20)
        7. Add updated_at timestamp to each row
        8. Return standings DataFrame
        
        PARAMETERS:
        - snapshot_date: Date for the snapshot (formatted as ISO timestamp in output)
                        Used for the updated_at column
        
        RETURN:
        - pd.DataFrame: Standings snapshot
                       Columns: ranking, team, wins, draws, losses, goals_for,
                               goals_against, points, updated_at
                       20 rows (one per valid team)
                       Sorted by ranking 1-20
        
        EMPTY DATA HANDLING:
        - If accumulated_schedule is empty, logs warning and returns empty DataFrame
        - This can happen on very first call if no games have been ingested yet
        
        UNSCORED MATCHES:
        - Games without matching scores are included in merged data (LEFT join)
        - When processing, missing home_goals/away_goals cause ValueError
        - These matches are skipped with continue (no stats updated)
        - This means unscored games don't affect standings (correct behavior)
        
        INVALID TEAMS:
        - If game references team not in valid_teams, match is skipped
        - This prevents corrupted/test data from affecting standings
        
        EXAMPLE:
        --------
        # After Monday 1: accumulated_schedule has 9 games
        standings = calculator._calculate_standings_from_accumulated(
            pd.Timestamp('2025-08-25 08:00:00')
        )
        # Returns DataFrame with 20 teams, stats for 9 games total
        
        # After Monday 2: accumulated_schedule has 18 games (9 old + 9 new)
        standings = calculator._calculate_standings_from_accumulated(
            pd.Timestamp('2025-09-01 08:00:00')
        )
        # Returns DataFrame with 20 teams, stats for 18 games total
        # Previous 9 games are still included in the calculation
        """
        # Merge accumulated data
        if len(self.accumulated_schedule) == 0:
            logger.warning(f"No accumulated schedule data for {snapshot_date}")
            return pd.DataFrame()
        
        merged_df = self.accumulated_schedule.merge(
            self.accumulated_scores,
            on="game_id",
            how="left"
        )
        
        logger.info(f"Merged games: {len(merged_df)} matches with scores")
        
        # Initialize team stats dictionary
        team_stats = {}
        for team in self.valid_teams:
            team_stats[team] = {
                "played": 0,
                "won": 0,
                "drawn": 0,
                "lost": 0,
                "goals_for": 0,
                "goals_against": 0,
                "points": 0
            }
        
        # Process each match
        for _, row in merged_df.iterrows():
            try:
                home_goals = int(row["home_goals"])
                away_goals = int(row["away_goals"])
            except (ValueError, TypeError):
                # Skip if goals are missing or invalid
                continue
            
            if "home_team" not in row or "away_team" not in row:
                continue
            
            home_team = row["home_team"]
            away_team = row["away_team"]
            
            # Skip if teams not in valid set
            if home_team not in self.valid_teams or away_team not in self.valid_teams:
                continue
            
            # Update match count for both teams
            team_stats[home_team]["played"] += 1
            team_stats[away_team]["played"] += 1
            
            # Update goals
            team_stats[home_team]["goals_for"] += home_goals
            team_stats[home_team]["goals_against"] += away_goals
            team_stats[away_team]["goals_for"] += away_goals
            team_stats[away_team]["goals_against"] += home_goals
            
            # Calculate result and points
            if home_goals > away_goals:
                # Home team wins
                team_stats[home_team]["won"] += 1
                team_stats[home_team]["points"] += 3
                team_stats[away_team]["lost"] += 1
            elif away_goals > home_goals:
                # Away team wins
                team_stats[away_team]["won"] += 1
                team_stats[away_team]["points"] += 3
                team_stats[home_team]["lost"] += 1
            else:
                # Draw
                team_stats[home_team]["drawn"] += 1
                team_stats[home_team]["points"] += 1
                team_stats[away_team]["drawn"] += 1
                team_stats[away_team]["points"] += 1
        
        # Convert to DataFrame
        standings_data = []
        for team, stats in team_stats.items():
            standings_data.append({
                "team": team,
                "wins": stats["won"],
                "draws": stats["drawn"],
                "losses": stats["lost"],
                "goals_for": stats["goals_for"],
                "goals_against": stats["goals_against"],
                "points": stats["points"],
                "updated_at": snapshot_date.strftime("%Y-%m-%dT%H:%M:%SZ")
            })
        
        standings_df = pd.DataFrame(standings_data)
        standings_df = standings_df.fillna(0)
        
        # Convert to integers
        int_columns = ["wins", "draws", "losses", "goals_for", "goals_against", "points"]
        for col in int_columns:
            standings_df[col] = standings_df[col].astype(int)
        
        # Sort by ranking criteria
        standings_df = standings_df.sort_values(
            by=["points", "goals_for", "goals_against", "team"],
            ascending=[False, False, False, True]
        ).reset_index(drop=True)
        
        # Add ranking column
        standings_df.insert(0, "ranking", range(1, len(standings_df) + 1))
        
        return standings_df
