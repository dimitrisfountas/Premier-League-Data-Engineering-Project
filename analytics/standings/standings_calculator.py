"""
╔══════════════════════════════════════════════════════════════════════════════╗
║             STANDINGS CALCULATOR - FINAL STANDINGS COMPUTATION               ║
║                  Calculates ranks based on match results                     ║
╚══════════════════════════════════════════════════════════════════════════════╝

PURPOSE:
Compute final league standings by aggregating match statistics for all teams
and ranking them according to league rules.

CALCULATION APPROACH:
1. Initialize stats for all valid teams (20 teams)
2. Iterate through each completed match:
   - Extract: home_team, away_team, home_goals, away_goals
   - Update goals: home_team.goals_for += home_goals, goals_against += away_goals
   - Calculate result: Win (3 pts) / Draw (1 pt each) / Loss (0 pts)
   - Update wins/draws/losses counters
3. Convert team stats dict to DataFrame
4. Sort by ranking criteria (see StandingsCalculator docstring)
5. Add ranking column (1-20)

RANKING PRIORITY (applied in order):
1. Total points (descending) - primary sorting criterion
2. Goal difference (goals_for - goals_against) (descending)
3. Goals scored (goals_for) (descending)
4. Team name (alphabetically ascending) - final tie-breaker

OUTPUT FORMAT:
DataFrame with columns:
  - ranking: 1-20 (position in league)
  - team: Team name
  - wins: Total wins
  - draws: Total draws
  - losses: Total losses
  - goals_for: Total goals scored
  - goals_against: Total goals conceded
  - points: Total points earned
"""
import pandas as pd
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class StandingsCalculator:
    """
    Computes final league standings from complete match data.
    
    RANKING RULES (applied in order):
    1. Points (descending) - 3 for win, 1 for draw, 0 for loss
    2. Goal difference (goals_for - goals_against) (descendin
    3. Goals scored (goals_for) (descending)
    4. Team name (alphabetically ascending) - final tie-breaker
    
    PROCESS:
    1. Initialize empty stats dict for each of the 20 valid teams
    2. Process each match: update winner/loser/draw, accumulate goals
    3. Build standings DataFrame with computed stats
    4. Sort by ranking criteria and add ranking column (1-20)
    """
    
    def __init__(self, scores_df: pd.DataFrame, valid_teams: set):
        """
        Parameters
        ----------
        scores_df : pd.DataFrame
            DataFrame with columns: game_id, home_goals, away_goals, etc.
        valid_teams : set
            Set of valid team names
        """
        self.scores_df = scores_df
        self.valid_teams = valid_teams
        self.standings = None
    
    def calculate_standings(self) -> pd.DataFrame:
        """
        Calculate final league standings.
        
        Returns
        -------
        pd.DataFrame
            Standings with columns: ranking, team, wins, draws, losses,
            goals_for, goals_against, points
        """
        # Initialize team stats
        team_stats = {}
        for team in self.valid_teams:
            team_stats[team] = {
                "won": 0,
                "drawn": 0,
                "lost": 0,
                "goals_for": 0,
                "goals_against": 0,
                "points": 0
            }
        
        # Process each match
        for _, row in self.scores_df.iterrows():
            # Extract game info from schedule (we'll need to get this from the merge)
            home_goals = row["home_goals"]
            away_goals = row["away_goals"]
            # Note: We need home_team and away_team from schedule
            # This will be passed via merge
            
            if "home_team" not in row or "away_team" not in row:
                logger.warning(f"Skipping game {row.get('game_id')}: missing team information")
                continue
            
            home_team = row["home_team"]
            away_team = row["away_team"]
            
            # Skip if teams not in valid set
            if home_team not in self.valid_teams or away_team not in self.valid_teams:
                continue
            
            # Update goals
            team_stats[home_team]["goals_for"] += home_goals
            team_stats[home_team]["goals_against"] += away_goals
            team_stats[away_team]["goals_for"] += away_goals
            team_stats[away_team]["goals_against"] += home_goals
            
            # Calculate points
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
                "points": stats["points"]
            })
        
        standings_df = pd.DataFrame(standings_data)
        
        # Fill any NaN values with 0 (for teams with missing score data)
        standings_df = standings_df.fillna(0)
        
        # Ensure integer columns are integers
        int_columns = ["wins", "draws", "losses", "goals_for", "goals_against", "points"]
        for col in int_columns:
            standings_df[col] = standings_df[col].astype(int)
        
        # Sort by ranking criteria (points, then goals_for, then team name for tie-breaking)
        standings_df = standings_df.sort_values(
            by=["points", "goals_for", "team"],
            ascending=[False, False, True]
        ).reset_index(drop=True)
        
        # Add ranking column
        standings_df.insert(0, "ranking", range(1, len(standings_df) + 1))
        
        self.standings = standings_df
        return standings_df
    
    def get_standings(self) -> pd.DataFrame:
        """Get the calculated standings."""
        if self.standings is None:
            self.calculate_standings()
        return self.standings
