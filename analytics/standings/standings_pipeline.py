"""
╔══════════════════════════════════════════════════════════════════════════════╗
║              FINAL STANDINGS PIPELINE - SINGLE SNAPSHOT MODE                 ║
║         Calculates league standings from complete validated data              ║
╚══════════════════════════════════════════════════════════════════════════════╝

PURPOSE:
Calculate final league standings using all validated schedule and scores data.
Produces a single standings snapshot representing the complete league season.

EXECUTION FLOW:
1. Load & validate schedule and scores data through quality pipeline
2. Merge schedule with scores (keeping all schedule records)
3. Filter to games with complete score data (home_goals, away_goals)
4. Calculate standings using StandingsCalculator:
   - Process each game: update team stats (wins/draws/losses, goals, points)
   - Rank teams by: points (desc) → goal_difference (desc) → goals_for (desc)
5. Save standings to standings_final.csv
6. Save execution log to standings_final.log

DIFFERENCE FROM HISTORICAL PIPELINE:
- Historical: Batch processing with Monday checkpoints (39 snapshots)
- Final: Single calculation on complete validated data (1 snapshot)

OUTPUT:
- standings_final.csv: 20 rows (one per team) with ranking, stats, points
"""
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import pandas as pd
import logging
from ingestion.csv_ingestor import CSVIngestor
from .standings_calculator import StandingsCalculator
from ingestion.pipeline.pipeline import DataPipelineCSV
from constants import valid_teams

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class StandingsPipeline:
    """
    Pipeline for calculating and saving final league standings.
    
    WORKFLOW:
    1. Load schedule and scores through DataPipelineCSV (validation + remediation)
    2. Merge schedule ← scores using LEFT join (keep all schedule records)
    3. Filter to games with complete scores (dropna on home_goals, away_goals)
    4. Pass merged data to StandingsCalculator
    5. Save final standings to CSV and log file
    
    DATA FLOW:
    Raw CSV files → Validation pipeline → Remediation → Merged DataFrame
    → StandingsCalculator → Final standings → Save to CSV/Log
    """
    
    def __init__(self, schedule_df: pd.DataFrame = None, scores_df: pd.DataFrame = None):
        self.schedule_df = schedule_df
        self.scores_df = scores_df
        self.standings_df = None
        self.log_path = Path("logs/standings/standings_final.log")
        self.csv_path = Path("outputs/standings_final.csv")
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
    
    def load_and_validate_data(self):
        """Load schedule and scores data through validation pipeline."""
        if self.schedule_df is None or self.scores_df is None:
            logger.info("Loading and validating schedule data...")
            schedule_pipeline = DataPipelineCSV("schedule", enable_remediation=True)
            self.schedule_df, _, _ = schedule_pipeline.run_pipeline()
            logger.info(f"Loaded and validated {len(self.schedule_df)} schedule records")
            
            logger.info("Loading and validating scores data...")
            scores_pipeline = DataPipelineCSV("scores", enable_remediation=True)
            self.scores_df, _, _ = scores_pipeline.run_pipeline()
            logger.info(f"Loaded and validated {len(self.scores_df)} scores records")
        else:
            logger.info("Using pre-validated data from main pipeline")
            logger.info(f"Schedule records: {len(self.schedule_df)}")
            logger.info(f"Scores records: {len(self.scores_df)}")
    
    def calculate_standings(self):
        """Calculate standings from schedule and scores."""
        logger.info("Calculating standings...")
        
        # Merge schedule with scores using LEFT join to keep all schedule matches
        merged_df = self.schedule_df.merge(
            self.scores_df,
            on="game_id",
            how="left"
        )
        logger.info(f"Merged {len(merged_df)} games with scores (including unmatched)")
        
        # Filter to only games that have scores
        merged_df = merged_df.dropna(subset=["home_goals", "away_goals"])
        logger.info(f"Calculated standings from {len(merged_df)} games with complete scores")
        
        # Calculate standings
        calculator = StandingsCalculator(merged_df, valid_teams)
        self.standings_df = calculator.calculate_standings()
        
        logger.info(f"Calculated standings for {len(self.standings_df)} teams")
        logger.info("\nStandings:")
        logger.info(self.standings_df.to_string())
    
    def save_standings(self):
        """Save standings to CSV file."""
        logger.info(f"Saving standings to {self.csv_path}...")
        self.standings_df.to_csv(self.csv_path, index=False)
        logger.info(f"Standings saved successfully")
    
    def save_log(self):
        """Save execution log to file."""
        logger.info(f"Saving execution log to {self.log_path}...")
        # The logging handlers should handle this, but we can also save a summary
        with open(self.log_path, "w") as f:
            f.write("STANDINGS CALCULATION LOG\n")
            f.write("="*60 + "\n\n")
            f.write(f"Schedule records loaded: {len(self.schedule_df)}\n")
            f.write(f"Scores records loaded: {len(self.scores_df)}\n")
            f.write(f"Teams in standings: {len(self.standings_df)}\n\n")
            f.write("Final Standings:\n")
            f.write("-"*60 + "\n")
            f.write(self.standings_df.to_string())
            f.write("\n\nExecution completed successfully.\n")
    
    def run(self):
        """Run the full standings pipeline."""
        logger.info("Starting Standings Pipeline...")
        logger.info("="*60)
        
        self.load_and_validate_data()
        self.calculate_standings()
        self.save_standings()
        self.save_log()
        
        logger.info("="*60)
        logger.info("Standings Pipeline completed successfully!")
        
        return self.standings_df


if __name__ == "__main__":
    pipeline = StandingsPipeline()
    pipeline.run()
