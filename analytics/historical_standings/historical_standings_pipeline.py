"""
╔══════════════════════════════════════════════════════════════════════════════╗
║           HISTORICAL STANDINGS PIPELINE - BATCH/INCREMENTAL MODE            ║
║                    Processes data in weekly batches                          ║
╚══════════════════════════════════════════════════════════════════════════════╝

PURPOSE:
Simulate weekly standings updates by processing batches of new games/scores
BETWEEN consecutive Mondays, maintaining accumulated state across iterations.

KEY DESIGN PATTERN:
- Pipeline handles batch extraction (filters by date range each iteration)
- Pipeline handles state management (manually appends to accumulated DataFrames)
- Calculator receives pre-built state and computes standings snapshot
- This separation allows pipeline to be "smart" about data arrival patterns

EXECUTION FLOW (DETAILED):
1. Load & validate complete schedule and scores files (one-time bulk read)
2. Initialize empty HistoricalStandingsCalculator
3. Start with current_monday = Aug 25, 2025, previous_monday = None
4. Loop while new data exists:
   a. BATCH EXTRACTION (Pipeline responsibility):
      - First iteration: Filter games/scores with timestamp <= current_monday
      - Other iterations: Filter games/scores with previous_monday < timestamp <= current_monday
      - Creates batch_schedule and batch_scores DataFrames
   b. APPEND TO STATE (Pipeline responsibility):
      - Concatenate batch_schedule to calculator.accumulated_schedule
      - Concatenate batch_scores to calculator.accumulated_scores
      - This grows the "running total" of data seen so far
   c. CALCULATE SNAPSHOT (Calculator responsibility):
      - Call calculator._calculate_standings_from_accumulated(current_monday)
      - Uses FULL accumulated data (not just batch) to compute standings
      - Returns 20-row DataFrame with standings snapshot
   d. COLLECT RESULTS (Pipeline responsibility):
      - Append standings snapshot to all_standings output DataFrame
   e. ADVANCE MONDAY (Pipeline responsibility):
      - previous_monday = current_monday
      - current_monday += 7 days
   f. STOP CONDITION:
      - If batch_schedule or batch_scores is empty, break loop
      - This signals no new data between consecutive Mondays
5. FINALIZE:
   - Combine all weekly snapshots into single output
   - Sort by date then ranking
   - Save to standings_historical.csv

BATCH PROCESSING ADVANTAGES:
- Stateful: Maintains running totals across iterations (no re-computation)
- Memory efficient: Only processes new batch each iteration (not re-reading old data)
- Production realistic: Matches real data arrival patterns (weekly)
- Dynamic stopping: Automatically stops when data exhausted (no hardcoded cutoffs)

EXAMPLE PROGRESSION:
Monday 1 (Aug 25): Process 9 new games
  → Accumulated total: 9 games
  → Standings snapshot: 20 teams based on 9 games
  → Output: 1 snapshot (20 rows)

Monday 2 (Sep 01): Process 9 NEW games (between Aug 25 and Sep 01)
  → Accumulated total: 18 games (9 old + 9 new)
  → Standings snapshot: 20 teams based on 18 games
  → Output: 2 snapshots total (40 rows)

Monday 3 (Sep 08): Process 11 NEW games (between Sep 01 and Sep 08)
  → Accumulated total: 29 games (18 old + 11 new)
  → Standings snapshot: 20 teams based on 29 games
  → Output: 3 snapshots total (60 rows)

...continuing until no new data found...

Final (39 weeks): ~380 total games accumulated
  → Final output: 39 snapshots × 20 teams = 780 rows
"""

import pandas as pd
import logging
from datetime import datetime, timedelta
from pathlib import Path
import sys

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from analytics.historical_standings.historical_standings_calculator import HistoricalStandingsCalculator
from ingestion.pipeline.pipeline import DataPipelineCSV
from constants import expected_columns, valid_teams

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class HistoricalStandingsPipeline:
    """
    Pipeline for calculating historical standings using batch/incremental processing.
    
    KEY INSIGHT:
    This pipeline handles batch extraction AND state management itself (not delegating to calculator).
    The calculator is stateless at initialization and receives pre-computed batches.
    
    WORKFLOW:
    1. Load and validate schedule/scores data (one-time, full datasets)
    2. Initialize calculator with empty accumulated state
    3. For each Monday starting Aug 25, 2025:
       a. Extract NEW BATCH of games/scores between previous_monday and current_monday
          (First iteration: all games/scores up to current_monday)
       b. Manually append batch to calculator.accumulated_schedule and accumulated_scores
       c. Call calculator._calculate_standings_from_accumulated(current_monday)
       d. Collect standings snapshot from returned DataFrame
       e. Advance to next Monday (current_monday += 7 days)
    4. Stop when no new games/scores found between consecutive Mondays (batch is empty)
    5. Concatenate all weekly snapshots into single DataFrame
    6. Save to standings_historical.csv with proper sorting (by date, then ranking)
    
    BATCH EXTRACTION LOGIC:
    - First Monday (Aug 25): Filter all games/scores with timestamp <= Aug 25
    - Second Monday (Sep 01): Filter only NEW games/scores with Aug 25 < timestamp <= Sep 01
    - Third Monday (Sep 08): Filter only NEW games/scores with Sep 01 < timestamp <= Sep 08
    - Continue until no new data found between consecutive Mondays
    
    EFFICIENCY:
    - Only processes NEW data each iteration (not recalculating old data)
    - Maintains state to enable progressive calculation
    - Dynamic filtering stops when data is exhausted
    - Suitable for production where data arrives weekly
    """
    
    def __init__(self, data_dir: str = "data", output_dir: str = "outputs"):
        """
        Initialize pipeline with output directories.
        
        PARAMETERS:
        - data_dir: Directory containing schedule.csv and scores.csv
        - output_dir: Where to write standings_historical.csv
        """
        self.data_dir = Path(data_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.output_file = self.output_dir / "standings_historical.csv"
        self.log_file = Path("logs/standings") / "standings_historical.log"
        self.log_file.parent.mkdir(parents=True, exist_ok=True)
        
        # File handler for detailed logging
        file_handler = logging.FileHandler(self.log_file, mode='w')
        file_handler.setFormatter(logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        ))
        logger.addHandler(file_handler)
    
    def run(self, schedule_df: pd.DataFrame = None, scores_df: pd.DataFrame = None) -> None:
        """
        Execute the historical standings pipeline with dynamic batch extraction.
        
        PARAMETERS:
        - schedule_df: Pre-validated schedule DataFrame (optional, loads from CSV if None)
        - scores_df: Pre-validated scores DataFrame (optional, loads from CSV if None)
        
        PROCESS:
        1. Load data (from CSV or use pre-validated DataFrames)
        2. Initialize empty HistoricalStandingsCalculator
        3. Convert date columns to datetime for efficient filtering
        4. Loop through Mondays (Aug 25, 2025 to May 18, 2026):
           a. EXTRACT BATCH:
              - First iteration: Filter all games/scores <= current_monday
              - Subsequent iterations: Filter NEW games/scores between previous_monday and current_monday
              - Stop if batch is empty (no new data)
           b. APPEND TO STATE:
              - Concatenate new batch to calculator.accumulated_schedule
              - Concatenate new batch to calculator.accumulated_scores
              - This grows the accumulated state each iteration
           c. CALCULATE SNAPSHOT:
              - Call calculator._calculate_standings_from_accumulated(current_monday)
              - Returns 20-row DataFrame with standings for this Monday
           d. COLLECT RESULTS:
              - Append standings snapshot to all_standings
           e. ADVANCE MONDAY:
              - current_monday += 7 days
              - previous_monday = old current_monday
        5. After loop completes:
           - Combine all weekly snapshots into single DataFrame
           - Sort by updated_at (date) then ranking (1-20)
           - Save to CSV file
           - Log summary report
        
        OUTPUT:
        - standings_historical.csv: ~780 rows (39 Mondays × 20 teams)
        - Each row: ranking, team, wins, draws, losses, goals_for, goals_against, points, updated_at
        - Sorted chronologically by date, then by ranking within each date
        
        EFFICIENCY NOTES:
        - Date filtering done ONCE at start (converts to datetime once)
        - Boolean masks reused for efficient filtering
        - Batch size grows each iteration, but only new data is appended
        - Stops automatically when data exhausted
        
        EXAMPLE:
        --------
        # Monday 1: Aug 25 (9 new games in batch)
        # → extracted 9 games between -∞ and Aug 25
        # → accumulated_schedule: 9 games
        # → snapshot: 20 teams with stats for 9 games
        
        # Monday 2: Sep 01 (9 new games in batch)
        # → extracted 9 games between Aug 25 and Sep 01
        # → accumulated_schedule: 18 games (9 old + 9 new)
        # → snapshot: 20 teams with stats for 18 games
        
        # Monday 3: Sep 08 (11 new games in batch)
        # → extracted 11 games between Sep 01 and Sep 08
        # → accumulated_schedule: 29 games (18 old + 11 new)
        # → snapshot: 20 teams with stats for 29 games
        """
        logger.info("=" * 80)
        logger.info("Starting Historical Standings Pipeline - BATCH MODE")
        logger.info("=" * 80)
        
        # ────────────────────────────────────────────────────────────────────────────
        # LOAD & VALIDATE DATA (ONE-TIME)
        # ────────────────────────────────────────────────────────────────────────────
        if schedule_df is None or scores_df is None:
            logger.info("\nLoading and validating data...")
            
            if schedule_df is None:
                logger.info("Loading schedule data...")
                schedule_pipeline = DataPipelineCSV(
                    dataset_name="schedule",
                    enable_remediation=True
                )
                schedule_df, _, _ = schedule_pipeline.run_pipeline()
                logger.info(f"Schedule DataFrame shape: {schedule_df.shape}")
            
            if scores_df is None:
                logger.info("Loading scores data...")
                scores_pipeline = DataPipelineCSV(
                    dataset_name="scores",
                    enable_remediation=True
                )
                scores_df, _, _ = scores_pipeline.run_pipeline()
                logger.info(f"Scores DataFrame shape: {scores_df.shape}")
        else:
            logger.info("\nUsing pre-validated data from main pipeline")
            logger.info(f"Schedule DataFrame shape: {schedule_df.shape}")
            logger.info(f"Scores DataFrame shape: {scores_df.shape}")
        
        # ────────────────────────────────────────────────────────────────────────────
        # INITIALIZE CALCULATOR WITH BATCH MODE
        # ────────────────────────────────────────────────────────────────────────────
        # Note: Calculator initialized WITHOUT data - it will be passed each iteration
        calculator = HistoricalStandingsCalculator(
            valid_teams=valid_teams
        )
        
        logger.info("\nCalculator initialized in BATCH PROCESSING mode")
        logger.info("  - Maintains accumulated_schedule (grows each iteration)")
        logger.info("  - Maintains accumulated_scores (grows each iteration)")
        
        # ────────────────────────────────────────────────────────────────────────────
        # PROCESS EACH MONDAY WITH BATCH APPROACH (DYNAMIC FILTERING)
        # ────────────────────────────────────────────────────────────────────────────
        # Extract NEW batch between consecutive Mondays, append to cumulative
        # Stop when batch returns empty (no new data between Mondays)
        
        all_standings = pd.DataFrame()
        previous_monday = None
        current_monday = pd.Timestamp("2025-08-25 08:00:00", tz="UTC")
        iteration = 0
        
        logger.info(f"\nStarting dynamic Monday filtering...")
        logger.info(f"Starting date: {current_monday}")
        logger.info(f"Will continue filtering between consecutive Mondays until no new data is found")
        
        # Convert dates once for efficiency
        schedule_dates = pd.to_datetime(schedule_df['game_start_time'])
        scores_dates = pd.to_datetime(scores_df['ingestion_time'])
        
        while True:
            # Extract batch BETWEEN previous Monday and current Monday (end of day)
            current_monday_end = current_monday + pd.Timedelta(days=1) - pd.Timedelta(microseconds=1)
            
            if previous_monday is None:
                # First iteration: get all data up to end of current Monday
                # This includes all games from start of season through end of this Monday
                batch_schedule = schedule_df[schedule_dates <= current_monday_end]
                batch_scores = scores_df[scores_dates <= current_monday_end]
            else:
                # Subsequent iterations: get only NEW data between the two Mondays (end of day)
                previous_monday_end = previous_monday + pd.Timedelta(days=1) - pd.Timedelta(microseconds=1)
                batch_schedule = schedule_df[(schedule_dates > previous_monday_end) & (schedule_dates <= current_monday_end)]
                batch_scores = scores_df[(scores_dates > previous_monday_end) & (scores_dates <= current_monday_end)]
            
            # Stop if no new data found in this batch
            if len(batch_schedule) == 0 or len(batch_scores) == 0:
                logger.info(f"\nNo new data found between {previous_monday} and {current_monday} - stopping iteration")
                break
            
            iteration += 1
            logger.info(f"\n[{iteration}] Processing {current_monday}")
            logger.info(f"  New games in batch: {len(batch_schedule)}")
            logger.info(f"  New scores in batch: {len(batch_scores)}")
            logger.info(f"  Accumulated schedule size before: {len(calculator.accumulated_schedule)}")
            logger.info(f"  Accumulated scores size before: {len(calculator.accumulated_scores)}")
            
            try:
                # ═══════════════════════════════════════════════════════════════════
                # APPEND BATCH TO CUMULATIVE AND CALCULATE STANDINGS
                # ═══════════════════════════════════════════════════════════════════
                
                # Append batch to cumulative
                if calculator.accumulated_schedule is None:
                    calculator.accumulated_schedule = batch_schedule.copy()
                    calculator.accumulated_scores = batch_scores.copy()
                else:
                    calculator.accumulated_schedule = pd.concat(
                        [calculator.accumulated_schedule, batch_schedule],
                        ignore_index=True
                    )
                    calculator.accumulated_scores = pd.concat(
                        [calculator.accumulated_scores, batch_scores],
                        ignore_index=True
                    )
                
                logger.info(f"  Accumulated schedule size: {len(calculator.accumulated_schedule)} games")
                logger.info(f"  Accumulated scores size: {len(calculator.accumulated_scores)} scores")
                
                # Calculate standings on CUMULATIVE data
                standings_snapshot = calculator._calculate_standings_from_accumulated(current_monday)
                
                # Add to accumulated standings (output)
                all_standings = pd.concat(
                    [all_standings, standings_snapshot],
                    ignore_index=True
                )
                
                logger.info(f"  Teams with standings: {len(standings_snapshot)}")
                logger.info(f"  Accumulated standings rows: {len(all_standings)}")
                
            except Exception as e:
                logger.error(f"Error processing {current_monday}: {str(e)}", exc_info=True)
            
            # Move to next Monday
            previous_monday = current_monday
            current_monday += timedelta(days=7)
        
        # ────────────────────────────────────────────────────────────────────────────
        # FINALIZE OUTPUT
        # ────────────────────────────────────────────────────────────────────────────
        logger.info(f"\nFinal standings shape: {all_standings.shape}")
        
        # Sort by updated_at and ranking
        all_standings = all_standings.sort_values(
            by=["updated_at", "ranking"],
            ascending=[True, True]
        ).reset_index(drop=True)
        
        # Save to CSV
        all_standings.to_csv(self.output_file, index=False)
        logger.info(f"\nHistorical standings saved to: {self.output_file}")
        logger.info(f"Total rows in output: {len(all_standings)}")
        
        # ────────────────────────────────────────────────────────────────────────────
        # SUMMARY REPORT
        # ────────────────────────────────────────────────────────────────────────────
        logger.info("\n" + "=" * 80)
        logger.info("HISTORICAL STANDINGS PIPELINE COMPLETED - BATCH MODE")
        logger.info("=" * 80)
        logger.info(f"Output file: {self.output_file}")
        logger.info(f"Log file: {self.log_file}")
        logger.info(f"Processing mode: BATCH/INCREMENTAL (stateful, dynamic filtering)")
        logger.info(f"Total snapshots: {iteration}")
        logger.info(f"Total rows (20 teams × {iteration} snapshots): {len(all_standings)}")
        
        # Get date range from accumulated data
        if len(all_standings) > 0:
            first_date = all_standings["updated_at"].min()
            last_date = all_standings["updated_at"].max()
            logger.info(f"Date range: {first_date} to {last_date}")
        
        logger.info(f"Final accumulated schedule: {len(calculator.accumulated_schedule)} games")
        logger.info(f"Final accumulated scores: {len(calculator.accumulated_scores)} scores")
        logger.info(f"Filtering stopped when no more data was found")
        logger.info("=" * 80)
        
        # Print sample output
        logger.info("\nSample output (first 5 rows):")
        logger.info("\n" + all_standings.head(5).to_string())
        
        # Print latest snapshot
        latest_date = all_standings["updated_at"].max()
        latest_snapshot = all_standings[all_standings["updated_at"] == latest_date]
        logger.info(f"\nLatest snapshot ({latest_date}):")
        logger.info("\n" + latest_snapshot.head(10).to_string())
        
        print("\n[OK] Historical standings pipeline completed successfully (BATCH MODE)")
        print(f"Output: {self.output_file}")


if __name__ == "__main__":
    pipeline = HistoricalStandingsPipeline()
    pipeline.run()
