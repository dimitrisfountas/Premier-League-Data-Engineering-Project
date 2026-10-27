"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                   MASTER DATA ENGINEERING PIPELINE                          ║
║                         Integration Orchestrator                            ║
╚══════════════════════════════════════════════════════════════════════════════╝

PIPELINE EXECUTION SERIES:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

┌─ STEP 1: DATA VALIDATION & REMEDIATION ─────────────────────────────────┐
│ INPUT:  schedule.csv, scores.csv (raw data)                             │
│ PROCESS: Ingest → Validate → Remediate → Store validated DataFrames     │
│ OUTPUT:  Validated DataFrames (in memory) + validation logs             │
│ PURPOSE: Ensure data quality before downstream processing               │
│ FILES:   pipeline/pipeline.py                                           │
└─────────────────────────────────────────────────────────────────────────┘
                                   ↓
┌─ STEP 2: HISTORICAL STANDINGS SIMULATION ───────────────────────────────┐
│ INPUT:  Validated schedule & scores DataFrames from STEP 1              │
│ PROCESS: Calculate standings at 39 weekly snapshots (Aug 25 - May 18)   │
│ OUTPUT:  outputs/standings_historical.csv (780 rows = 39 weeks × 20)    │
│ PURPOSE: Show league progression over time                              │
│ FILES:   analytics/historical_standings_pipeline.py                     │
│          analytics/historical_standings_calculator.py                   │
└─────────────────────────────────────────────────────────────────────────┘
                                   ↓
┌─ STEP 4: RECENT FORM ANALYSIS (SQL-BASED) ──────────────────────────────┐
│ INPUT:  outputs/standings_historical.csv from STEP 2                    │
│ PROCESS: Load into SQLite → Compare consecutive snapshots               │
│          → Rank top 3 teams per Monday                                   │
│ OUTPUT:  outputs/recent_form.csv (114 rows = 38 weeks × 3 teams)       │
│ PURPOSE: Identify top performers in each weekly interval                │
│ TECH:    SQLite in-memory database with JOIN queries                    │
│ FILES:   analytics/form_analysis.py                                     │
└─────────────────────────────────────────────────────────────────────────┘
                                   ↓
         (STEP 3 runs in PARALLEL with STEPS 2 & 4)
         ├─ Uses validated data from STEP 1 directly
         └─ Does not depend on STEP 2 or STEP 4
                                   ↓
┌─ STEP 3: FINAL STANDINGS CALCULATION ────────────────────────────────────┐
│ INPUT:  Validated schedule & scores DataFrames from STEP 1              │
│ PROCESS: Aggregate results for entire season → Rank all 20 teams        │
│ OUTPUT:  outputs/standings_final.csv (20 rows = all teams final)        │
│ PURPOSE: Show final league position after all 380 matches               │
│ FILES:   standings/standings_pipeline.py                                │
│          standings/standings_calculator.py                              │
└─────────────────────────────────────────────────────────────────────────┘

═════════════════════════════════════════════════════════════════════════════

EXECUTION FLOW DIAGRAM:
═════════════════════════════════════════════════════════════════════════════

   schedule.csv ┐
               ├──→ [STEP 1: Validation & Remediation] ──→ logs/validation/
    scores.csv ┘
                        ↓
                  validated_data (dict)
                  ├─ schedule: DataFrame ✓
                  ├─ scores: DataFrame ✓
                  └─ logs: CSV files ✓
                        ↓
        ┌───────────────┼───────────────┐
        ↓               ↓               ↓
   [STEP 2]        [STEP 4]        [STEP 3]
   Historical       Form            Final
   Standings        Analysis        Standings
   (38 weeks ×      (SQLite,        (All season
   20 teams)        Top 3/week)      x 20 teams)
        ↓               ↓               ↓
   standings_      recent_form.    standings_
   historical.csv  csv             final.csv
        ↓
   (STEP 4 uses
    this output)

═════════════════════════════════════════════════════════════════════════════
"""

import sys
import logging
import pandas as pd
from pathlib import Path
from datetime import datetime

# Add paths for imports
sys.path.insert(0, str(Path(__file__).parent))

# Import pipeline components
from ingestion.pipeline.pipeline import DataPipelineCSV  # STEP 1: Validation & Remediation
from analytics.historical_standings.historical_standings_pipeline import HistoricalStandingsPipeline  # STEP 2
from analytics.standings.standings_pipeline import StandingsPipeline  # STEP 3
from analytics.form.form_analysis import FormAnalyzer  # STEP 4


def setup_logging():
    """
    Configure logging to file and console with multiple handlers.
    
    OUTPUT DESTINATIONS:
    - File (DEBUG level): logs/pipeline/pipeline_YYYYMMDD_HHMMSS.log
      Contains all debug information for troubleshooting
    - Console (INFO level): Terminal output
      Shows only important progress updates and errors
    
    RETURN:
    - logger: Configured logger instance
    """
    log_dir = Path("logs/pipeline")
    log_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"pipeline_{timestamp}.log"
    
    # Create logger
    logger = logging.getLogger("pipeline")
    logger.setLevel(logging.DEBUG)
    
    # File handler (all messages)
    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.DEBUG)
    
    # Console handler (info and above only)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    
    # Formatter
    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    
    logger.addHandler(fh)
    logger.addHandler(ch)
    
    return logger


def format_validation_results(results):
    """
    Format validation results for display.
    
    PROCESS:
    1. Separate failed tests from passed tests
    2. Display failed tests with their expectations
    3. Display pass/fail summary
    
    PARAMETERS:
    - results: List of validation result dicts
    
    RETURN:
    - Formatted string for logging
    """
    if not results:
        return "No validation checks ran."
    
    failed = [r for r in results if not r.get("success", False)]
    passed = [r for r in results if r.get("success", False)]
    
    output = []
    if failed:
        output.append(f"[FAILED] {len(failed)} test(s) failed:")
        for r in failed:
            expectation = r.get("expectation", "Unknown")
            output.append(f"  - {expectation}")
    else:
        output.append("[PASSED] All tests passed")
    
    return "\n".join(output)


def main():
    """
    Execute complete data engineering pipeline with all 4 steps.
    
    SERIES OF EXECUTION:
    1. Setup logging (initialize log files)
    2. STEP 1: Data Validation & Remediation (schedule & scores)
    3. STEP 2: Historical Standings Simulation (39 weekly snapshots)
    4. STEP 4: Recent Form Analysis (SQL, top 3 teams per week)
    5. STEP 3: Final Standings Calculation (final season rankings)
    6. Summary report and deliverables list
    
    DATA FLOW:
    - STEP 1 validates and stores data → passed to STEP 2 & 3
    - STEP 2 outputs file → consumed by STEP 4 (via CSV)
    - STEP 3 runs independently (parallel execution)
    
    RETURN:
    - True: All steps completed successfully
    - False: Error encountered, check logs/pipeline/pipeline_*.log
    """
    logger = setup_logging()
    
    logger.info("="*80)
    logger.info("DATA ENGINEERING PIPELINE - MASTER ORCHESTRATION")
    logger.info("="*80)
    
    # ════════════════════════════════════════════════════════════════════════════
    # STEP 1: DATA VALIDATION & REMEDIATION
    # ════════════════════════════════════════════════════════════════════════════
    # PURPOSE: Ensure input data is clean before downstream processing
    # 
    # PROCESS:
    # 1. Load CSV files (schedule.csv, scores.csv)
    # 2. Run validation checks (null values, duplicates, type validation)
    # 3. If checks fail:
    #    a. Apply remediation (drop nulls, remove duplicates)
    #    b. Re-validate
    #    c. Store results
    # 4. If checks pass: Skip remediation
    # 5. Store validated DataFrames in memory for STEP 2 & 3
    #
    # OUTPUT: logs/validation/*.csv with test results
    # ════════════════════════════════════════════════════════════════════════════
    logger.info("\n" + "="*80)
    logger.info("STEP 1: DATA VALIDATION & REMEDIATION")
    logger.info("="*80)
    logger.info("Processing schedule and scores datasets...")
    logger.info("Status: Ingestion -> Validation -> Remediation")
    
    datasets = ["schedule", "scores"]
    validated_data = {}
    
    for dataset_name in datasets:
        logger.info(f"\nValidating {dataset_name.upper()}...")
        
        try:
            # Initialize pipeline component for this dataset
            pipeline = DataPipelineCSV(dataset_name, enable_remediation=True)
            
            # Run complete validation + remediation workflow
            df, _, _ = pipeline.run_pipeline()
            
            # Store validated dataframe in memory for downstream use
            validated_data[dataset_name] = df
            
            # Read validation log to show what failed and was fixed
            log_file = Path("logs/validation") / f"{dataset_name}_validation_full.csv"
            if log_file.exists():
                try:
                    val_df = pd.read_csv(log_file)
                    
                    # Check if validation_stage column exists (skip if old format)
                    if "validation_stage" not in val_df.columns:
                        logger.info(f"Validation completed (validation logs need regeneration)")
                        continue
                    
                    # Show pre-remediation failures (unique)
                    pre_failures = val_df[
                        (val_df["validation_stage"] == "pre-remediation") & 
                        (val_df["success"].astype(str).str.lower() == "false")
                    ]["expectation"].unique()
                    
                    if len(pre_failures) > 0:
                        logger.warning(f"\nIssues detected before remediation:")
                        for exp in pre_failures:
                            logger.warning(f"  - {exp}")
                        
                        # Check if they were fixed
                        post_failures = val_df[
                            (val_df["validation_stage"] == "post-remediation") & 
                            (val_df["success"].astype(str).str.lower() == "false")
                        ]["expectation"].unique()
                        
                        fixed = len(pre_failures) - len(post_failures)
                        if fixed > 0:
                            logger.info(f"Remediation fixed {fixed} issue(s)")
                    else:
                        logger.info(f"No issues found")
                except Exception as e:
                    logger.warning(f"Could not parse validation logs: {str(e)}")
                    logger.info(f"Validation completed (logs will be updated next run)")
            
        except Exception as e:
            logger.error(f"Error validating {dataset_name}: {str(e)}", exc_info=True)
            return False
    
    logger.info("\n" + "="*80)
    logger.info("[OK] Data validation completed successfully")
    logger.info("="*80)
    
    # ════════════════════════════════════════════════════════════════════════════
    # STEP 2: HISTORICAL STANDINGS SIMULATION
    # ════════════════════════════════════════════════════════════════════════════
    # PURPOSE: Calculate league standings at multiple points in time
    #
    # PROCESS:
    # 1. Iterate through 39 Mondays (Aug 25, 2025 to May 18, 2026)
    # 2. For each Monday:
    #    a. Filter matches played up to that date
    #    b. Calculate standings (wins, losses, draws, points)
    #    c. Rank teams by points, goal difference, etc.
    # 3. Aggregate all snapshots into one file
    #
    # OUTPUT: outputs/standings_historical.csv (780 rows)
    #         39 Mondays × 20 teams per Monday
    # ════════════════════════════════════════════════════════════════════════════
    logger.info("\n" + "="*80)
    logger.info("STEP 2: HISTORICAL STANDINGS SIMULATION")
    logger.info("="*80)
    logger.info("Simulating weekly pipeline runs from Aug 25, 2025 to May 18, 2026")
    logger.info("Status: 39 Monday snapshots x 20 teams = 780 historical records")
    
    try:
        historical_pipeline = HistoricalStandingsPipeline()
        historical_pipeline.run(
            schedule_df=validated_data["schedule"],
            scores_df=validated_data["scores"]
        )
        logger.info("[OK] Historical standings simulation completed successfully")
    except Exception as e:
        logger.error(f"Error in historical standings pipeline: {str(e)}", exc_info=True)
        return False
    
    # ════════════════════════════════════════════════════════════════════════════
    # STEP 3: FINAL STANDINGS CALCULATION
    # ════════════════════════════════════════════════════════════════════════════
    # PURPOSE: Calculate final season standings using all 380 matches
    #
    # PROCESS:
    # 1. Merge schedule with scores on game_id
    # 2. Calculate aggregate stats for each team:
    #    - Wins, draws, losses (entire season)
    #    - Goals for, goals against (entire season)
    #    - Total points
    # 3. Rank teams using priority system
    # 4. Save as outputs/standings_final.csv
    #
    # RANKING: Points > Goal Diff > Goals For > Team Name
    #
    # OUTPUT: outputs/standings_final.csv (20 rows, all teams)
    # ════════════════════════════════════════════════════════════════════════════
    logger.info("\n" + "="*80)
    logger.info("STEP 3: FINAL STANDINGS CALCULATION")
    logger.info("="*80)
    logger.info("Calculating final season standings for all 20 teams")
    
    try:
        standings_pipeline = StandingsPipeline(
            schedule_df=validated_data["schedule"],
            scores_df=validated_data["scores"]
        )
        standings = standings_pipeline.run()
        
        logger.info("\n" + "="*80)
        logger.info("FINAL LEAGUE STANDINGS")
        logger.info("="*80)
        logger.info(f"\n{standings.to_string(index=False)}")
        logger.info("\n[OK] Final standings calculation completed successfully")
    except Exception as e:
        logger.error(f"Error in final standings pipeline: {str(e)}", exc_info=True)
        return False
    
    # ════════════════════════════════════════════════════════════════════════════
    # STEP 4: RECENT FORM ANALYSIS
    # ════════════════════════════════════════════════════════════════════════════
    # PURPOSE: Identify top 3 teams based on LAST 5 MATCHES performance
    #
    # TECHNOLOGY: SQLite in-memory database with SQL subqueries
    #
    # PROCESS:
    # 1. Load standings_historical.csv (output from STEP 2) into SQLite table
    # 2. For each Monday:
    #    a. Find each team's record from 5 matches ago (using total_matches)
    #    b. Calculate metrics from the last 5 matches (delta between records)
    #    c. Build form string from win/draw/loss counts in those 5 matches
    #    d. Rank teams by: Points DESC > Goal Diff DESC > Goals Conceded ASC
    #    e. Extract top 3 teams
    # 3. Save results to outputs/recent_form.csv
    #
    # OUTPUT: outputs/recent_form.csv (~114 rows)
    #         ~3 teams × 38 Mondays (first Monday has no baseline)
    #
    # SCHEMA: form_ranking, team, goals_for, goals_against, form, points, updated_at
    # FORM EXAMPLES: "LDWWW" (loss, draw, 3 wins), "LLLLL" (5 losses), "W" (1 match)
    logger.info("\n" + "="*80)
    logger.info("STEP 4: RECENT FORM ANALYSIS")
    logger.info("="*80)
    logger.info("Identifying top 3 teams by recent performance at each Monday")
    logger.info("Technology: SQLite in-memory database with SQL JOIN queries")
    
    try:
        form_analyzer = FormAnalyzer()
        form_df = form_analyzer.run()
        
        logger.info("[OK] Form analysis completed successfully")
        logger.info(f"Analyzed form for {len(form_df)} team-date combinations")
        
        # Show sample results
        logger.info("\nSample recent form results (first 15 rows):")
        logger.info("\n" + form_df.head(15).to_string(index=False))
        
    except Exception as e:
        logger.error(f"Error in form analysis: {str(e)}", exc_info=True)
        return False
    
    # ════════════════════════════════════════════════════════════════════════════
    # COMPLETION REPORT
    # ════════════════════════════════════════════════════════════════════════════
    # Summary of all outputs and their purposes
    # ════════════════════════════════════════════════════════════════════════════
    logger.info("\n" + "="*80)
    logger.info("PIPELINE EXECUTION COMPLETED")
    logger.info("="*80)
    logger.info("\nDELIVERABLES GENERATED:")
    logger.info("\nValidation Logs (STEP 1):")
    logger.info("  |- logs/validation/schedule_validation_summary.csv")
    logger.info("  |- logs/validation/schedule_validation_full.csv")
    logger.info("  |- logs/validation/scores_validation_summary.csv")
    logger.info("  |- logs/validation/scores_validation_full.csv")
    logger.info("\nHistorical Standings (STEP 2):")
    logger.info("  |- outputs/standings_historical.csv (780 rows, 39 weekly snapshots)")
    logger.info("  |- logs/standings/standings_historical.log")
    logger.info("\nFinal Standings (STEP 3):")
    logger.info("  |- outputs/standings_final.csv (20 teams, final rankings)")
    logger.info("  |- logs/standings/standings_final.log")
    logger.info("\nRecent Form Analysis (STEP 4):")
    logger.info("  |- outputs/recent_form.csv (top 3 teams at each Monday)")
    logger.info("\n" + "="*80)
    logger.info("[OK] All pipeline tasks completed successfully")
    logger.info("="*80)
    
    return True


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
