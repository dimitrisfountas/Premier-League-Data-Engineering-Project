#--------------------------------------------------------
# Schema validation Constants
#--------------------------------------------------------

# Expected columns per dataset
expected_columns = { 
    "schedule":
        {
        "game_id",
        "round",
        "home_team", 
        "away_team",
        "game_start_time", 
        "updated_at", 
        "source",
        "ingestion_path", 
        "last_updated_at"
        },
    "scores":
    {
        "game_id", 
        "home_goals", 
        "away_goals",
        "ingestion_time", 
        "updated_at", 
        "source", 
        "ingestion_path",
        "last_updated_at"}
    }

# --------------------------------------------------------
# Valid team names for schedule dataset
#--------------------------------------------------------

valid_teams = {
    "Arsenal",
    "Aston Villa",
    "Bournemouth",
    "Brentford",
    "Brighton",
    "Burnley",
    "Chelsea",
    "Crystal Palace",
    "Everton",
    "Fulham",
    "Leeds United",
    "Liverpool",
    "Manchester City",
    "Manchester Utd",
    "Newcastle",
    "Nottingham",
    "Sunderland",
    "Tottenham Hotspur",
    "West Ham",
    "Wolves",
}