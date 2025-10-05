import requests
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import logging
from typing import Dict, List, Optional
import os
from dotenv import load_dotenv
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from contextlib import contextmanager
import re

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# MySQL Configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'user': os.getenv('DB_USER', 'root'),
    'database': os.getenv('DB_NAME', 'cricbuzz2'),
    'password': os.getenv('DB_PASSWORD', 'Root'),
    'port': int(os.getenv('DB_PORT', 3306))
}

# API Config
RAPIDAPI_KEY = os.getenv('RAPIDAPI_KEY', "5ce88f231emsh2f38201f4043345p127a9djsndd5f0ca90c70")
RAPIDAPI_HOST = "cricbuzz-cricket.p.rapidapi.com"

API_ENDPOINTS = {
    'live_matches': f"https://cricbuzz-cricket.p.rapidapi.com/matches/v1/live",
    'match_scorecard': f"https://cricbuzz-cricket.p.rapidapi.com/mcenter/v1/{{match_id}}/scard",
    'match_commentary': f"https://cricbuzz-cricket.p.rapidapi.com/mcenter/v1/{{match_id}}/comm"
}

HEADERS = {
    "x-rapidapi-key": RAPIDAPI_KEY,
    "x-rapidapi-host": RAPIDAPI_HOST
}

def create_session_with_retries(retries: int = 3, backoff_factor: float = 0.5) -> requests.Session:
    """Create a requests session with retry logic."""
    session = requests.Session()
    retry_strategy = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

# Create global session
SESSION = create_session_with_retries()

def save_response_for_debug(data: dict, filename: str):
    """Save API response to file for debugging."""
    import json
    debug_dir = 'debug_responses'
    os.makedirs(debug_dir, exist_ok=True)
    filepath = os.path.join(debug_dir, filename)
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=2)
    logger.debug(f"Saved debug response to {filepath}")

def fetch_api_data(url: str, endpoint_name: str = "API", save_debug: bool = False) -> Optional[dict]:
    """Fetch data from API with error handling and retries."""
    try:
        response = SESSION.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        data = response.json()
        logger.info(f"✓ Successfully fetched {endpoint_name}")
        
        if save_debug:
            filename = f"{endpoint_name.replace(' ', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            save_response_for_debug(data, filename)
        
        return data
    except requests.Timeout:
        logger.error(f"✗ Request timeout for {endpoint_name}")
        return None
    except requests.RequestException as e:
        logger.error(f"✗ HTTP error for {endpoint_name}: {e}")
        return None
    except ValueError as e:
        logger.error(f"✗ JSON decode error for {endpoint_name}: {e}")
        return None

def safe_get(dictionary: dict, *keys, default=None):
    """Safely navigate nested dictionaries with multiple possible keys."""
    for key in keys:
        value = dictionary.get(key)
        if value is not None:
            return value
    return default

def safe_float(val):
    """Convert value to float safely, return None if invalid."""
    if val is None or val == '':
        return None
    try:
        if isinstance(val, (int, float)):
            return float(val)
        if isinstance(val, str):
            import re
            match = re.search(r"[-+]?\d*\.?\d+", val)
            if match:
                return float(match.group())
        return None
    except (ValueError, TypeError):
        return None

def safe_int(val):
    """Convert value to int safely, return None if invalid."""
    if val is None or val == '':
        return None
    try:
        if isinstance(val, bool):
            return int(val)
        return int(float(val))
    except (ValueError, TypeError):
        return None

def extract_commentary_data(commentary_json: dict, match_id: int) -> List[dict]:
    """Extract commentary lines into a flat list of dicts."""
    commentary_lines = []
    
    comwrapper = commentary_json.get('comwrapper', [])
    
    if not comwrapper:
        logger.debug(f"No commentary data found for match {match_id}")
        return commentary_lines
    
    for item in comwrapper:
        comm = item.get('commentary', {})
        
        if not comm:
            continue
        
        # Extract runs scored from commentary text or event type
        runs_scored = 0
        comm_text = comm.get('commtxt', '')
        event_type = comm.get('eventtype', '')
        
        if event_type == 'FOUR':
            runs_scored = 4
        elif event_type == 'SIX':
            runs_scored = 6
        else:
            run_match = re.search(r'(\d+)\s+runs?', comm_text)
            if run_match:
                runs_scored = int(run_match.group(1))
            elif 'no run' in comm_text.lower():
                runs_scored = 0
        
        commentary_lines.append({
            'match_id': match_id,
            'innings': safe_int(comm.get('inningsid')),
            'over_number': safe_float(comm.get('overnum')),
            'ball_number': safe_int(comm.get('ballnbr')),
            'timestamp': comm.get(datetime('timestamp')),
            'event_type': event_type,
            'commentary_text': comm_text,
            'runs_scored': runs_scored,
            'bat_team_score': safe_int(comm.get('batteamscore')),
            'toss_winner':comm.get('tosswinnername'),
            'fetched_at': datetime.now()
        })
    
    return commentary_lines

def extract_venue_info(venue_dict: dict, match_id: int) -> Optional[dict]:
    """Extract and flatten venue information."""
    if not venue_dict:
        return None
    
    venue_info = {
        'match_id': match_id,
        'venue_id': venue_dict.get('id'),
        'ground': venue_dict.get('ground'),
        'city': venue_dict.get('city'),
        'timezone': venue_dict.get('timezone'),
        'latitude': venue_dict.get('latitude'),
        'longitude': venue_dict.get('longitude')
    }
    return venue_info

def extract_team_info(team_dict: dict, match_id: int, team_role: str) -> Optional[dict]:
    """Extract and flatten team information."""
    if not team_dict:
        return None
    
    team_info = {
        'match_id': match_id,
        'team_role': team_role,
        'team_id': safe_get(team_dict, 'teamid', 'teamId'),
        'team_name': safe_get(team_dict, 'teamname', 'teamName'),
        'team_sname': safe_get(team_dict, 'teamsname', 'teamSName')
    }
    return team_info

def extract_official_info(official_dict: dict, match_id: int, role: str) -> Optional[dict]:
    """Extract umpire or referee information."""
    if not official_dict or not official_dict.get('id'):
        return None
    
    return {
        'match_id': match_id,
        'role': role,
        'official_id': official_dict.get('id'),
        'name': official_dict.get('name'),
        'country': official_dict.get('country')
    }

def extract_player_stats_and_partnerships(scorecard_data: dict, match_id: int) -> tuple:
    """
    Extract batting, bowling, and partnership statistics from scorecard.
    Returns: (batsmen_list, bowlers_list, partnerships_list)
    """
    all_batsmen = []
    all_bowlers = []
    all_partnerships = []
    
    if not scorecard_data:
        logger.warning(f"No scorecard data for match {match_id}")
        return all_batsmen, all_bowlers, all_partnerships
    
    scorecards = None
    if 'scorecard' in scorecard_data:
        scorecards = scorecard_data['scorecard']
    elif 'scoreCard' in scorecard_data:
        scorecards = scorecard_data['scoreCard']
    elif 'innings' in scorecard_data:
        scorecards = scorecard_data['innings']
    
    if not scorecards:
        logger.warning(f"Could not find scorecard data for match {match_id}")
        return all_batsmen, all_bowlers, all_partnerships
    
    for innings in scorecards:
        innings_id = innings.get('inningsid') or innings.get('inningsId')
        batting_team_name = innings.get('batteamname') or innings.get('batTeamName')

        # Build position lookup
        batsman_positions = {}
        batsmen_list = innings.get('batsman') or innings.get('batCardList', [])
        
        # Extract batsmen with positions
        for position, bat_card in enumerate(batsmen_list, start=1):
            batsman_id = bat_card.get('id') or bat_card.get('batId')
            if batsman_id:
                batsman_positions[batsman_id] = position
            
            batsman = {
                'match_id': match_id,
                'innings_id': innings_id,
                'team_name': batting_team_name,
                'batsman_id': batsman_id,
                'batsman_name': bat_card.get('name') or bat_card.get('batName'),
                'batting_position': position,
                'runs': safe_int(bat_card.get('runs')),
                'balls_faced': safe_int(bat_card.get('balls')),
                'fours': safe_int(bat_card.get('fours')),
                'sixes': safe_int(bat_card.get('sixes')),
                'strike_rate': safe_float(bat_card.get('strkrate') or bat_card.get('strikeRate')),
                'out_desc': bat_card.get('outdec') or bat_card.get('outDesc')
            }
            all_batsmen.append(batsman)

        # Extract bowlers
        bowlers_list = innings.get('bowler') or innings.get('bowlCardList', [])
        for bowl_card in bowlers_list:
            bowler = {
                'match_id': match_id,
                'innings_id': innings_id,
                'team_name': batting_team_name,
                'bowler_id': bowl_card.get('id') or bowl_card.get('bowlId'),
                'bowler_name': bowl_card.get('name') or bowl_card.get('bowlName'),
                'overs': safe_float(bowl_card.get('overs')),
                'maidens': safe_int(bowl_card.get('maidens')),
                'runs_conceded': safe_int(bowl_card.get('runs')),
                'wickets': safe_int(bowl_card.get('wickets')),
                'economy': safe_float(bowl_card.get('economy')),
                'no_balls': safe_int(bowl_card.get('noballs') or bowl_card.get('no_balls', 0)),
                'wides': safe_int(bowl_card.get('wides', 0))
            }
            all_bowlers.append(bowler)
        
        # Extract partnerships
        partnership_obj = innings.get('partnership', {})
        partnerships_list = partnership_obj.get('partnership', [])
        
        for idx, partnership in enumerate(partnerships_list):
            bat1_id = partnership.get('bat1id')
            bat2_id = partnership.get('bat2id')
            bat1_position = batsman_positions.get(bat1_id)
            bat2_position = batsman_positions.get(bat2_id)
            
            partnership_record = {
                'match_id': match_id,
                'innings_id': innings_id,
                'team_name': batting_team_name,
                'partnership_number': idx + 1,
                'bat1_id': bat1_id,
                'bat1_name': partnership.get('bat1name'),
                'bat1_runs': safe_int(partnership.get('bat1runs')),
                'bat1_balls': safe_int(partnership.get('bat1balls')),
                'bat1_fours': safe_int(partnership.get('bat1fours')),
                'bat1_sixes': safe_int(partnership.get('bat1sixes')),
                'bat1_position': bat1_position,
                'bat2_id': bat2_id,
                'bat2_name': partnership.get('bat2name'),
                'bat2_runs': safe_int(partnership.get('bat2runs')),
                'bat2_balls': safe_int(partnership.get('bat2balls')),
                'bat2_fours': safe_int(partnership.get('bat2fours')),
                'bat2_sixes': safe_int(partnership.get('bat2sixes')),
                'bat2_position': bat2_position,
                'total_runs': safe_int(partnership.get('totalruns')),
                'total_balls': safe_int(partnership.get('totalballs')),
                'is_adjacent': (
                    abs(bat1_position - bat2_position) == 1 
                    if bat1_position and bat2_position else None
                ),
                'fetched_at': datetime.now()
            }
            all_partnerships.append(partnership_record)

    return all_batsmen, all_bowlers, all_partnerships

def extract_scorecard_metadata(scorecard_data: dict, match_id: int) -> Optional[dict]:
    """Extract match completion status and result from scorecard."""
    if not scorecard_data:
        return None
    
    metadata = {
        'match_id': match_id,
        'is_match_complete': scorecard_data.get('ismatchcomplete', False),
        'match_status': scorecard_data.get('status'),
    }
    
    return metadata

def flatten_json(
    data: dict,
    fetch_player_data: bool = True,
    fetch_commentary: bool = True
) -> Dict[str, pd.DataFrame]:
    """
    Flatten nested JSON response into normalized DataFrames.
    Includes match info, venues, teams, officials, series, scorecards, partnerships, and commentary.
    """
    all_matches = []
    all_venues = []
    all_teams = []
    all_officials = []
    all_series = []
    all_batsmen = []
    all_bowlers = []
    all_partnerships = []  # NEW
    all_scorecard_metadata = []
    all_commentary = []

    # Handle both possible JSON structures
    matches_list = []
    match_scores_dict = {}
    
    if isinstance(data, list):
        matches_list = data
    elif isinstance(data, dict) and "typeMatches" in data:
        type_matches = data.get("typeMatches", [])
        for type_match in type_matches:
            for series_match in type_match.get("seriesMatches", []):
                series_wrapper = series_match.get("seriesAdWrapper")
                if series_wrapper and "matches" in series_wrapper:
                    for match in series_wrapper.get("matches", []):
                        match_info = match.get("matchInfo", {})
                        match_score = match.get("matchScore", {})
                        matches_list.append(match_info)
                        if match_info.get('matchId') or match_info.get('matchid'):
                            mid = match_info.get('matchId') or match_info.get('matchid')
                            match_scores_dict[mid] = match_score
    elif isinstance(data, dict) and "matches" in data:
        matches_list = data.get("matches", [])

    logger.info(f"Found {len(matches_list)} matches to process")

    for idx, match in enumerate(matches_list):
        match_id = safe_get(match, 'matchid', 'matchId')
        if not match_id:
            logger.warning("Skipping match without matchId")
            continue

        # Extract match score data if available
        match_score = match_scores_dict.get(match_id, {})
        team1_score = match_score.get('team1Score', {})
        team2_score = match_score.get('team2Score', {})
        
        team1_inngs1 = team1_score.get('inngs1', {})
        team1_inngs2 = team1_score.get('inngs2', {})
        team2_inngs1 = team2_score.get('inngs1', {})
        team2_inngs2 = team2_score.get('inngs2', {})

        # Match Information
        match_info = {
            'match_id': match_id,
            'series_id': safe_get(match, 'seriesid', 'seriesId'),
            'series_name': safe_get(match, 'seriesname', 'seriesName'),
            'match_desc': safe_get(match, 'matchdesc', 'matchDesc'),
            'match_format': safe_get(match, 'matchformat', 'matchFormat'),
            'runs': safe_get(match, 'runs'),
            'state': match.get('state'),
            'status': match.get('status'),
            'curr_bat_team_id': safe_get(match, 'currbatteamid', 'currBatTeamId'),
            'toss_status': safe_get(match, 'tossstatus', 'tossStatus'),
            'team1_inngs1_runs': safe_int(team1_inngs1.get('runs')),
            'team1_inngs1_wickets': safe_int(team1_inngs1.get('wickets')),
            'team1_inngs1_overs': safe_float(team1_inngs1.get('overs')),
            'team1_inngs1_declared': team1_inngs1.get('isDeclared', False),
            'team1_inngs2_runs': safe_int(team1_inngs2.get('runs')),
            'team1_inngs2_wickets': safe_int(team1_inngs2.get('wickets')),
            'team1_inngs2_overs': safe_float(team1_inngs2.get('overs')),
            'team1_inngs2_declared': team1_inngs2.get('isDeclared', False),
            'team2_inngs1_runs': safe_int(team2_inngs1.get('runs')),
            'team2_inngs1_wickets': safe_int(team2_inngs1.get('wickets')),
            'team2_inngs1_overs': safe_float(team2_inngs1.get('overs')),
            'team2_inngs1_declared': team2_inngs1.get('isDeclared', False),
            'team2_inngs2_runs': safe_int(team2_inngs2.get('runs')),
            'team2_inngs2_wickets': safe_int(team2_inngs2.get('wickets')),
            'team2_inngs2_overs': safe_float(team2_inngs2.get('overs')),
            'team2_inngs2_declared': team2_inngs2.get('isDeclared', False)
        }
        all_matches.append(match_info)

        # Venue Information
        venue_info_dict = safe_get(match, 'venueinfo', 'venueInfo', 'venue')
        if venue_info_dict:
            venue = extract_venue_info(venue_info_dict, match_id)
            if venue:
                all_venues.append(venue)

        # Teams Information
        team1_dict = match.get('team1')
        if team1_dict:
            team1 = extract_team_info(team1_dict, match_id, 'team1')
            if team1:
                all_teams.append(team1)

        team2_dict = match.get('team2')
        if team2_dict:
            team2 = extract_team_info(team2_dict, match_id, 'team2')
            if team2:
                all_teams.append(team2)

        # Officials
        for i in [1, 2, 3]:
            umpire = match.get(f'umpire{i}')
            if umpire:
                official = extract_official_info(umpire, match_id, f'umpire{i}')
                if official:
                    all_officials.append(official)

        referee = match.get('referee')
        if referee:
            official = extract_official_info(referee, match_id, 'referee')
            if official:
                all_officials.append(official)

        # Series
        series_id = safe_get(match, 'seriesid', 'seriesId')
        if series_id:
            all_series.append({
                'series_id': series_id,
                'series_name': safe_get(match, 'seriesname', 'seriesName'),
                'match_type': safe_get(match, 'matchtype', 'matchType'),
                'series_type': safe_get(match, 'seriestype', 'seriesType'),
                'match_id': match_id,
                'series_start_dt': safe_get(match, 'seriesstartdt', 'seriesStartDt'),
                'series_end_dt': safe_get(match, 'seriesenddt', 'seriesEndDt'),
                'fetched_at': datetime.now()
            })

        # Scorecard (Player Stats and Partnerships)
        if fetch_player_data:
            logger.info(f"  Fetching player stats for match {match_id} ({idx+1}/{len(matches_list)})")
            scorecard_url = API_ENDPOINTS['match_scorecard'].format(match_id=match_id)
            scorecard_data = fetch_api_data(scorecard_url, f"scorecard for match {match_id}")

            if scorecard_data:
                try:
                    batsmen, bowlers, partnerships = extract_player_stats_and_partnerships(scorecard_data, match_id)
                    all_batsmen.extend(batsmen)
                    all_bowlers.extend(bowlers)
                    all_partnerships.extend(partnerships)  # NEW
                    logger.info(f"    → {len(batsmen)} batsmen, {len(bowlers)} bowlers, {len(partnerships)} partnerships")

                    metadata = extract_scorecard_metadata(scorecard_data, match_id)
                    if metadata:
                        all_scorecard_metadata.append(metadata)
                except Exception as e:
                    logger.error(f"    ✗ Error extracting player stats: {e}")
            else:
                logger.warning(f"    ✗ No scorecard data returned")

            time.sleep(0.5)

        # Commentary
        if fetch_commentary:
            logger.info(f"  Fetching commentary for match {match_id}")
            commentary_url = API_ENDPOINTS['match_commentary'].format(match_id=match_id)
            commentary_data = fetch_api_data(commentary_url, f"commentary for match {match_id}")

            if commentary_data:
                try:
                    commentary_lines = extract_commentary_data(commentary_data, match_id)
                    all_commentary.extend(commentary_lines)
                    logger.info(f"    → {len(commentary_lines)} commentary lines")
                except Exception as e:
                    logger.error(f"    ✗ Error extracting commentary: {e}")
            else:
                logger.warning(f"    ✗ No commentary data returned")

            time.sleep(0.5)

    # Build DataFrames
    logger.info("Building DataFrames from collected data")

    dataframes = {
        'live_match_info': pd.DataFrame(all_matches),
        'live_venues': pd.DataFrame(all_venues),
        'live_teams': pd.DataFrame(all_teams),
        'live_officials': pd.DataFrame(all_officials),
        'live_series': pd.DataFrame(all_series),
        'live_batting_stats': pd.DataFrame(all_batsmen),
        'live_bowling_stats': pd.DataFrame(all_bowlers),
        'live_partnerships': pd.DataFrame(all_partnerships),  # NEW
        'live_scorecard_metadata': pd.DataFrame(all_scorecard_metadata),
        'live_commentary': pd.DataFrame(all_commentary)
    }

    # Log row counts
    for table_name, df in dataframes.items():
        logger.info(f"{table_name}: {len(df)} rows")

    return dataframes

@contextmanager
def get_db_engine():
    """Context manager for database engine."""
    connection_string = (
        f"mysql+mysqlconnector://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )
    engine = create_engine(connection_string, pool_pre_ping=True)
    try:
        yield engine
    finally:
        engine.dispose()

def fetch_and_store_all(append_mode: bool = False, fetch_player_data: bool = True, fetch_commentary: bool = True, debug_mode: bool = False):
    """
    Fetch live cricket data from API and store in MySQL database.
    
    Args:
        append_mode: If True, append to existing tables. If False, replace tables.
        fetch_player_data: Whether to fetch detailed player statistics (slower)
        fetch_commentary: Whether to fetch ball-by-ball commentary
        debug_mode: If True, save API responses to files for debugging
    """
    logger.info("=" * 60)
    logger.info("Starting Cricbuzz Data Pipeline")
    logger.info("=" * 60)
    
    # Fetch live matches
    data = fetch_api_data(API_ENDPOINTS['live_matches'], "live matches", save_debug=debug_mode)
    if not data:
        logger.error("Failed to fetch live matches data")
        return

    # Flatten and structure data
    try:
        tables = flatten_json(data, fetch_player_data=fetch_player_data, fetch_commentary=fetch_commentary)
    except Exception as e:
        logger.error(f"Error flattening JSON: {e}", exc_info=True)
        return

    # Connect to MySQL and store data
    try:
        with get_db_engine() as engine:
            logger.info("Connected to MySQL database")

            if_exists_mode = 'append' if append_mode else 'replace'
            
            for table_name, df in tables.items():
                if not df.empty:
                    # Remove duplicates based on natural keys
                    if table_name == 'live_match_info':
                        df = df.drop_duplicates(subset=['match_id'], keep='last')
                    elif table_name == 'live_venues':
                        df = df.drop_duplicates(subset=['match_id', 'venue_id'], keep='last')
                    elif table_name == 'live_teams':
                        df = df.drop_duplicates(subset=['match_id', 'team_id'], keep='last')
                    elif table_name == 'live_batting_stats':
                        df = df.drop_duplicates(subset=['match_id', 'innings_id', 'batsman_id'], keep='last')
                    elif table_name == 'live_bowling_stats':
                        df = df.drop_duplicates(subset=['match_id', 'innings_id', 'bowler_id'], keep='last')
                    elif table_name == 'live_partnerships':  # NEW
                        df = df.drop_duplicates(subset=['match_id', 'innings_id', 'bat1_id', 'bat2_id'], keep='last')
                    elif table_name == 'live_commentary':
                        df = df.drop_duplicates(subset=['match_id', 'innings', 'timestamp', 'ball_number'], keep='last')
                    else:
                        df = df.drop_duplicates()
                    
                    df.to_sql(
                        table_name, 
                        con=engine, 
                        if_exists=if_exists_mode, 
                        index=False,
                        chunksize=1000
                    )
                    logger.info(f"✓ Stored {len(df)} rows in '{table_name}' table")
                else:
                    logger.warning(f"⚠ No data for '{table_name}' table. Skipped.")

            logger.info("=" * 60)
            logger.info("Data pipeline completed successfully!")
            logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Database error: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    # Run with all features enabled
    fetch_and_store_all(
        append_mode=False, 
        fetch_player_data=True, 
        fetch_commentary=True,
        debug_mode=False
    )