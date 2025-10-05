import requests
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import logging
import time
from typing import List, Dict, Optional
import os
from dotenv import load_dotenv
from collections import defaultdict

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

# Configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'user': os.getenv('DB_USER', 'root'),
    'database': os.getenv('DB_NAME', 'cricbuzz2'),
    'password': os.getenv('DB_PASSWORD', 'Root'),
    'port': int(os.getenv('DB_PORT', 3306))
}

RAPIDAPI_KEY = os.getenv('RAPIDAPI_KEY', "5ce88f231emsh2f38201f4043345p127a9djsndd5f0ca90c70")
RAPIDAPI_HOST = "cricbuzz-cricket.p.rapidapi.com"

HEADERS = {
    "x-rapidapi-key": RAPIDAPI_KEY,
    "x-rapidapi-host": RAPIDAPI_HOST
}

# Match formats: 0=All, 1=Test, 2=ODI, 3=T20I, 4=T20
MATCH_FORMATS = {
    0: 'All',
    1: 'Test',
    2: 'ODI',
    3: 'T20I',
    4: 'T20'
}

# Configuration for strike rate fetching
FETCH_STRIKE_RATES = True  # Set to False to skip strike rate calculation
SR_PLAYER_LIMIT = 15  # Number of top players per year to fetch strike rates for
MAX_MATCHES_TO_FETCH = 30  # Maximum recent matches to process

def fetch_stats(stats_type: str, year: str, format_type: int = 0) -> Optional[dict]:
    """Fetch cricket statistics from Cricbuzz API."""
    url = f"https://cricbuzz-cricket.p.rapidapi.com/stats/v1/topstats/{format_type}"
    querystring = {"statsType": stats_type, "year": year}
    
    try:
        response = requests.get(url, headers=HEADERS, params=querystring, timeout=30)
        response.raise_for_status()
        logger.info(f"✓ Fetched {stats_type} for {year} (format: {MATCH_FORMATS[format_type]})")
        return response.json()
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 500:
            logger.warning(f"⚠ {stats_type} not available for {year}/{MATCH_FORMATS[format_type]}")
        else:
            logger.error(f"✗ Error fetching {stats_type} for {year}: {e}")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"✗ Error fetching {stats_type} for {year}: {e}")
        return None

def fetch_recent_matches() -> List[int]:
    """Fetch recent match IDs."""
    url = "https://cricbuzz-cricket.p.rapidapi.com/matches/v1/recent"
    
    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        match_ids = []
        if 'typeMatches' in data:
            for type_match in data['typeMatches']:
                if 'seriesMatches' in type_match:
                    for series in type_match['seriesMatches']:
                        if 'seriesAdWrapper' in series:
                            matches = series['seriesAdWrapper'].get('matches', [])
                            for match in matches:
                                if 'matchInfo' in match:
                                    match_info = match['matchInfo']
                                    if 'matchId' in match_info:
                                        match_ids.append(match_info['matchId'])
        
        return match_ids
    except Exception as e:
        logger.warning(f"Could not fetch recent matches: {e}")
        return []

def fetch_match_scorecard(match_id: int) -> Optional[dict]:
    """Fetch detailed scorecard for a match."""
    url = f"https://cricbuzz-cricket.p.rapidapi.com/mcenter/v1/{match_id}/scard"
    
    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.debug(f"Could not fetch scorecard for match {match_id}")
        return None

def extract_player_batting_from_scorecard(scorecard: dict) -> Dict[int, Dict]:
    """Extract batting statistics for all players from a scorecard."""
    player_stats = {}
    
    if not scorecard or 'scoreCard' not in scorecard:
        return player_stats
    
    try:
        for inning in scorecard.get('scoreCard', []):
            if 'batTeamDetails' not in inning:
                continue
            
            bat_team = inning['batTeamDetails']
            if 'batsmenData' not in bat_team:
                continue
            
            for bat_id_str, bat_data in bat_team['batsmenData'].items():
                try:
                    # Try to extract player ID from the key
                    player_id = int(bat_id_str) if bat_id_str.isdigit() else None
                    
                    # If player_id is not directly available, try from batId
                    if not player_id and 'batId' in bat_data:
                        player_id = int(bat_data['batId'])
                    
                    if not player_id:
                        continue
                    
                    runs = int(bat_data.get('runs', 0))
                    balls = int(bat_data.get('balls', 0))
                    
                    if player_id not in player_stats:
                        player_stats[player_id] = {
                            'runs': 0,
                            'balls': 0,
                            'innings': 0,
                            'name': bat_data.get('batName', '')
                        }
                    
                    player_stats[player_id]['runs'] += runs
                    player_stats[player_id]['balls'] += balls
                    if balls > 0:
                        player_stats[player_id]['innings'] += 1
                        
                except (ValueError, KeyError, TypeError) as e:
                    continue
    except Exception as e:
        logger.debug(f"Error parsing scorecard: {e}")
    
    return player_stats

def calculate_strike_rates_from_matches(player_ids: List[int]) -> Dict[int, float]:
    """Calculate strike rates for players by fetching match scorecards."""
    logger.info(f"  📊 Fetching match data to calculate strike rates...")
    
    # Get recent matches
    match_ids = fetch_recent_matches()
    
    if not match_ids:
        logger.warning(f"  ⚠ No matches found")
        return {}
    
    # Limit matches to avoid too many API calls
    match_ids = match_ids[:MAX_MATCHES_TO_FETCH]
    logger.info(f"  Processing {len(match_ids)} recent matches...")
    
    # Aggregate player stats across matches
    aggregated_stats = defaultdict(lambda: {'runs': 0, 'balls': 0, 'innings': 0, 'name': ''})
    
    successful_fetches = 0
    for i, match_id in enumerate(match_ids):
        if i > 0 and i % 10 == 0:
            logger.info(f"    Progress: {i}/{len(match_ids)} matches processed...")
        
        scorecard = fetch_match_scorecard(match_id)
        if not scorecard:
            time.sleep(0.3)
            continue
        
        successful_fetches += 1
        player_data = extract_player_batting_from_scorecard(scorecard)
        
        # Aggregate stats for target players
        for player_id, stats in player_data.items():
            if player_id in player_ids:
                aggregated_stats[player_id]['runs'] += stats['runs']
                aggregated_stats[player_id]['balls'] += stats['balls']
                aggregated_stats[player_id]['innings'] += stats['innings']
                if not aggregated_stats[player_id]['name']:
                    aggregated_stats[player_id]['name'] = stats['name']
        
        time.sleep(0.5)  # Rate limiting
    
    logger.info(f"  ✓ Successfully fetched {successful_fetches} scorecards")
    
    # Calculate strike rates
    strike_rates = {}
    for player_id, stats in aggregated_stats.items():
        if stats['balls'] > 0:
            strike_rate = (stats['runs'] / stats['balls']) * 100
            strike_rates[player_id] = round(strike_rate, 2)
            logger.debug(f"    {stats['name']}: SR={strike_rate:.2f} ({stats['runs']} runs, {stats['balls']} balls)")
    
    if strike_rates:
        logger.info(f"  ✓ Calculated strike rates for {len(strike_rates)} players")
    else:
        logger.warning(f"  ⚠ Could not calculate any strike rates")
    
    return strike_rates

def parse_batting_stats(data: dict, year: str, format_type: int, stats_type: str) -> List[Dict]:
    """Parse batting statistics from API response."""
    records = []
    
    if not data or 'values' not in data:
        return records
    
    for player_data in data.get('values', []):
        values = player_data.get('values', [])
        
        if not values or len(values) < 6:
            continue
        
        try:
            record = {
                'year': int(year),
                'format': MATCH_FORMATS[format_type],
                'stats_type': stats_type,
                'player_id': int(values[0]) if values[0] and values[0].isdigit() else None,
                'player_name': values[1] if len(values) > 1 else None,
                'matches': int(values[2]) if len(values) > 2 and values[2].isdigit() else None,
                'innings': int(values[3]) if len(values) > 3 and values[3].isdigit() else None,
                'runs': int(values[4]) if len(values) > 4 and values[4].isdigit() else None,
                'average': float(values[5]) if len(values) > 5 and values[5].replace('.','').replace('-','').isdigit() else None,
                'strike_rate': None,
                'fetched_at': datetime.now()
            }
            records.append(record)
        except (ValueError, IndexError) as e:
            logger.warning(f"Error parsing player data: {e}")
            continue
    
    return records

def enrich_with_strike_rates(batting_records: List[Dict]) -> List[Dict]:
    """Enrich batting records with strike rates from match data."""
    if not batting_records or not FETCH_STRIKE_RATES:
        return batting_records
    
    # Sort by runs and get top players
    sorted_records = sorted(batting_records, key=lambda x: x.get('runs', 0), reverse=True)
    top_players = sorted_records[:SR_PLAYER_LIMIT]
    player_ids = [p['player_id'] for p in top_players if p.get('player_id')]
    
    if not player_ids:
        return batting_records
    
    logger.info(f"  🎯 Targeting top {len(player_ids)} run-scorers for strike rate calculation")
    
    # Fetch strike rates
    strike_rates = calculate_strike_rates_from_matches(player_ids)
    
    # Merge strike rates back into records
    matched = 0
    for record in batting_records:
        player_id = record.get('player_id')
        if player_id and player_id in strike_rates:
            record['strike_rate'] = strike_rates[player_id]
            matched += 1
    
    if matched:
        logger.info(f"  ✅ Successfully added strike rates for {matched}/{len(player_ids)} players")
    else:
        logger.warning(f"  ⚠ Could not match any strike rates")
    
    return batting_records

def parse_bowling_stats(data: dict, year: str, format_type: int, stats_type: str) -> List[Dict]:
    """Parse bowling statistics from API response."""
    records = []
    
    if not data or 'values' not in data:
        return records
    
    for player_data in data.get('values', []):
        values = player_data.get('values', [])
        
        if not values or len(values) < 6:
            continue
        
        try:
            record = {
                'year': int(year),
                'format': MATCH_FORMATS[format_type],
                'stats_type': stats_type,
                'player_id': int(values[0]) if values[0] and values[0].isdigit() else None,
                'player_name': values[1] if len(values) > 1 else None,
                'matches': int(values[2]) if len(values) > 2 and values[2].isdigit() else None,
                'overs': float(values[3]) if len(values) > 3 and values[3].replace('.','').isdigit() else None,
                'wickets': int(values[4]) if len(values) > 4 and values[4].isdigit() else None,
                'average': float(values[5]) if len(values) > 5 and values[5].replace('.','').replace('-','').isdigit() else None,
                'fetched_at': datetime.now()
            }
            records.append(record)
        except (ValueError, IndexError) as e:
            logger.warning(f"Error parsing bowler data: {e}")
            continue
    
    return records

def fetch_all_yearly_stats(start_year: int = 2020, end_year: int = 2025, formats: List[int] = [0]):
    """Fetch all batting and bowling stats for specified years and formats."""
    all_batting_stats = []
    all_bowling_stats = []
    
    logger.info("=" * 60)
    logger.info(f"🏏 Cricket Stats Fetcher")
    logger.info("=" * 60)
    logger.info(f"Years: {start_year} to {end_year}")
    logger.info(f"Formats: {[MATCH_FORMATS[f] for f in formats]}")
    logger.info(f"Strike Rate Calculation: {'ENABLED' if FETCH_STRIKE_RATES else 'DISABLED'}")
    if FETCH_STRIKE_RATES:
        logger.info(f"  → Top {SR_PLAYER_LIMIT} players per year")
        logger.info(f"  → Processing up to {MAX_MATCHES_TO_FETCH} recent matches")
    logger.info("=" * 60)
    
    for year in range(start_year, end_year + 1):
        year_str = str(year)
        logger.info(f"\n📅 Processing Year: {year}")
        
        for format_type in formats:
            format_name = MATCH_FORMATS[format_type]
            logger.info(f"\n  🏆 Format: {format_name}")
            
            # Fetch batting stats (mostRuns)
            batting_data = fetch_stats('mostRuns', year_str, format_type)
            batting_records = []
            if batting_data:
                batting_records = parse_batting_stats(batting_data, year_str, format_type, 'mostRuns')
                logger.info(f"    → Fetched {len(batting_records)} batting records")
                
                # Enrich with strike rates from match data
                if FETCH_STRIKE_RATES and batting_records:
                    batting_records = enrich_with_strike_rates(batting_records)
            
            all_batting_stats.extend(batting_records)
            time.sleep(1)
            
            # Fetch bowling stats
            bowling_data = fetch_stats('mostWickets', year_str, format_type)
            if bowling_data:
                bowling_records = parse_bowling_stats(bowling_data, year_str, format_type, 'mostWickets')
                all_bowling_stats.extend(bowling_records)
                logger.info(f"    → Fetched {len(bowling_records)} bowling records")
            
            time.sleep(1)
    
    return all_batting_stats, all_bowling_stats

def store_stats_in_db(batting_stats: List[Dict], bowling_stats: List[Dict]):
    """Store statistics in MySQL database."""
    try:
        connection_string = (
            f"mysql+mysqlconnector://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
            f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        )
        engine = create_engine(connection_string, pool_pre_ping=True)
        
        logger.info("\n" + "=" * 60)
        logger.info("💾 Storing data in database")
        logger.info("=" * 60)
        
        if batting_stats:
            df_batting = pd.DataFrame(batting_stats)
            df_batting = df_batting.drop_duplicates(
                subset=['year', 'format', 'player_id'], 
                keep='last'
            )
            df_batting.to_sql(
                'yearly_batting_stats', 
                con=engine, 
                if_exists='replace',
                index=False
            )
            logger.info(f"✓ Stored {len(df_batting)} batting stats records")
            
            # Show strike rate coverage
            sr_count = df_batting['strike_rate'].notna().sum()
            sr_percentage = (sr_count/len(df_batting)*100) if len(df_batting) > 0 else 0
            logger.info(f"  → Strike rates: {sr_count}/{len(df_batting)} records ({sr_percentage:.1f}%)")
        
        if bowling_stats:
            df_bowling = pd.DataFrame(bowling_stats)
            df_bowling = df_bowling.drop_duplicates(
                subset=['year', 'format', 'player_id'], 
                keep='last'
            )
            df_bowling.to_sql(
                'yearly_bowling_stats', 
                con=engine, 
                if_exists='replace',
                index=False
            )
            logger.info(f"✓ Stored {len(df_bowling)} bowling stats records")
        
        engine.dispose()
        
    except Exception as e:
        logger.error(f"❌ Database error: {e}")
        raise

if __name__ == "__main__":
    # Fetch stats from 2020 to 2025 for all formats
    batting_stats, bowling_stats = fetch_all_yearly_stats(
        start_year=2020,
        end_year=2025,
        formats=[0]  # 0 = All formats combined
    )
    
    # Store in database
    if batting_stats or bowling_stats:
        store_stats_in_db(batting_stats, bowling_stats)
        
        logger.info("\n" + "=" * 60)
        logger.info("✅ Historical stats fetch completed!")
        logger.info("=" * 60)
        logger.info(f"Total batting records: {len(batting_stats)}")
        logger.info(f"Total bowling records: {len(bowling_stats)}")
        
        if FETCH_STRIKE_RATES:
            sr_count = sum(1 for r in batting_stats if r.get('strike_rate') is not None)
            logger.info(f"Records with strike rate: {sr_count}")
    else:
        logger.warning("⚠ No data fetched")