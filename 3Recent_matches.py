import requests
import pandas as pd
from sqlalchemy import create_engine
import schedule
import time
from datetime import datetime

DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'database': 'cricbuzz2',
    'password': 'Root',
    'port': 3306
}

def extract_score(score_dict):
    """Extract score information from score dictionary"""
    if not score_dict:
        return "N/A"
    
    innings = list(score_dict.values())
    if not innings:
        return "N/A"
    
    latest = innings[-1]
    runs = latest.get('runs', 'N/A')
    wickets = latest.get('wickets', 'N/A')
    overs = latest.get('overs', 'N/A')
    return f"{runs}/{wickets} ({overs} ov)"

def fetch_and_store():
    print(f"[{datetime.now()}] Fetching recent matches...")
    
    url = "https://cricbuzz-cricket.p.rapidapi.com/matches/v1/recent"
    headers = {
        "x-rapidapi-key": "f493d43d51mshd2b86741565d751p1ef6a6jsn5103eba1f487",
        "x-rapidapi-host": "cricbuzz-cricket.p.rapidapi.com"
    }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        recent_matches = response.json()
    except Exception as e:
        print(f"Error fetching data: {e}")
        return
    
    rows = []
    
    for type_match in recent_matches.get('typeMatches', []):
        match_type = type_match.get('matchType', 'N/A')
        
        for series in type_match.get('seriesMatches', []):
            series_wrapper = series.get('seriesAdWrapper')
            if not series_wrapper:
                continue
                
            series_name = series_wrapper.get('seriesName', 'N/A')
            
            for match in series_wrapper.get('matches', []):
                info = match.get('matchInfo', {})
                score = match.get('matchScore', {})
                
                team1 = info.get('team1', {}).get('teamName', 'N/A')
                team2 = info.get('team2', {}).get('teamName', 'N/A')
                
                venue_info = info.get('venueInfo', {})
                ground = venue_info.get('ground', 'N/A')
                city = venue_info.get('city', 'N/A')
                venue = f"{ground}, {city}"
                
                start_time = info.get('startDate', 'N/A')
                if start_time != 'N/A':
                    try:
                        timestamp = int(start_time) / 1000
                        start_time = pd.to_datetime(timestamp, unit='s')
                    except:
                        pass
                
                status = info.get('status', 'N/A')
                
                team1_score = extract_score(score.get('team1Score'))
                team2_score = extract_score(score.get('team2Score'))
                
                rows.append({
                    'Match_Type': match_type,
                    'Series_Name': series_name,
                    'Team_1': team1,
                    'Team_2': team2,
                    'Team_1_Score': team1_score,
                    'Team_2_Score': team2_score,
                    'Venue': venue,
                    'Start_Time': start_time,
                    'Status': status
                })
    
    if not rows:
        print("No recent matches found")
        return
    
    df = pd.DataFrame(rows)
    
    try:
        engine = create_engine(f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
        with engine.connect() as connection:
            df.to_sql('recent_matches', con=connection, if_exists='replace', index=False)
        print(f"Data updated in database. {len(df)} matches stored.")
        
        print("\nRecent matches sample:")
        print(df[['Team_1', 'Team_2', 'Status', 'Team_1_Score', 'Team_2_Score']].head())
        
    except Exception as e:
        print(f"Error storing data to database: {e}")

if __name__ == "__main__":
    fetch_and_store()

    schedule.every(6).hours.do(fetch_and_store)
    print("Service started. Running every 6 hours. Press Ctrl+C to stop.")
    
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)
    except KeyboardInterrupt:
        print("\nService stopped.")
