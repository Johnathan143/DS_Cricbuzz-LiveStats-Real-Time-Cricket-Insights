import requests
import pymysql
import time

# Database config
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'database': 'cricbuzz2',
    'password': 'Root',
    'port': 3306
}

# API config
headers = {
    "X-RapidAPI-Key": "446f502ff3msh08667e5149ab515p181e4cjsnf41ba001f3da",
    "X-RapidAPI-Host": "cricbuzz-cricket.p.rapidapi.com"
}

def setup_database():
    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    # Drop the existing table to recreate with new schema
    cursor.execute('DROP TABLE IF EXISTS player_stats')
    print("🗑️ Dropped existing player_stats table")
    
    # Create new table with all columns
    cursor.execute('''
    CREATE TABLE player_stats (
        player_id INT,
        player_name VARCHAR(255),
        format_type VARCHAR(20),
        match_id INT,
        matches INT DEFAULT 0,
        innings INT DEFAULT 0,
        runs INT DEFAULT 0,
        balls INT DEFAULT 0,
        highest VARCHAR(10) DEFAULT '0',
        average DECIMAL(6,2) DEFAULT 0.00,
        strike_rate DECIMAL(6,2) DEFAULT 0.00,
        not_out INT DEFAULT 0,
        wickets INT DEFAULT 0,
        bowling_average DECIMAL(6,2) DEFAULT 0.00,
        bowling_strike_rate DECIMAL(6,2) DEFAULT 0.00,
        economy_rate DECIMAL(6,2) DEFAULT 0.00,
        overs_bowled DECIMAL(8,1) DEFAULT 0.0,
        maidens INT DEFAULT 0,
        runs_conceded INT DEFAULT 0,
        best_bowling VARCHAR(10) DEFAULT '0/0',
        five_wickets INT DEFAULT 0,
        ten_wickets INT DEFAULT 0,
        fours INT DEFAULT 0,
        sixes INT DEFAULT 0,
        ducks INT DEFAULT 0,
        fifties INT DEFAULT 0,
        hundreds INT DEFAULT 0,
        two_hundreds INT DEFAULT 0,
        three_hundreds INT DEFAULT 0,
        four_hundreds INT DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (player_id, format_type),
        INDEX idx_player_name (player_name),
        INDEX idx_format (format_type),
        INDEX idx_runs (runs DESC),
        INDEX idx_wickets (wickets DESC)
    )
    ''')
    
    conn.commit()
    conn.close()
    print("✅ Database recreated with new schema")

def get_all_format_stats(batting_data):
    headers = batting_data.get("headers", [])
    
    # Skip ROWHEADER, get actual format columns
    format_headers = [h for h in headers[1:] if h.upper() not in ['ROWHEADER']]
    
    # Create stats for each format
    all_stats = {}
    for i, format_name in enumerate(format_headers, 1):  # Start from index 1
        all_stats[format_name] = {}
    
    # Extract stats for each row
    for row in batting_data.get("values", []):
        values = row.get("values", [])
        if len(values) > len(format_headers):
            stat_name = values[0].lower()
            
            # For each format, get the value
            for i, format_name in enumerate(format_headers, 1):
                if len(values) > i:
                    value = values[i]
                    
                    if stat_name == "matches":
                        all_stats[format_name]['matches'] = int(value) if value.isdigit() else 0
                    elif stat_name == "innings":
                        all_stats[format_name]['innings'] = int(value) if value.isdigit() else 0
                    elif stat_name == "runs":
                        all_stats[format_name]['runs'] = int(value) if value.isdigit() else 0
                    elif stat_name == "balls":
                        all_stats[format_name]['balls'] = int(value) if value.isdigit() else 0
                    elif stat_name == "highest":
                        all_stats[format_name]['highest'] = str(value)
                    elif stat_name == "average":
                        all_stats[format_name]['average'] = float(value) if value.replace('.','').replace('-','').isdigit() else 0.0
                    elif stat_name == "sr":
                        all_stats[format_name]['strike_rate'] = float(value) if value.replace('.','').replace('-','').isdigit() else 0.0
                    elif stat_name == "not out":
                        all_stats[format_name]['not_out'] = int(value) if value.isdigit() else 0
                    elif stat_name == "fours":
                        all_stats[format_name]['fours'] = int(value) if value.isdigit() else 0
                    elif stat_name == "sixes":
                        all_stats[format_name]['sixes'] = int(value) if value.isdigit() else 0
                    elif stat_name == "ducks":
                        all_stats[format_name]['ducks'] = int(value) if value.isdigit() else 0
                    elif stat_name == "50s":
                        all_stats[format_name]['fifties'] = int(value) if value.isdigit() else 0
                    elif stat_name == "100s":
                        all_stats[format_name]['hundreds'] = int(value) if value.isdigit() else 0
                    elif stat_name == "200s":
                        all_stats[format_name]['two_hundreds'] = int(value) if value.isdigit() else 0
                    elif stat_name == "300s":
                        all_stats[format_name]['three_hundreds'] = int(value) if value.isdigit() else 0
                    elif stat_name == "400s":
                        all_stats[format_name]['four_hundreds'] = int(value) if value.isdigit() else 0
    
    return all_stats

def get_bowling_stats(bowling_data):
    headers = bowling_data.get("headers", [])
    
    # Skip ROWHEADER, get actual format columns
    format_headers = [h for h in headers[1:] if h.upper() not in ['ROWHEADER']]
    
    # Create bowling stats for each format
    bowling_stats = {}
    for i, format_name in enumerate(format_headers, 1):
        bowling_stats[format_name] = {}
    
    # Extract bowling stats for each row
    for row in bowling_data.get("values", []):
        values = row.get("values", [])
        if len(values) > len(format_headers):
            stat_name = values[0].lower()
            
            # For each format, get the value
            for i, format_name in enumerate(format_headers, 1):
                if len(values) > i:
                    value = values[i]
                    
                    if stat_name == "wickets":
                        bowling_stats[format_name]['wickets'] = int(value) if value.isdigit() else 0
                    elif stat_name == "avg":  # Changed from "average" to "avg"
                        bowling_stats[format_name]['bowling_average'] = float(value) if value.replace('.','').replace('-','').isdigit() else 0.0
                    elif stat_name == "sr":
                        bowling_stats[format_name]['bowling_strike_rate'] = float(value) if value.replace('.','').replace('-','').isdigit() else 0.0
                    elif stat_name == "eco":  # Changed from "econ" to "eco"
                        bowling_stats[format_name]['economy_rate'] = float(value) if value.replace('.','').replace('-','').isdigit() else 0.0
                    elif stat_name == "balls":
                        # Calculate overs from balls (6 balls = 1 over)
                        balls_count = int(value) if value.isdigit() else 0
                        bowling_stats[format_name]['overs_bowled'] = round(balls_count / 6, 1)
                    elif stat_name == "maidens":
                        bowling_stats[format_name]['maidens'] = int(value) if value.isdigit() else 0
                    elif stat_name == "runs":
                        bowling_stats[format_name]['runs_conceded'] = int(value) if value.isdigit() else 0
                    elif stat_name == "bbi":  # Best bowling in innings
                        bowling_stats[format_name]['best_bowling'] = str(value) if value != "-/-" else "0/0"
                    elif stat_name == "5w":
                        bowling_stats[format_name]['five_wickets'] = int(value) if value.isdigit() else 0
                    elif stat_name == "10w":
                        bowling_stats[format_name]['ten_wickets'] = int(value) if value.isdigit() else 0
    
    return bowling_stats
    

def merge_stats(batting_stats, bowling_stats):
    merged_stats = {}
    
    # Get all format types from both batting and bowling
    all_formats = set(list(batting_stats.keys()) + list(bowling_stats.keys()))
    
    for format_type in all_formats:
        merged_stats[format_type] = {}
        
        # Add batting stats
        if format_type in batting_stats:
            merged_stats[format_type].update(batting_stats[format_type])
        
        # Add bowling stats
        if format_type in bowling_stats:
            merged_stats[format_type].update(bowling_stats[format_type])
    
    return merged_stats

def save_player_stats(player_id, player_name, all_format_stats):
    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    for format_type, stats in all_format_stats.items():
        cursor.execute('''
        INSERT INTO player_stats (
            player_id, player_name, format_type, match_id, matches, innings, runs, balls, highest,
            average, strike_rate, not_out, wickets, bowling_average, bowling_strike_rate,
            economy_rate, overs_bowled, maidens, runs_conceded, best_bowling,
            five_wickets, ten_wickets, fours, sixes, ducks, fifties, hundreds,
            two_hundreds, three_hundreds, four_hundreds
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON DUPLICATE KEY UPDATE 
            player_name=%s, matches=%s, innings=%s, runs=%s, balls=%s, highest=%s,
            average=%s, strike_rate=%s, not_out=%s, wickets=%s, bowling_average=%s,
            bowling_strike_rate=%s, economy_rate=%s, overs_bowled=%s, maidens=%s,
            runs_conceded=%s, best_bowling=%s, five_wickets=%s, ten_wickets=%s,
            fours=%s, sixes=%s, ducks=%s, fifties=%s, hundreds=%s, two_hundreds=%s,
            three_hundreds=%s, four_hundreds=%s, updated_at=NOW()
        ''', (
            # Insert values (29 values)
            player_id, player_name, format_type,
            stats.get('matches', 0), stats.get('innings', 0), stats.get('runs', 0),
            stats.get('balls', 0), stats.get('highest', '0'), stats.get('average', 0.0),
            stats.get('strike_rate', 0.0), stats.get('not_out', 0), stats.get('wickets', 0),
            stats.get('bowling_average', 0.0), stats.get('bowling_strike_rate', 0.0),
            stats.get('economy_rate', 0.0), stats.get('overs_bowled', 0.0),
            stats.get('maidens', 0), stats.get('runs_conceded', 0),
            stats.get('best_bowling', '0/0'), stats.get('five_wickets', 0),
            stats.get('ten_wickets', 0), stats.get('fours', 0), stats.get('sixes', 0),
            stats.get('ducks', 0), stats.get('fifties', 0), stats.get('hundreds', 0),
            stats.get('two_hundreds', 0), stats.get('three_hundreds', 0),
            stats.get('four_hundreds', 0),
            # Update values (27 values)
            player_name, stats.get('matches', 0), stats.get('innings', 0), stats.get('runs', 0),
            stats.get('balls', 0), stats.get('highest', '0'), stats.get('average', 0.0),
            stats.get('strike_rate', 0.0), stats.get('not_out', 0), stats.get('wickets', 0),
            stats.get('bowling_average', 0.0), stats.get('bowling_strike_rate', 0.0),
            stats.get('economy_rate', 0.0), stats.get('overs_bowled', 0.0),
            stats.get('maidens', 0), stats.get('runs_conceded', 0),
            stats.get('best_bowling', '0/0'), stats.get('five_wickets', 0),
            stats.get('ten_wickets', 0), stats.get('fours', 0), stats.get('sixes', 0),
            stats.get('ducks', 0), stats.get('fifties', 0), stats.get('hundreds', 0),
            stats.get('two_hundreds', 0), stats.get('three_hundreds', 0),
            stats.get('four_hundreds', 0)
        ))
    
    conn.commit()
    conn.close()

def main():
    setup_database()
    
    player_ids = [25, 104, 1413, 38, 102, 101, 35, 213, 29, 576, 27, 265, 247, 240, 105, 34, 36, 370, 3864, 3531]
    
    for player_id in player_ids:
        try:
            # Get player info
            info_url = f"https://cricbuzz-cricket.p.rapidapi.com/stats/v1/player/{player_id}"
            info_response = requests.get(info_url, headers=headers)
            
            # Get batting stats
            batting_url = f"https://cricbuzz-cricket.p.rapidapi.com/stats/v1/player/{player_id}/batting"
            batting_response = requests.get(batting_url, headers=headers)
            
            # Get bowling stats (Fixed the URL - was using batting URL for bowling)
            bowling_url = f"https://cricbuzz-cricket.p.rapidapi.com/stats/v1/player/{player_id}/bowling"
            bowling_response = requests.get(bowling_url, headers=headers)
            
            # Debug the responses
            print(f"Player {player_id} - Info: {info_response.status_code}, Batting: {batting_response.status_code}, Bowling: {bowling_response.status_code}")
            
            if info_response.status_code == 200:
                player_info = info_response.json()
                player_name = player_info.get("name", "Unknown")
                
                # Initialize stats
                batting_stats = {}
                bowling_stats = {}
                
                # Get batting stats
                if batting_response.status_code == 200:
                    batting_data = batting_response.json()
                    batting_stats = get_all_format_stats(batting_data)
                
                # Get bowling stats
                if bowling_response.status_code == 200:
                    bowling_data = bowling_response.json()
                    bowling_stats = get_bowling_stats(bowling_data)
                
                # Merge batting and bowling stats
                all_format_stats = merge_stats(batting_stats, bowling_stats)
                
                if all_format_stats:
                    save_player_stats(player_id, player_name, all_format_stats)
                    
                    # Show summary of all formats
                    formats_summary = []
                    for fmt, stats in all_format_stats.items():
                        runs = stats.get('runs', 0)
                        wickets = stats.get('wickets', 0)
                        formats_summary.append(f"{fmt}: {runs} runs, {wickets} wickets")
                    
                    print(f"✅ {player_name}: {', '.join(formats_summary)}")
                else:
                    print(f"⚠️ No stats available for {player_name}")
            else:
                print(f"❌ Failed for player {player_id}")
                if info_response.status_code != 200:
                    print(f"   Info API error: {info_response.text[:100]}")
            
            time.sleep(0.5)  # Rate limit
            
        except Exception as e:
            print(f"❌ Error with player {player_id}: {e}")
    
    print("Done!")

if __name__ == "__main__":
    main()