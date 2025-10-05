import requests
import pymysql
import time
import json

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'database': 'cricbuzz2',
    'password': 'Root',
    'port': 3306
}

# API configuration
headers = {
    "X-RapidAPI-Key": "b0f0df8844mshebee6ecac358d75p1fbe7ajsn5ae505036ae4",
    "X-RapidAPI-Host": "cricbuzz-cricket.p.rapidapi.com"
}

def setup_database():
    """Create database tables if they don't exist"""
    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # First check if table exists and what columns it has
    cursor.execute("SHOW TABLES LIKE 'venues'")
    table_exists = cursor.fetchone()
    
    if not table_exists:
        # Create new table with all columns
        cursor.execute('''
        CREATE TABLE venues (
            venue_id INT PRIMARY KEY,
            venue_name VARCHAR(255),
            city VARCHAR(100),
            country VARCHAR(100),
            timezone VARCHAR(10),
            capacity INT,
            ends TEXT,
            home_team VARCHAR(100),
            image_url TEXT,
            total_matches INT,
            matches_won_batting_first INT,
            matches_won_bowling_first INT,
            avg_first_inns INT,
            avg_second_inns INT,
            highest_total VARCHAR(255),
            lowest_total VARCHAR(255),
            highest_chased VARCHAR(255),
            lowest_defended VARCHAR(255)
        )
        ''')
    else:
        # Add missing columns to existing table
        try:
            cursor.execute("ALTER TABLE venues ADD COLUMN timezone VARCHAR(10)")
        except:
            pass  # Column might already exist
        try:
            cursor.execute("ALTER TABLE venues ADD COLUMN ends TEXT")
        except:
            pass
        try:
            cursor.execute("ALTER TABLE venues ADD COLUMN home_team VARCHAR(100)")
        except:
            pass
        try:
            cursor.execute("ALTER TABLE venues ADD COLUMN image_url TEXT")
        except:
            pass
        try:
            cursor.execute("ALTER TABLE venues ADD COLUMN total_matches INT")
        except:
            pass
        try:
            cursor.execute("ALTER TABLE venues ADD COLUMN matches_won_batting_first INT")
        except:
            pass
        try:
            cursor.execute("ALTER TABLE venues ADD COLUMN matches_won_bowling_first INT")
        except:
            pass
        try:
            cursor.execute("ALTER TABLE venues ADD COLUMN avg_first_inns INT")
        except:
            pass
        try:
            cursor.execute("ALTER TABLE venues ADD COLUMN avg_second_inns INT")
        except:
            pass
        try:
            cursor.execute("ALTER TABLE venues ADD COLUMN highest_total VARCHAR(255)")
        except:
            pass
        try:
            cursor.execute("ALTER TABLE venues ADD COLUMN lowest_total VARCHAR(255)")
        except:
            pass
        try:
            cursor.execute("ALTER TABLE venues ADD COLUMN highest_chased VARCHAR(255)")
        except:
            pass
        try:
            cursor.execute("ALTER TABLE venues ADD COLUMN lowest_defended VARCHAR(255)")
        except:
            pass
    
    conn.commit()
    conn.close()
    print("✅ Database setup complete")

def get_venue_basic_data(venue_id):
    """Fetch basic venue data from first API endpoint"""
    try:
        url = f"https://cricbuzz-cricket.p.rapidapi.com/venues/v1/{venue_id}"
        response = requests.get(url, headers=headers, timeout=10)
        
        print(f"   Basic Data Status Code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            return data
        else:
            print(f"   API error for venue {venue_id} basic data: {response.status_code}")
            return None
            
    except Exception as e:
        print(f"   Error fetching basic venue {venue_id}: {e}")
        return None

def get_venue_stats_data(venue_id):
    """Fetch venue stats data from second API endpoint"""
    try:
        url = f"https://cricbuzz-cricket.p.rapidapi.com/stats/v1/venue/{venue_id}"
        response = requests.get(url, headers=headers, timeout=10)
        
        print(f"   Stats Data Status Code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            return data
        else:
            print(f"   API error for venue {venue_id} stats data: {response.status_code}")
            return None
            
    except Exception as e:
        print(f"   Error fetching stats venue {venue_id}: {e}")
        return None

def parse_venue_stats(venue_stats):
    """Parse venue statistics from the stats API response"""
    stats = {}
    
    if not venue_stats or 'venueStats' not in venue_stats:
        return stats
    
    for stat in venue_stats['venueStats']:
        key = stat.get('key', '').lower()
        value = stat.get('value', '')
        
        if 'total matches' in key:
            try:
                stats['total_matches'] = int(value)
            except ValueError:
                stats['total_matches'] = 0
                
        elif 'matches won batting first' in key:
            try:
                stats['matches_won_batting_first'] = int(value)
            except ValueError:
                stats['matches_won_batting_first'] = 0
                
        elif 'matches won bowling first' in key:
            try:
                stats['matches_won_bowling_first'] = int(value)
            except ValueError:
                stats['matches_won_bowling_first'] = 0
                
        elif 'avg. scores recorded' in key:
            # Parse "1st inns-310\n2nd inns-337\n3rd inns-337\n4th inns-159"
            try:
                lines = value.split('\n')
                for line in lines:
                    if '1st inns-' in line:
                        stats['avg_first_inns'] = int(line.split('-')[1])
                    elif '2nd inns-' in line:
                        stats['avg_second_inns'] = int(line.split('-')[1])
            except (ValueError, IndexError):
                stats['avg_first_inns'] = 0
                stats['avg_second_inns'] = 0
                
        elif 'highest total recorded' in key:
            stats['highest_total'] = value
            
        elif 'lowest total recorded' in key:
            stats['lowest_total'] = value
            
        elif 'highest score chased' in key:
            stats['highest_chased'] = value
            
        elif 'lowest score defended' in key:
            stats['lowest_defended'] = value
    
    return stats

def save_venue(venue_id, basic_data, stats_data):
    """Save combined venue data to database"""
    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # Extract basic venue information
    venue_name = basic_data.get('ground', 'Unknown')
    city = basic_data.get('city', '')
    country = basic_data.get('country', '')
    timezone = basic_data.get('timezone', '')
    ends = basic_data.get('ends', '')
    home_team = basic_data.get('homeTeam', '')
    image_url = basic_data.get('imageUrl', '')

    # Parse capacity
    capacity_raw = basic_data.get('capacity', '0')
    if isinstance(capacity_raw, str):
        try:
            import re
            numbers = re.findall(r'[\d,]+', capacity_raw)
            if numbers:
                capacity = int(numbers[0].replace(',', ''))
            else:
                capacity = 0
        except (ValueError, AttributeError):
            capacity = 0
    else:
        capacity = int(capacity_raw) if capacity_raw else 0

    # Parse statistics
    stats = parse_venue_stats(stats_data)
    
    print(f"   Saving - ID: {venue_id}, Name: {venue_name}, City: {city}, Country: {country}")
    print(f"   Stats - Total Matches: {stats.get('total_matches', 0)}")

    cursor.execute('''
    INSERT INTO venues (
        venue_id, venue_name, city, country, timezone, capacity, ends, home_team, image_url,
        total_matches, matches_won_batting_first, matches_won_bowling_first,
        avg_first_inns, avg_second_inns, highest_total, lowest_total,
        highest_chased, lowest_defended
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE 
        venue_name=VALUES(venue_name),
        city=VALUES(city),
        country=VALUES(country),
        timezone=VALUES(timezone),
        capacity=VALUES(capacity),
        ends=VALUES(ends),
        home_team=VALUES(home_team),
        image_url=VALUES(image_url),
        total_matches=VALUES(total_matches),
        matches_won_batting_first=VALUES(matches_won_batting_first),
        matches_won_bowling_first=VALUES(matches_won_bowling_first),
        avg_first_inns=VALUES(avg_first_inns),
        avg_second_inns=VALUES(avg_second_inns),
        highest_total=VALUES(highest_total),
        lowest_total=VALUES(lowest_total),
        highest_chased=VALUES(highest_chased),
        lowest_defended=VALUES(lowest_defended)
    ''', (
        venue_id, venue_name, city, country, timezone, capacity, ends, home_team, image_url,
        stats.get('total_matches'), stats.get('matches_won_batting_first'), 
        stats.get('matches_won_bowling_first'), stats.get('avg_first_inns'), 
        stats.get('avg_second_inns'), stats.get('highest_total'), 
        stats.get('lowest_total'), stats.get('highest_chased'), 
        stats.get('lowest_defended')
    ))
    
    conn.commit()
    conn.close()

def main():
    setup_database()
    
    venue_ids = [50, 80, 11, 154, 380, 81, 485, 27, 851, 76, 51, 511, 87, 31, 335, 512, 40, 485]
    
    print(f"Processing {len(venue_ids)} venues...")
    
    for venue_id in venue_ids:
        print(f"\n=== Processing venue {venue_id} ===")
        
        # Get data from both endpoints
        basic_data = get_venue_basic_data(venue_id)
        time.sleep(0.5)  # Small delay between requests
        
        stats_data = get_venue_stats_data(venue_id)
        time.sleep(0.5)
        
        if basic_data:
            save_venue(venue_id, basic_data, stats_data or {})
            venue_name = basic_data.get('ground', 'Unknown')
            city = basic_data.get('city', 'Unknown')
            print(f"✅ Saved: {venue_name}, {city}")
        else:
            print(f"❌ No basic data returned for venue {venue_id}")
        
        time.sleep(1)  # Delay between venues
    
    print("\nProcessing complete!")
    
    # Display results
    print("\n=== Database Contents ===")
    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("SELECT venue_id, venue_name, city, country, total_matches FROM venues ORDER BY venue_id")
    rows = cursor.fetchall()
    for row in rows:
        print(f"Venue {row[0]}: {row[1]}, {row[2]}, {row[3]} - {row[4]} matches")
    conn.close()

if __name__ == "__main__":
    main()