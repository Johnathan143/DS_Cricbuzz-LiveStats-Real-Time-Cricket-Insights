import requests
import pymysql

# --- DB CONFIG ---
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'database': 'cricbuzz2',
    'password': 'Root',
    'port': 3306
}

conn = pymysql.connect(**DB_CONFIG)
cursor = conn.cursor()
print("✅ Connected to DB")
cursor.execute('''
CREATE TABLE IF NOT EXISTS player_info (
    player_id INT PRIMARY KEY,
    player_name VARCHAR(255),
    role VARCHAR(100),
    dob VARCHAR(100),
    birth_place VARCHAR(255),
    country VARCHAR(100),
    batting_style VARCHAR(100),
    bowling_style VARCHAR(100),
    major_teams TEXT,
    playing_role VARCHAR(100)
)
''')
conn.commit()

# --- Create Table ---
def insert_player_info(player_data):
    player_id = int(player_data.get("id"))
    player_name = player_data.get("name", "")
    role = player_data.get("role", "") 
    dob = player_data.get("DoBFormat", "")
    birth_place = player_data.get("birthPlace", "")
    country = player_data.get("intlTeam", "")
    batting_style = player_data.get("bat", "")
    bowling_style = player_data.get("bowl", "")
    major_teams = player_data.get("teams", "")
    playing_role = player_data.get("role", "")

    sql = '''
    INSERT INTO player_info (
        player_id, player_name, role, dob, birth_place, country,
        batting_style, bowling_style, major_teams, playing_role
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE 
        player_name=VALUES(player_name),
        role=VALUES(role),
        dob=VALUES(dob),
        birth_place=VALUES(birth_place),
        country=VALUES(country),
        batting_style=VALUES(batting_style),
        bowling_style=VALUES(bowling_style),
        major_teams=VALUES(major_teams),
        playing_role=VALUES(playing_role)
    '''
    values = (
        player_id, player_name, role, dob, birth_place, country,
        batting_style, bowling_style, major_teams, playing_role
    )
    cursor.execute(sql, values)
    conn.commit()

# --- Fetch from API and Save ---
headers = {
    "x-rapidapi-key": "42b8caf1ffmsh7ed1d227c4e7986p130ca0jsn26ba0afc0b48",
    "x-rapidapi-host": "cricbuzz-cricket.p.rapidapi.com"
}

player_ids = [8733, 576, 11808, 13940, 13866, 1413, 7915, 9428, 10896, 14504, 11813, 10636, 9129, 8257, 14701, 9647, 11195, 12086, 587, 8683, 10945, 8808, 10744, 14691, 8271, 10276, 10808, 9311, 10551, 14726, 8292, 13217, 24729, 14659, 10754, 12926]
for pid in player_ids:
     url = f"https://cricbuzz-cricket.p.rapidapi.com/stats/v1/player/{pid}"
     response = requests.get(url, headers=headers)
     
     if response.status_code == 200:
        player_data = response.json()
        insert_player_info(player_data)
        print(f"✅ Inserted {pid}")
     else:
         print(f"❌ Failed to insert {pid}")