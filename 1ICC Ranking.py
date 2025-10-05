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

# --- CREATE TABLES ---
cursor.execute('''
    CREATE TABLE IF NOT EXISTS ICC_RANKS (
        player_id INT PRIMARY KEY,
        player_rank INT,
        player_name VARCHAR(100),
        country VARCHAR(100),
        rating INT,
        points INT
    )
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS TEAM_STANDINGS (
        `rank` INT PRIMARY KEY,
        flag INT,
        team VARCHAR(100),
        pct FLOAT
    )
''')
conn.commit()
print("✅ Tables created or already exist")

# --- API CONFIG ---
headers = {
	"x-rapidapi-key": "f493d43d51mshd2b86741565d751p1ef6a6jsn5103eba1f487",
	"x-rapidapi-host": "cricbuzz-cricket.p.rapidapi.com"
}

# --- 1. PLAYER RANKINGS ---
url_players = "https://cricbuzz-cricket.p.rapidapi.com/stats/v1/rankings/batsmen"
params = {"formatType": "test"}   # change to 'odi' or 't20' if needed

response = requests.get(url_players, headers=headers, params=params)
player_data = response.json()

count = 0
for player in player_data.get('rank', []):
    try:
        player_id = int(player.get('id'))
        player_rank = int(player.get('rank', 0))
        player_name = player.get('name')
        country = player.get('country')
        rating = int(player.get('rating', 0))
        points = int(player.get('points', 0))

        cursor.execute('''
            INSERT IGNORE INTO ICC_RANKS (player_id, player_rank, player_name, country, rating, points)
            VALUES (%s, %s, %s, %s, %s, %s)
        ''', (player_id, player_rank, player_name, country, rating, points))
        count += 1
    except Exception as e:
        print(f"Error processing player {player}: {e}")
print(f"✅ Inserted {count} players into ICC_RANKS")

# --- 2. TEAM STANDINGS ---
url_teams = "https://cricbuzz-cricket.p.rapidapi.com/stats/v1/iccstanding/team/matchtype/1"  # 1=test, 2=odi, 3=t20

response = requests.get(url_teams, headers=headers)
team_data = response.json()

count = 0
for row in team_data['values']:
    rank, flag, team, pct = row['value']
    cursor.execute(
        "INSERT INTO TEAM_STANDINGS (`rank`, flag, team, pct) VALUES (%s, %s, %s, %s) ON DUPLICATE KEY UPDATE flag=%s, team=%s, pct=%s",
        (int(rank), int(flag), team, float(pct), int(flag), team, float(pct))
    )

conn.commit()
conn.close()
print("🎉 Done")
