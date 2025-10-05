import requests
import pymysql
import json

# ---------------- DB CONFIG ----------------
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'Root',
    'database': 'Cricbuzz2',
    "port": 3306
}

conn = pymysql.connect(**DB_CONFIG)
cursor = conn.cursor()
print("✅ Connected to DB")

# ---------------- TABLE ----------------
cursor.execute('''
    CREATE TABLE IF NOT EXISTS SCOREBOARD (
        id INT AUTO_INCREMENT PRIMARY KEY,
        record_type ENUM('batsman','partnership','bowler') NOT NULL,
        
        -- Batsman fields
        player_id INT,
        player_name VARCHAR(100),
        runs INT,
        balls INT,
        dots INT,
        fours INT,
        sixes INT,
        strike_rate FLOAT,
        dismissal VARCHAR(200),
        
        -- Partnership fields
        bat_partner_id INT,
        bat_partner_name VARCHAR(100),
        bat_partner_runs INT,
        bat_partner_fours INT,
        bat_partner_sixes INT,
        total_runs INT,
        total_balls INT,
        
        -- Bowler fields
        bowler_id INT,
        bowler_name VARCHAR(100),
        overs FLOAT,
        maidens INT,
        bowler_runs INT,
        wickets INT,
        economy FLOAT
    )
''')
print("✅ Table created or already exists")
conn.commit()

# ---------------- API ----------------
urls = [
    "https://cricbuzz-cricket.p.rapidapi.com/mcenter/v1/40381/hscard",
    "https://cricbuzz-cricket.p.rapidapi.com/mcenter/v1/40381/scard"
]

headers = {
	"x-rapidapi-key": "446f502ff3msh08667e5149ab515p181e4cjsnf41ba001f3da",
	"x-rapidapi-host": "cricbuzz-cricket.p.rapidapi.com"
}

count = 0

# Process both URLs
for url in urls:
    print(f"Fetching data from: {url}")
    response = requests.get(url, headers=headers)
    Score = response.json()
    
    # Uncomment this once to inspect API structure:
    # print(json.dumps(Score, indent=4))
    
    for scard in Score.get("scorecard", []):

        # 🏏 Batsmen - Using actual JSON field names
        for bat in scard.get("batsman", []):
            # Convert strike rate from string to float
            strike_rate = None
            if bat.get("strkrate"):
                try:
                    strike_rate = float(bat.get("strkrate"))
                except (ValueError, TypeError):
                    strike_rate = None
            
            cursor.execute('''
                         INSERT INTO SCOREBOARD
                         (record_type, player_id, player_name, runs, balls, dots, fours, sixes, strike_rate, dismissal)
                         VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                         ''', (
                            'batsman',
                            bat.get("id"),
                            bat.get("name"),
                            bat.get("runs"),
                            bat.get("balls"),
                            None,  # dots field doesn't exist for batsmen in API
                            bat.get("fours"),
                            bat.get("sixes"),
                            strike_rate,
                            bat.get("outdec")
                     ))
            count += 1

        # 🎯 Bowlers - Using actual JSON field names
        for bowler in scard.get("bowler", []):
            # Convert string values to proper types
            overs_val = None
            if bowler.get("overs"):
                try:
                    overs_val = float(bowler.get("overs"))
                except (ValueError, TypeError):
                    overs_val = None
            
            economy_val = None
            if bowler.get("economy"):
                try:
                    economy_val = float(bowler.get("economy"))
                except (ValueError, TypeError):
                    economy_val = None
            
            cursor.execute('''
                INSERT INTO SCOREBOARD
                (record_type, bowler_id, bowler_name, overs, maidens, bowler_runs, wickets, economy)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ''', ( "bowler",
                bowler.get("id"),
                bowler.get("name"),
                overs_val,
                bowler.get("maidens"),
                bowler.get("runs"),
                bowler.get("wickets"),
                economy_val
            ))
            count += 1

        # 🤝 Partnerships - Using exact JSON structure
        partnership_data = scard.get("partnership", {})
        if partnership_data and "partnership" in partnership_data:
            for p in partnership_data["partnership"]:
                cursor.execute('''
                    INSERT INTO SCOREBOARD
                    (record_type, player_id, player_name, runs, fours, sixes,
                     bat_partner_id, bat_partner_name, bat_partner_runs, bat_partner_fours, bat_partner_sixes,
                     total_runs, total_balls)
                    VALUES ('partnership', %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ''', (
                    p.get("bat1id"), 
                    p.get("bat1name"), 
                    p.get("bat1runs"), 
                    p.get("bat1fours"), 
                    p.get("bat1sixes"),
                    p.get("bat2id"), 
                    p.get("bat2name"), 
                    p.get("bat2runs"), 
                    p.get("bat2fours"), 
                    p.get("bat2sixes"),
                    p.get("totalruns"), 
                    p.get("totalballs")
                ))
                count += 1

conn.commit()
print(f"✅ Inserted {count} records into the database")

# Optional: Display some sample data
print("\n📊 Sample data from database:")
cursor.execute("SELECT record_type, player_name, runs, dismissal FROM SCOREBOARD WHERE record_type='batsman' LIMIT 5")
batsmen_data = cursor.fetchall()
for row in batsmen_data:
    print(f"  {row[0]}: {row[1]} - {row[2]} runs ({row[3]})")

print("\n🤝 Partnership data:")
cursor.execute("SELECT player_name, bat_partner_name, total_runs FROM SCOREBOARD WHERE record_type='partnership' LIMIT 3")
partnership_data = cursor.fetchall()
for row in partnership_data:
    print(f"  {row[0]} & {row[1]} - {row[2]} runs")

conn.close()