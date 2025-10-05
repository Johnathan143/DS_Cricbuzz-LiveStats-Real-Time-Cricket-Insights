import pymysql
import requests
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "Root",
    "database": "cricbuzz2",
    "port": 3306
}
conn=pymysql.connect(**DB_CONFIG)
cursor=conn.cursor()
print("✅ Connected to DB")

# create combined table (added innings_name)
cursor.execute("""
CREATE TABLE IF NOT EXISTS match_commentary (
    id INT AUTO_INCREMENT PRIMARY KEY,
    match_id INT,
    series_id INT,
    series_name VARCHAR(255),
    match_desc VARCHAR(255),
    format VARCHAR(50),
    state VARCHAR(50),
    status VARCHAR(255),
    team1_id INT,
    team1_name VARCHAR(255),
    team2_id INT,
    team2_name VARCHAR(255),
    toss_winner_id INT,
    toss_winner_name VARCHAR(255),
    toss_decision VARCHAR(50),
    winning_team_id INT,
    innings_id INT,
    innings_name VARCHAR(100),
    overnum FLOAT,
    ballnbr INT,
    eventtype VARCHAR(100),
    commtxt TEXT,
    timestamp BIGINT,
    batscore INT
)
""")
conn.commit()

# API headers
headers = {
    "X-RapidAPI-Key": "f493d43d51mshd2b86741565d751p1ef6a6jsn5103eba1f487",
    "X-RapidAPI-Host": "cricbuzz-cricket.p.rapidapi.com"
}

def insert_match_with_commentary(match_id, info, comm_data):
    """Insert both match info + commentary into DB"""
    headers_info = info.get("matchInfo", {}) or info.get("matchheaders", {})
    toss = headers_info.get("tossResults", {}) or headers_info.get("tossresults", {})

    for wrapper in comm_data.get("commLines", []) or comm_data.get("comwrapper", []):
        comm = wrapper.get("commentary", wrapper)

        cursor.execute("""
    INSERT INTO match_commentary (
        match_id, series_id, series_name, match_desc, format, state, status,
        team1_id, team1_name, team2_id, team2_name,
        toss_winner_id, toss_winner_name, toss_decision, winning_team_id,
        innings_id, innings_name, overnum, ballnbr,
        eventtype, commtxt, timestamp, batscore
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
""", (
            match_id,
            headers_info.get("seriesId") or headers_info.get("seriesid"),
            headers_info.get("seriesName") or headers_info.get("seriesname"),
            headers_info.get("matchDesc") or headers_info.get("matchdesc"),
            headers_info.get("matchFormat") or headers_info.get("matchformat"),
            headers_info.get("state"),
            headers_info.get("status"),
            headers_info.get("team1", {}).get("id") or headers_info.get("team1", {}).get("teamid"),
            headers_info.get("team1", {}).get("name") or headers_info.get("team1", {}).get("teamname"),
            headers_info.get("team2", {}).get("id") or headers_info.get("team2", {}).get("teamid"),
            headers_info.get("team2", {}).get("name") or headers_info.get("team2", {}).get("teamname"),
            toss.get("winnerId") or toss.get("tosswinnerid"),
            toss.get("winnerName") or toss.get("tosswinnername"),
            toss.get("decision"),
            headers_info.get("winningTeamId") or headers_info.get("winningteamid"),
            comm.get("inningsId") or comm.get("inningsid"),
            comm.get("inningsName") or comm.get("inningsname"),
            comm.get("overNumber") or comm.get("overnum"),
            comm.get("ballNbr") or comm.get("ballnbr"),
            comm.get("event") or comm.get("eventtype"),
            comm.get("commText") or comm.get("commtxt"),
            comm.get("timestamp"),
            comm.get("batTeamScore") or comm.get("batteamscore")
        ))

    conn.commit()


# Example list of match_ids
match_ids = [113289,113274,113262,113280,113271,113670,113658,113676,113661,
             133858,133864,133869,119852,135101,135090,135096,135079]

# Loop over matches and fetch API data
for match_id in match_ids:
    try:
        # Fetch match info
        info_url = f"https://cricbuzz-cricket.p.rapidapi.com/mcenter/v1/{match_id}"
        info_resp = requests.get(info_url, headers=headers)
        info_data = info_resp.json()

        # Fetch commentary
        comm_url = f"https://cricbuzz-cricket.p.rapidapi.com/mcenter/v1/{match_id}/comm"
        comm_resp = requests.get(comm_url, headers=headers)
        comm_data = comm_resp.json()

        # Insert into DB
        insert_match_with_commentary(match_id, info_data, comm_data)
        print(f"✅ Inserted match {match_id}")

    except Exception as e:
        print(f"❌ Error processing match {match_id}: {e}")
