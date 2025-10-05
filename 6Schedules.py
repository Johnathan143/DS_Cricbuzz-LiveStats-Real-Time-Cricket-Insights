"""import datetime
import requests
import pandas as pd
from sqlalchemy import create_engine,text
import schedule
import time
import sqlalchemy

DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'database': 'cricbuzz2',
    'password': 'Root',
    'port': 3306
}
conn = sqlalchemy.create_engine(f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}") 
cursor = conn.connect()
print("✅ Connected to DB")

# Create schedules table if not exists
with conn.connect() as cursor:
    cursor.execute(text('''
        CREATE TABLE IF NOT EXISTS schedules (
            Date VARCHAR(50),
            Series VARCHAR(255),
            `Match Description` VARCHAR(255),
            `Match Format` VARCHAR(50),
            `Start Time` VARCHAR(50),
            `Team 1` VARCHAR(100),
            `Team 2` VARCHAR(100),
            Venue VARCHAR(255),
            Country VARCHAR(100),
            `Match ID` INT PRIMARY KEY
        )
    '''))

    cursor.execute(text('''
        CREATE TABLE IF NOT EXISTS schedules_list (
            Date VARCHAR(50),
            Series VARCHAR(255),
            `Match Description` VARCHAR(255),
            `Match Format` VARCHAR(50),
            `Start Time` VARCHAR(50),
            `Team 1` VARCHAR(100),
            `Team 2` VARCHAR(100),
            Venue VARCHAR(255),
            Country VARCHAR(100),
            `Match ID` INT PRIMARY KEY
        )
    '''))
    cursor.commit()   # Commit after executing DDL
# ---------------------------------Upcoming Matches---------------------------------
def fetch_and_store_schedules():
    url = "https://cricbuzz-cricket.p.rapidapi.com/matches/v1/upcoming"
    headers = {
        "X-RapidAPI-Key": "42b8caf1ffmsh7ed1d227c4e7986p130ca0jsn26ba0afc0b48",
        "X-RapidAPI-Host": "cricbuzz-cricket.p.rapidapi.com"
    }
    response = requests.get(url, headers=headers)
    data = response.json()

    matches = []

    # Traverse typeMatches -> seriesMatches -> seriesAdWrapper -> matches -> matchInfo
    for type_match in data.get("typeMatches", []):
        for series in type_match.get("seriesMatches", []):
            if "seriesAdWrapper" in series:
                for match in series["seriesAdWrapper"].get("matches", []):
                    info = match.get("matchInfo", {})
                    venue = info.get("venueInfo", {})

                    matches.append({
                        "Date": info.get("startDate", ""),
                        "Series": info.get("seriesName", ""),
                        "Match Description": info.get("matchDesc", ""),
                        "Match Format": info.get("matchFormat", ""),
                        "Start Time": info.get("startDate", ""),
                        "Team 1": info.get("team1", {}).get("teamName", ""),
                        "Team 2": info.get("team2", {}).get("teamName", ""),
                        "Venue": venue.get("ground", ""),
                        "Country": venue.get("city", ""),
                        "Match ID": info.get("matchId", 0)
                    })

    df = pd.DataFrame(matches)
    df.to_sql("schedules", con=conn, if_exists="replace", index=False)
    print(f"✅ Schedules data updated at {datetime.datetime.now()}")

#----------------------------------Schedule list----------------------------
def fetch_and_store_schedules_list():
    url = "https://cricbuzz-cricket.p.rapidapi.com/schedule/v1/international"
    headers = {
        "X-RapidAPI-Key": "9793fa047dmsh6c827c37fd8d876p157048jsn34cc11ac1717",
        "X-RapidAPI-Host": "cricbuzz-cricket.p.rapidapi.com"
    }
    response = requests.get(url, headers=headers)
    data = response.json()

    matches = []

    # Traverse matchScheduleMap -> scheduleAdWrapper -> matchScheduleList -> matchInfo
    for sched in data.get("matchScheduleMap", []):
        if "scheduleAdWrapper" in sched:
            date = sched["scheduleAdWrapper"].get("date", "")
            for series in sched["scheduleAdWrapper"].get("matchScheduleList", []):
                for match in series.get("matchInfo", []):
                    venue = match.get("venueInfo", {})
                    matches.append({
                        "Date": date,
                        "Series": series.get("seriesName", ""),
                        "Match Description": match.get("matchDesc", ""),
                        "Match Format": match.get("matchFormat", ""),
                        "Start Time": match.get("startDate", ""),
                        "Team 1": match.get("team1", {}).get("teamName", ""),
                        "Team 2": match.get("team2", {}).get("teamName", ""),
                        "Venue": venue.get("ground", ""),
                        "Country": venue.get("country", ""),
                        "Match ID": match.get("matchId", 0)
                    })

    df = pd.DataFrame(matches)
    df.to_sql("schedules_list", con=conn, if_exists="replace", index=False)
    print(f"✅ Schedules list data updated at {datetime.datetime.now()}")
"""

import pandas as pd
import requests
from sqlalchemy import create_engine
from datetime import datetime

# --- DB Config ---
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'Root',
    'database': 'cricbuzz2',
    'port': 3306
}
engine = create_engine(
    f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
)

def fetch_and_store_schedules():
    url = "https://cricbuzz-cricket.p.rapidapi.com/matches/v1/upcoming"
    headers = {
        "x-rapidapi-key": "f493d43d51mshd2b86741565d751p1ef6a6jsn5103eba1f487",
        "x-rapidapi-host": "cricbuzz-cricket.p.rapidapi.com"
    }

    response = requests.get(url, headers=headers)
    print("Upcoming API status:", response.status_code)

    try:
        data = response.json()
    except Exception as e:
        print("⚠️ Failed to parse JSON:", e)
        return

    print("Top-level keys:", list(data.keys()))

    matches = []
    if "typeMatches" in data:
        for match_type in data["typeMatches"]:
            for series in match_type.get("seriesMatches", []):
                series_info = series.get("seriesAdWrapper", {}).get("seriesName", "Unknown Series")
                for match in series.get("seriesAdWrapper", {}).get("matches", []):
                    info = match.get("matchInfo", {})
                    matches.append({
                        "matchId": info.get("matchId"),
                        "series": series_info,
                        "team1": info.get("team1", {}).get("teamName"),
                        "team2": info.get("team2", {}).get("teamName"),
                        "venue": info.get("venueInfo", {}).get("ground"),
                        "startTime": datetime.fromtimestamp(int(info.get("startDate")) / 1000),
                        "status": info.get("status")
                    })
    else:
        print("⚠️ No typeMatches in response. Message:", data.get("message"))

    # ✅ Always create df (even if empty)
    df = pd.DataFrame(matches)
    print(f"Fetched {len(df)} upcoming matches")

    if not df.empty:
        df.to_sql("schedules", con=engine, if_exists="replace", index=False)
        print("✅ Schedules saved to DB")
    else:
        print("⚠️ No data to insert. Skipping DB operation.")

fetch_and_store_schedules()