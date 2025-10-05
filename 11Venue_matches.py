import requests
import pymysql
import time

# DB config
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'Root',
    'database': 'cricbuzz2',
    'port': 3306
}

conn = pymysql.connect(**DB_CONFIG)
cursor = conn.cursor()
print("✅ Connected to DB")

# API headers
headers = {
    "X-RapidAPI-Key": "b0f0df8844mshebee6ecac358d75p1fbe7ajsn5ae505036ae4",
    "X-RapidAPI-Host": "cricbuzz-cricket.p.rapidapi.com"
}

def fetch_venue_matches(venue_id):
    url = f"https://cricbuzz-cricket.p.rapidapi.com/venues/v1/{venue_id}/matches"
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()
        inserted_count = 0

        for detail in data.get("matchDetails", []):   # <-- Fix here
            series_map = detail.get("matchDetailsMap")
            if not series_map:
                continue

            series_name = series_map.get("key", "")
            series_id = series_map.get("seriesId", None)

            for match in series_map.get("match", []):  # <-- Fix here
                match_info = match.get("matchInfo", {})
                if not match_info:
                    continue

                match_id = match_info.get("matchId")
                match_desc = match_info.get("matchDesc", "")
                match_format = match_info.get("matchFormat", "")
                start_time = match_info.get("startDate", 0)
                end_time = match_info.get("endDate", 0)
                team1 = match_info.get("team1", {}).get("teamName", "")
                team2 = match_info.get("team2", {}).get("teamName", "")

                venue = match_info.get("venueInfo", {})
                venue_id = venue.get("id", )
                venue_name = venue.get("ground", "")
                city = venue.get("city", "")
                country = venue.get("country", "")

                cursor.execute("""
                    INSERT INTO venue_matches 
                        (match_id, venue_id, series_id, series_name, match_desc, match_format, start_time, end_time, team1, team2, venue_name, city, country)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        series_id=VALUES(series_id),
                        series_name=VALUES(series_name),
                        match_id=VALUES(match_id),
                        match_desc=VALUES(match_desc),
                        match_format=VALUES(match_format),
                        start_time=VALUES(start_time),
                        end_time=VALUES(end_time),
                        team1=VALUES(team1),
                        team2=VALUES(team2),
                        venue_name=VALUES(venue_name),
                        city=VALUES(city),
                        country=VALUES(country)
                """, (
                    match_id, venue_id, series_id, series_name, match_desc, match_format,
                    start_time, end_time, team1, team2, venue_name, city, country
                ))
                inserted_count += 1

        conn.commit()
        print(f"✅ Inserted/Updated {inserted_count} matches for venue ID {venue_id}")
    else:
        print(f"❌ Failed to fetch data for venue ID {venue_id}: {response.status_code}")


# Venue IDs list
venue_ids = [50, 80, 11, 154, 380, 81, 485, 27, 851, 76, 51, 511, 87, 31, 335, 512, 40, 485]

for venue_id in venue_ids:
    fetch_venue_matches(venue_id)
    time.sleep(1)  # avoid API rate limit

cursor.close()
conn.close()
