import requests
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'database': 'cricbuzz2',
    'password': 'Root',
    'port': 3306
}

def fetch_team_results():
    print(f"[{datetime.now()}] Fetching team results...")

    url = "https://cricbuzz-cricket.p.rapidapi.com/teams/v1/2/results"
    headers = {
        "x-rapidapi-key": "f493d43d51mshd2b86741565d751p1ef6a6jsn5103eba1f487",
        "x-rapidapi-host": "cricbuzz-cricket.p.rapidapi.com"
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        print(f"Error fetching data: {e}")
        return

    rows = []

    # API response has "teamMatchesData" (list of series blocks)
    for series in data.get("teamMatchesData", []):
        match_map = series.get("matchDetailsMap", {})
        for match in match_map.get("match", []):
            info = match.get("matchInfo", {})
            score = match.get("matchScore", {})

        # Series name (prefer from matchInfo, else fallback to matchDetailsMap.key)
        series_name = info.get("seriesName") or match_map.get("key", "N/A")

        for match_wrapper in series.get("matchDetailsMap", {}).get("match", []):
            info = match_wrapper.get("matchInfo", {})
            score = match_wrapper.get("matchScore", {})

            team1 = info.get("team1", {}).get("teamName", "N/A")
            team2 = info.get("team2", {}).get("teamName", "N/A")

            status = info.get("status", "N/A")
            match_type = info.get("matchFormat", "N/A")

            # convert timestamp
            start_time = info.get("startDate", "N/A")
            if start_time != "N/A":
                try:
                    start_time = pd.to_datetime(int(start_time) / 1000, unit="s")
                except:
                    pass

            # extract scores
            def extract_score(score_dict):
                if not score_dict:
                    return "N/A"
                innings = list(score_dict.values())
                if not innings:
                    return "N/A"
                latest = innings[-1]
                return f"{latest.get('runs', 'N/A')}/{latest.get('wickets', 'N/A')} ({latest.get('overs', 'N/A')} ov)"

            team1_score = extract_score(score.get("team1Score"))
            team2_score = extract_score(score.get("team2Score"))

            rows.append({
                "Series_Name": series_name,
                "Match_Type": match_type,
                "Team_1": team1,
                "Team_2": team2,
                "Team_1_Score": team1_score,
                "Team_2_Score": team2_score,
                "Start_Time": start_time,
                "Status": status
            })

    if not rows:
        print("⚠️ No matches found in API response")
        return

    df = pd.DataFrame(rows)

    try:
        engine = create_engine(
            f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        )
        with engine.connect() as conn:
            df.to_sql("team_results", con=conn, if_exists="replace", index=False)

        print(f"✅ {len(df)} matches stored in database")
        print(df.head())
    except Exception as e:
        print(f"Error saving to DB: {e}")


if __name__ == "__main__":
    fetch_team_results()