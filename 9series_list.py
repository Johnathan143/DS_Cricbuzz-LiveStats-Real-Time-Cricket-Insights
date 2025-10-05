import requests
import pandas as pd
import pymysql
from sqlalchemy import create_engine
from datetime import datetime

DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'Root',
    'port': 3306,
    'database': 'cricbuzz2'
}

# Step 1: Ensure database exists
def setup_database():
    conn = pymysql.connect(
        host=DB_CONFIG['host'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password'],
        port=DB_CONFIG['port']
    )
    cursor = conn.cursor()
    cursor.execute("CREATE DATABASE IF NOT EXISTS cricbuzz2;")
    conn.close()
    print("✅ Database checked/created")

setup_database()

# Step 2: Create SQLAlchemy engine
engine = create_engine(
    f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
)
print("✅ Connected to DB")

# Step 3: Fetch and Store Series
def fetch_and_store_series():
    url = "https://cricbuzz-cricket.p.rapidapi.com/series/v1/archives/international"
    querystring = {"year": "2024"}
    headers = {
        "x-rapidapi-key": "141ab0c4a8msh93aae951a18e4d0p102908jsn5b5b50ac879f",
        "x-rapidapi-host": "cricbuzz-cricket.p.rapidapi.com"
    }

    response = requests.get(url, headers=headers, params=querystring)
    data = response.json()

    series_list = []
    for group in data.get("seriesMapProto", []):
        for series in group.get("series", []):
            # Convert timestamps from ms → datetime
            start_dt = datetime.fromtimestamp(int(series.get("startDt", 0)) / 1000) if series.get("startDt") else None
            end_dt = datetime.fromtimestamp(int(series.get("endDt", 0)) / 1000) if series.get("endDt") else None

            series_list.append({
                "Series_ID": series.get("id"),
                "Series_Name": series.get("name"),
                "Start_Date": start_dt,
                "End_Date": end_dt,
                "Year": group.get("date")
            })

    df = pd.DataFrame(series_list)

    # Store in MySQL (auto-create table if not exists)
    df.to_sql("series_list", engine, if_exists="replace", index=False)

    print("✅ Series data fetched and stored in DB")

# Run once to test
fetch_and_store_series()
