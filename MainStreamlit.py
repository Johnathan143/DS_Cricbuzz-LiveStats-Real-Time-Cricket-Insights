import streamlit as st
import pandas as pd
import numpy as np
import pymysql
import mysql.connector
from mysql.connector import Error
from datetime import datetime
import plotly.express as px
import plotly.graph_objects as go
# ========== PAGE CONFIG ==========
st.set_page_config(
    page_title="Mizaru's Cricket Live",
    layout="wide",
    initial_sidebar_state="expanded",
    page_icon="🏏"
)
# ========== ENHANCED CSS ==========
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Poppins:wght@400;600;700&display=swap');
    
    :root {
        --bg-primary: #ffffff;
        --bg-secondary: #f5f7fa;
        --text-primary: #1a1a1a;
        --text-secondary: #666666;
        --border-color: #e0e0e0;
        --card-shadow: rgba(0,0,0,0.1);
    }
    
    @media (prefers-color-scheme: dark) {
        :root {
            --bg-primary: #1e1e1e;
            --bg-secondary: #2d2d2d;
            --text-primary: #e0e0e0;
            --text-secondary: #b0b0b0;
            --border-color: #404040;
            --card-shadow: rgba(0,0,0,0.3);
        }
    }
    
    * { font-family: 'Poppins', sans-serif; }
    
    .main-header {
        text-align: center;
        padding: 30px;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        border-radius: 15px;
        margin-bottom: 30px;
        box-shadow: 0 10px 30px rgba(102, 126, 234, 0.3);
    }
    
    .main-header h1 {
        color: white;
        font-size: 2.5em;
        margin: 0;
        text-shadow: 2px 2px 4px rgba(0,0,0,0.2);
    }
    
    .score-card {
        background: var(--bg-primary);
        padding: 25px;
        border-radius: 15px;
        box-shadow: 0 5px 20px var(--card-shadow);
        margin: 15px 0;
        border-left: 5px solid #667eea;
    }
    
    .team-score {
        font-size: 2.5em;
        font-weight: 700;
        color: #667eea;
        margin: 10px 0;
    }
    
    .innings-score {
        background: var(--bg-secondary);
        padding: 15px;
        border-radius: 10px;
        margin: 10px 0;
        border-left: 3px solid #764ba2;
    }
    
    .live-indicator {
        display: inline-block;
        width: 12px;
        height: 12px;
        background-color: #ff4444;
        border-radius: 50%;
        animation: pulse 2s infinite;
        margin-right: 8px;
    }
    
    @keyframes pulse {
        0%, 100% { opacity: 1; }
        50% { opacity: 0.5; }
    }
    
    .commentary-card {
        background: var(--bg-secondary);
        padding: 15px;
        border-radius: 10px;
        margin-bottom: 12px;
        border-left: 4px solid #667eea;
        transition: all 0.2s;
    }
    
    .commentary-card:hover {
        transform: translateX(5px);
        box-shadow: 0 3px 10px var(--card-shadow);
    }
    
    .commentary-over {
        display: inline-block;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 5px 12px;
        border-radius: 15px;
        font-weight: 600;
        font-size: 0.85em;
        margin-right: 10px;
    }
    
    .commentary-event {
        display: inline-block;
        padding: 3px 10px;
        border-radius: 12px;
        font-size: 0.8em;
        font-weight: 600;
        margin-left: 8px;
    }
    
    .event-wicket {
        background: #ff4444;
        color: white;
    }
    
    .event-boundary {
        background: #4CAF50;
        color: white;
    }
    
    .event-six {
        background: #FF9800;
        color: white;
    }
</style>
""", unsafe_allow_html=True)

# ------------------ DATABASE CONFIG ------------------
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'Root',
    'database': 'cricbuzz2',
    'port': 3306
}

def get_mysql_conn():
    return pymysql.connect(**DB_CONFIG)

def run_query(sql, params=None):
    conn = get_mysql_conn()
    if conn is None:
        return pd.DataFrame()
    try:
        df = pd.read_sql(sql, conn, params=params)
        return df
    except Exception as e:
        st.error(f"Query failed: {e}")
        return pd.DataFrame()
    finally:
        try:
            conn.close()
        except:
            pass

def modify_query(sql, params=None):
    """
    Execute INSERT/UPDATE/DELETE.
    """
    conn = get_mysql_conn()
    if conn is None:
        raise Exception("DB connection not available")
    cursor = conn.cursor()
    try:
        cursor.execute(sql, params or ())
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

@st.cache_data(ttl=30)
def get_table_data(table_name):
    conn = get_mysql_conn()
    if conn:
        try:
            df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
            return df
        except Exception as e:
            st.error(f"Error fetching {table_name}: {str(e)}")
            return pd.DataFrame()
        finally:
            try:
                conn.close()
            except:
                pass
    return pd.DataFrame()

@st.cache_data(ttl=30)
def get_match_data(match_id):
    """Get all data for a specific match"""
    conn = get_mysql_conn()
    if not conn:
        return None, None, None, None, None, None, None
    
    try:
        # Get match info
        match_query = "SELECT * FROM live_match_info WHERE match_id = %s"
        match_info = pd.read_sql(match_query, conn, params=[match_id])
        
        # Get teams
        teams_query = "SELECT * FROM live_teams WHERE match_id = %s"
        teams = pd.read_sql(teams_query, conn, params=[match_id])
        
        # Get venue
        venue_query = "SELECT * FROM live_venues WHERE match_id = %s"
        venue = pd.read_sql(venue_query, conn, params=[match_id])
        
        # Get batting stats
        batting_query = "SELECT * FROM live_batting_stats WHERE match_id = %s"
        batting_stats = pd.read_sql(batting_query, conn, params=[match_id])
        
        # Get bowling stats
        bowling_query = "SELECT * FROM live_bowling_stats WHERE match_id = %s"
        bowling_stats = pd.read_sql(bowling_query, conn, params=[match_id])
        
        # Get scorecard metadata
        scorecard_query = "SELECT * FROM live_scorecard_metadata WHERE match_id = %s"
        scorecard_meta = pd.read_sql(scorecard_query, conn, params=[match_id])
        
        # Get commentary
        commentary_query = "SELECT * FROM live_commentary WHERE match_id = %s ORDER BY timestamp DESC"
        commentary = pd.read_sql(commentary_query, conn, params=[match_id])
        
        return match_info, teams, venue, batting_stats, bowling_stats, scorecard_meta, commentary
    except Exception as e:
        st.error(f"Error fetching match data: {str(e)}")
        return None, None, None, None, None, None, None
    finally:
        try:
            conn.close()
        except:
            pass

def clean_numeric_column(df, col):
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    return df

def format_score(runs, wickets, overs, declared=False):
    """Format score as 'runs/wickets (overs)'"""
    if pd.isna(runs):
        return "Yet to bat"
    
    try:
        # cast to int if possible
        runs_int = int(runs)
    except:
        runs_int = runs
    score = f"{runs_int}"
    if not pd.isna(wickets):
        try:
            wickets_int = int(wickets)
            score += f"/{wickets_int}"
        except:
            score += f"/{wickets}"
    if not pd.isna(overs):
        score += f" ({overs} ov)"
    if declared:
        score += " dec"
    
    return score

# ========== HEADER ==========
st.markdown("""
<div class="main-header">
    <h1>🏏🐵Mizaru's Cricket Live Info🏏</h1>
    <p style="color: white; margin: 0;">Real-time Cricket Analytics & Statistics</p>
</div>
""", unsafe_allow_html=True)

# ========== SIDEBAR ==========
pages = [
    "Live Scores",
    "Player Stats",
    "SQL Analytics",
    "CRUD Operations"
]
page = st.sidebar.selectbox("Choose a page:", pages)

st.sidebar.markdown("---")
st.sidebar.markdown("#### 📊 Detailed scorecards")
st.sidebar.markdown("#### 🏆 Series information")
st.sidebar.markdown("#### 🎯 Interactive match selection")

# ========== LOAD MATCHES ==========
with st.spinner("Loading live matches..."):
    live_matches = get_table_data("live_match_info")

if page == "Live Scores":
    if live_matches.empty:
        st.warning("No live matches available")
        st.stop()

    # ========== DISPLAY ALL LIVE MATCHES ==========
    st.markdown("## 🔴 Live Matches")

    for idx, match_row in live_matches.iterrows():
        match_id = match_row.get('match_id', '')

        # Get complete match data
        match_info, teams_df, venue_df, batting_df, bowling_df, scorecard_df, commentary_df = get_match_data(match_id)

        if match_info is None or match_info.empty:
            st.warning(f"No data available for Match {match_id}")
            continue

        match = match_info.iloc[0]

        # Get team names
        team1_name = "Team 1"
        team2_name = "Team 2"
        if teams_df is not None and not teams_df.empty:
            team1_row = teams_df[teams_df['team_role'] == 'team1']
            team2_row = teams_df[teams_df['team_role'] == 'team2']
            if not team1_row.empty:
                team1_name = team1_row.iloc[0].get('team_name', 'Team 1')
            if not team2_row.empty:
                team2_name = team2_row.iloc[0].get('team_name', 'Team 2')

        # Create expandable match card
        with st.expander(
            f"🏏 {team1_name} vs {team2_name} - {match.get('match_format', '')} (Match {match_id})",
            expanded=(idx == 0)
        ):
            # ========== MATCH HEADER ==========
            col1, col2, col3 = st.columns([1, 2, 1])

            with col1:
                st.markdown(f"**🏆 Series:** {match.get('series_name', 'N/A')}")
                st.markdown(f"**📅 Format:** {match.get('match_format', 'N/A')}")
                st.markdown(f"**🎲 Toss:** {match.get('toss_status', 'N/A')}")

            with col2:
                venue_name = "Unknown Venue"
                if venue_df is not None and not venue_df.empty:
                    venue_row = venue_df.iloc[0]
                    venue_name = f"{venue_row.get('ground', 'Unknown')}, {venue_row.get('city', '')}"
                st.markdown(f"### 🏟️ {venue_name}")

                state = match.get('state', 'Unknown')
                if state in ['In Progress', 'Live']:
                    st.markdown(f"**📊 Status:** <span class='live-indicator'></span>**{match.get('status', 'Live')}**", unsafe_allow_html=True)
                else:
                    st.markdown(f"**📊 Status:** {match.get('status', 'Complete')}")

            with col3:
                st.markdown(f"**🆔 Match ID:** {match_id}")
                st.markdown(f"**📍 State:** {state}")

            st.markdown("---")

            # ========== LIVE SCORE ==========
            st.markdown("### 📊 Match Score")

            score_col1, score_col2 = st.columns(2)

            with score_col1:
                st.markdown(f"#### {team1_name}")

                # Innings 1
                team1_inn1_score = format_score(
                    match.get('team1_inngs1_runs'),
                    match.get('team1_inngs1_wickets'),
                    match.get('team1_inngs1_overs'),
                    match.get('team1_inngs1_declared', False)
                )
                st.markdown(f"**Innings 1:** {team1_inn1_score}")

                # Innings 2 (if exists)
                if not pd.isna(match.get('team1_inngs2_runs')):
                    team1_inn2_score = format_score(
                        match.get('team1_inngs2_runs'),
                        match.get('team1_inngs2_wickets'),
                        match.get('team1_inngs2_overs'),
                        match.get('team1_inngs2_declared', False)
                    )
                    st.markdown(f"**Innings 2:** {team1_inn2_score}")

            with score_col2:
                st.markdown(f"#### {team2_name}")

                # Innings 1
                team2_inn1_score = format_score(
                    match.get('team2_inngs1_runs'),
                    match.get('team2_inngs1_wickets'),
                    match.get('team2_inngs1_overs'),
                    match.get('team2_inngs1_declared', False)
                )
                st.markdown(f"**Innings 1:** {team2_inn1_score}")

                # Innings 2 (if exists)
                if not pd.isna(match.get('team2_inngs2_runs')):
                    team2_inn2_score = format_score(
                        match.get('team2_inngs2_runs'),
                        match.get('team2_inngs2_wickets'),
                        match.get('team2_inngs2_overs'),
                        match.get('team2_inngs2_declared', False)
                    )
                    st.markdown(f"**Innings 2:** {team2_inn2_score}")

            st.markdown("---")

            # ========== MATCH TABS ==========
            tab1, tab2, tab3, tab4 = st.tabs(["🏏 Batting Stats", "🎳 Bowling Stats", "💬 Live Commentary", "📋 Match Details"])

            # BATTING TAB
            with tab1:
                if batting_df is None or batting_df.empty:
                    st.info("No batting data available")
                else:
                    # Clean numeric columns
                    for col in ['runs', 'strike_rate', 'fours', 'sixes', 'balls_faced']:
                        batting_df = clean_numeric_column(batting_df, col)

                    # Summary Stats
                    col1, col2, col3, col4, col5 = st.columns(5)

                    with col1:
                        total_runs = int(batting_df['runs'].sum(skipna=True))
                        st.metric("Total Runs", total_runs)

                    with col2:
                        batsmen = len(batting_df)
                        st.metric("Batsmen", batsmen)

                    with col3:
                        avg_sr = batting_df['strike_rate'].mean(skipna=True)
                        st.metric("Avg Strike Rate", f"{avg_sr:.1f}")

                    with col4:
                        fours = int(batting_df['fours'].sum(skipna=True))
                        st.metric("Fours", fours)

                    with col5:
                        sixes = int(batting_df['sixes'].sum(skipna=True))
                        st.metric("Sixes", sixes)

                    st.markdown("<br>", unsafe_allow_html=True)

                    # Filter by team
                    if 'team_name' in batting_df.columns:
                        teams = ["All Teams"] + sorted(batting_df['team_name'].dropna().unique().tolist())
                        selected_team = st.selectbox(f"Filter by Team", teams, key=f"bat_team_{match_id}")

                        if selected_team != "All Teams":
                            batting_df = batting_df[batting_df['team_name'] == selected_team]

                    # Display table
                    display_cols = ['batsman_name', 'team_name', 'runs', 'balls_faced', 'fours', 'sixes', 'strike_rate', 'out_desc']
                    display_df = batting_df[[col for col in display_cols if col in batting_df.columns]]
                    st.dataframe(display_df, use_container_width=True, height=300)

                    # Top performers chart
                    if len(batting_df) > 0 and 'batsman_name' in batting_df.columns:
                        top_batsmen = batting_df.nlargest(5, 'runs')
                        if not top_batsmen.empty:
                            fig = px.bar(
                                top_batsmen,
                                x='batsman_name',
                                y='runs',
                                title='Top 5 Run Scorers',
                                color='runs',
                                color_continuous_scale='Viridis'
                            )
                            fig.update_layout(height=300, showlegend=False)
                            st.plotly_chart(fig, use_container_width=True)

            # BOWLING TAB
            with tab2:
                if bowling_df is None or bowling_df.empty:
                    st.info("No bowling data available")
                else:
                    # Clean numeric columns
                    for col in ['wickets', 'economy', 'overs', 'runs_conceded']:
                        bowling_df = clean_numeric_column(bowling_df, col)

                    # Summary Stats
                    col1, col2, col3, col4 = st.columns(4)

                    with col1:
                        total_wickets = int(bowling_df['wickets'].sum(skipna=True))
                        st.metric("Total Wickets", total_wickets)

                    with col2:
                        bowlers = len(bowling_df)
                        st.metric("Bowlers", bowlers)

                    with col3:
                        avg_econ = bowling_df['economy'].mean(skipna=True)
                        st.metric("Avg Economy", f"{avg_econ:.2f}")

                    with col4:
                        total_overs = bowling_df['overs'].sum(skipna=True)
                        st.metric("Total Overs", f"{total_overs:.1f}")

                    st.markdown("<br>", unsafe_allow_html=True)

                    # Filter by team
                    if 'team_name' in bowling_df.columns:
                        teams = ["All Teams"] + sorted(bowling_df['team_name'].dropna().unique().tolist())
                        selected_team = st.selectbox(f"Filter by Team", teams, key=f"bowl_team_{match_id}")

                        if selected_team != "All Teams":
                            bowling_df = bowling_df[bowling_df['team_name'] == selected_team]

                    # Display table
                    display_cols = ['bowler_name', 'team_name', 'overs', 'runs_conceded', 'wickets', 'economy', 'maidens']
                    display_df = bowling_df[[col for col in display_cols if col in bowling_df.columns]]
                    st.dataframe(display_df, use_container_width=True, height=300)

                    # Top performers chart
                    if len(bowling_df) > 0 and 'bowler_name' in bowling_df.columns:
                        top_bowlers = bowling_df.nlargest(5, 'wickets')
                        if not top_bowlers.empty:
                            fig = px.bar(
                                top_bowlers,
                                x='bowler_name',
                                y='wickets',
                                title='Top 5 Wicket Takers',
                                color='wickets',
                                color_continuous_scale='Reds'
                            )
                            fig.update_layout(height=300, showlegend=False)
                            st.plotly_chart(fig, use_container_width=True)

            # LIVE COMMENTARY TAB
            with tab3:
                if commentary_df is None or commentary_df.empty:
                    st.info("No live commentary available")
                else:
                    st.markdown("### 💬 Ball-by-Ball Commentary")

                    # Commentary filters
                    col1, col2, col3 = st.columns(3)

                    with col1:
                        if 'innings' in commentary_df.columns:
                            innings_options = ["All Innings"] + sorted(commentary_df['innings'].dropna().unique().tolist())
                            selected_innings = st.selectbox("Filter by Innings", innings_options, key=f"comm_innings_{match_id}")
                            if selected_innings != "All Innings":
                                commentary_df = commentary_df[commentary_df['innings'] == selected_innings]

                    with col2:
                        show_count = st.slider("Show last N balls", 10, 100, 30, key=f"comm_count_{match_id}")

                    with col3:
                        if 'event_type' in commentary_df.columns:
                            event_filter = st.multiselect(
                                "Filter by Event",
                                options=commentary_df['event_type'].dropna().unique().tolist(),
                                key=f"comm_event_{match_id}"
                            )
                            if event_filter:
                                commentary_df = commentary_df[commentary_df['event_type'].isin(event_filter)]

                    st.markdown("<br>", unsafe_allow_html=True)

                    # Display commentary
                    commentary_display = commentary_df.head(show_count)

                    if len(commentary_display) == 0:
                        st.info("No commentary matches your filters")
                    else:
                        for idx2, row in commentary_display.iterrows():
                            over_num = row.get('over_number', 'N/A')
                            ball_num = row.get('ball_number', 'N/A')
                            comm_text = row.get('commentary_text', 'No commentary available')
                            event_type = row.get('event_type', '')
                            runs = row.get('runs_scored', 0)

                            # Determine event badge
                            event_badge = ""
                            if event_type and str(event_type).lower() == 'wicket':
                                event_badge = "<span class='commentary-event event-wicket'>WICKET!</span>"
                            elif runs == 6:
                                event_badge = "<span class='commentary-event event-six'>SIX!</span>"
                            elif runs == 4:
                                event_badge = "<span class='commentary-event event-boundary'>FOUR!</span>"

                            st.markdown(f"""
                            <div class='commentary-card'>
                                <div>
                                    <span class='commentary-over'>Over {over_num}.{ball_num}</span>
                                    {event_badge}
                                </div>
                                <div class='commentary-text'>{comm_text}</div>
                            </div>
                            """, unsafe_allow_html=True)

                    st.markdown("<br>", unsafe_allow_html=True)

                    # Commentary Statistics
                    if len(commentary_df) > 0:
                        st.markdown("#### 📊 Commentary Statistics")

                        stat_col1, stat_col2, stat_col3, stat_col4 = st.columns(4)

                        with stat_col1:
                            total_balls = len(commentary_df)
                            st.metric("Total Balls", total_balls)

                        with stat_col2:
                            if 'runs_scored' in commentary_df.columns:
                                total_runs = int(commentary_df['runs_scored'].sum(skipna=True))
                                st.metric("Total Runs", total_runs)

                        with stat_col3:
                            if 'event_type' in commentary_df.columns:
                                wickets = len(commentary_df[commentary_df['event_type'].str.lower() == 'wicket'])
                                st.metric("Wickets", wickets)

                        with stat_col4:
                            if 'runs_scored' in commentary_df.columns:
                                boundaries = len(commentary_df[commentary_df['runs_scored'].isin([4, 6])])
                                st.metric("Boundaries", boundaries)

            # MATCH DETAILS TAB
            with tab4:
                st.markdown("### 📋 Complete Match Information")

                # Display all match info
                match_details = match_info.iloc[0].to_dict()

                # Organize into sections
                col1, col2 = st.columns(2)

                with col1:
                    st.markdown("#### 🏏 Match Details")
                    st.json({
                        "Match ID": match_details.get('match_id'),
                        "Series": match_details.get('series_name'),
                        "Match Description": match_details.get('match_desc'),
                        "Format": match_details.get('match_format'),
                        "State": match_details.get('state'),
                        "Status": match_details.get('status')
                    })

                with col2:
                    st.markdown("#### 📊 Score Summary")
                    st.json({
                        "Team 1 Inn 1": format_score(
                            match_details.get('team1_inngs1_runs'),
                            match_details.get('team1_inngs1_wickets'),
                            match_details.get('team1_inngs1_overs')
                        ),
                        "Team 1 Inn 2": format_score(
                            match_details.get('team1_inngs2_runs'),
                            match_details.get('team1_inngs2_wickets'),
                            match_details.get('team1_inngs2_overs')
                        ),
                        "Team 2 Inn 1": format_score(
                            match_details.get('team2_inngs1_runs'),
                            match_details.get('team2_inngs1_wickets'),
                            match_details.get('team2_inngs1_overs')
                        ),
                        "Team 2 Inn 2": format_score(
                            match_details.get('team2_inngs2_runs'),
                            match_details.get('team2_inngs2_wickets'),
                            match_details.get('team2_inngs2_overs')
                        )
                    })

                if scorecard_df is not None and not scorecard_df.empty:
                    st.markdown("#### 📝 Scorecard Metadata")
                    st.dataframe(scorecard_df, use_container_width=True)

            st.markdown("<br>", unsafe_allow_html=True)

# ------------------ PLAYER STATS ------------------
elif page == "Player Stats":
    st.header("🌟 Player & Team Statistics")

    icc_ranks = get_table_data("icc_ranks")
    player_stats = get_table_data("player_stats")
    team_results = get_table_data("team_results")

    col1, col2, col3 = st.columns(3)
    col1.metric("🏆 Ranked Players", icc_ranks.shape[0])
    col2.metric("📊 Player Records", player_stats.shape[0])
    col3.metric("📌 Team Results", team_results.shape[0])

    tab1, tab2, tab3 = st.tabs(["🏆 ICC Ranks", "🧑‍💻 Player Stats", "👥 Team Results"])

    with tab1:
        if icc_ranks.empty:
            st.info("No ICC ranks found in the database.")
        else:
            st.subheader("🏆 ICC Player Rankings")
            st.dataframe(icc_ranks)

    with tab2:
        if player_stats.empty:
            st.info("No player stats found in the database.")
        else:
            st.subheader("📊 Player Batting & Bowling Stats")
            format_filter = st.selectbox("Filter by Format", ["All"] + player_stats["format_type"].unique().tolist())
            if format_filter != "All":
                player_stats = player_stats[player_stats["format_type"] == format_filter]
            st.dataframe(player_stats)
            if "runs" in player_stats.columns:
                st.bar_chart(player_stats.groupby("player_name")["runs"].sum().sort_values(ascending=False).head(10))

    with tab3:
        if team_results.empty:
            st.info("No team results found in the database.")
        else:
            st.subheader("👥 Team Match Results")
            st.dataframe(team_results)

# ------------------ SQL ANALYTICS ------------------
elif page == "SQL Analytics":
    st.header("📊 SQL Analytics & Insights")

    # Intro Row with Metrics
    col1, col2, col3 = st.columns(3)
    col1.metric("📝 Total Queries", 25)
    col2.metric("🔎 Beginner Queries", 8)
    col3.metric("⚡ Advanced Queries", 10)
    st.markdown("---")

    queries = {
        # Beginner Level
        "1. Players representing India": """
            SELECT player_name, playing_role, batting_style, bowling_style
            FROM player_info
            WHERE country = 'India';
        """,
        "2. Recent cricket matches": """
            SELECT CONCAT(Team_1, ' vs ', Team_2) AS match_desc, Team_1, Team_2, Venue, Start_Time
            FROM recent_matches
            WHERE Start_Time >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
            ORDER BY Start_Time DESC;
        """,
        "3. Top 10 ODI run scorers": """
            SELECT player_id, player_name, matches, innings, runs, average, strike_rate, hundreds, fifties
            FROM player_stats
            WHERE format_type = 'ODI'
            ORDER BY runs DESC
            LIMIT 10;
        """,
        "4. Venues with capacity > 50,000": """
            SELECT venue_name, city, country, capacity
            FROM venues
            WHERE capacity > 50000
            ORDER BY capacity DESC;
        """,
        "5. Matches won by team": """
            SELECT team_name,
            COUNT(*) as total_wins
            FROM (
            SELECT Team_1 as team_name
            FROM recent_matches 
            WHERE Status LIKE '%won by%' AND Status LIKE CONCAT(Team_1, '%')
            UNION ALL
            SELECT Team_2 as team_name
            FROM recent_matches 
            WHERE Status LIKE '%won by%' AND Status LIKE CONCAT(Team_2, '%')
            ) as wins
            GROUP BY team_name
            ORDER BY total_wins DESC;
        """,
        "6. Player count by role": """
            SELECT playing_role, COUNT(*) AS player_count
            FROM player_info
            GROUP BY playing_role;
        """,
        "7. Highest individual score by format": """
            SELECT format_type, MAX(runs) AS highest
            FROM player_stats
            GROUP BY format_type;
        """,
        "8. Series started in 2024": """
            SELECT Series_ID, Series_Name, Start_Date, End_Date, Year
            FROM series_list
        """,

        # Intermediate Level
        "9. All-rounders with 1000+ runs & 50+ wickets": """
            SELECT player_name, runs, wickets, format_type
            FROM player_stats
            WHERE runs > 1000 AND wickets > 50;
        """,
        "10. Last 20 completed matches": """
            SELECT CONCAT(Team_1, ' vs ', Team_2) AS match_desc, Team_1, Team_2, Status, Venue, Start_Time
            FROM recent_matches
            ORDER BY Start_Time DESC
            LIMIT 20;
        """,
        "11. Player performance across formats": """
            SELECT player_name, 
                SUM(CASE WHEN format_type='Test' THEN runs ELSE 0 END) AS test_runs,
                SUM(CASE WHEN format_type='ODI' THEN runs ELSE 0 END) AS odi_runs,
                SUM(CASE WHEN format_type='T20I' THEN runs ELSE 0 END) AS t20i_runs,
                AVG(average) AS overall_avg
            FROM player_stats
            GROUP BY player_name
            HAVING COUNT(DISTINCT format_type) >= 2;
        """,
        "12. Team home vs away wins": """
            SELECT t.team_name,
            SUM(CASE WHEN t.team_name = rm.Team_1 AND rm.Status LIKE CONCAT(t.team_name, '%') THEN 1 ELSE 0 END) AS home_wins,
            SUM(CASE WHEN t.team_name = rm.Team_2 AND rm.Status LIKE CONCAT(t.team_name, '%') THEN 1 ELSE 0 END) AS away_wins
            FROM recent_matches rm
            JOIN (
            SELECT Team_1 AS team_name FROM recent_matches
            UNION
            SELECT Team_2 FROM recent_matches
            ) t ON t.team_name IN (rm.Team_1, rm.Team_2)
            GROUP BY t.team_name
            ORDER BY t.team_name;
        """,
        "13. 100+ run partnerships (adjacent batsmen)": """
            SELECT 
                bat1_name AS batsman1,
                bat1_position AS position1,
                bat2_name AS batsman2,
                bat2_position AS position2,
                total_runs AS partnership_runs,
                innings_id,
                team_name,
                match_id
            FROM live_partnerships
            WHERE 
                total_runs >= 100
                AND is_adjacent = 1
            ORDER BY 
                total_runs DESC;
        """,
        "14. Bowling performance by venue": """
        SELECT 
            ps.player_name,
            vm.venue_name,
            AVG(ps.economy_rate) AS avg_economy,
            SUM(ps.wickets) AS total_wickets,
            COUNT(*) AS matches
        FROM player_stats ps
        JOIN venue_matches vm 
            ON ps.format_type = vm.match_format
        WHERE ps.overs_bowled >= 4
        GROUP BY ps.player_name, vm.venue_name
        HAVING matches >= 3
        ORDER BY total_wickets DESC;
        """,
        "15. Player performance in close matches": """
            SELECT series_name, match_type, team_1, team_2, status, start_time
            FROM recent_matches
            WHERE status LIKE '%won by % run%'
                AND CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(status, ' ', -2), ' ', 1) AS UNSIGNED) < 50
                OR status LIKE '%won by % wkt%'
                AND CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(status, ' ', -2), ' ', 1) AS UNSIGNED) < 5
            ORDER BY start_time DESC;
        """,
        "16. Batting performance by year (since 2020)": """
            SELECT 
                player_name,
                format_type,
                AVG(runs) AS avg_runs,
                AVG(strike_rate) AS avg_sr,
                COUNT(*) AS records
            FROM player_stats
            GROUP BY player_name, format_type
            HAVING records >= 5;
        """,
        # Advanced Level
        "17. Toss win advantage analysis": """
            SELECT toss_decision, 
                ROUND(100 * SUM(CASE WHEN toss_winner = match_winner THEN 1 ELSE 0 END)/COUNT(*),2) AS win_pct
            FROM live_matches
            GROUP BY toss_decision;
        """,
        "18. Most economical bowlers (ODI & T20)": """
            SELECT bowler_name, AVG(economy_rate) AS avg_economy, SUM(wickets) AS total_wickets, COUNT(*) AS matches
            FROM player_bowling_stats
            WHERE format IN ('ODI', 'T20I')
            GROUP BY bowler_name
            HAVING matches >= 10 AND AVG(overs_bowled) >= 2
            ORDER BY avg_economy ASC;
        """,
        "19. Most consistent batsmen (since 2022)": """
            SELECT player_name, AVG(runs) AS avg_runs, STDDEV(runs) AS run_stddev
            FROM player_batting_stats
            WHERE balls_faced >= 10 AND match_date >= '2022-01-01'
            GROUP BY player_name
            HAVING COUNT(*) >= 5
            ORDER BY run_stddev ASC;
        """,
        "20. Player matches & averages by format": """
            SELECT player_name, 
                SUM(CASE WHEN format='Test' THEN 1 ELSE 0 END) AS test_matches,
                SUM(CASE WHEN format='ODI' THEN 1 ELSE 0 END) AS odi_matches,
                SUM(CASE WHEN format='T20I' THEN 1 ELSE 0 END) AS t20_matches,
                AVG(CASE WHEN format='Test' THEN batting_average END) AS test_avg,
                AVG(CASE WHEN format='ODI' THEN batting_average END) AS odi_avg,
                AVG(CASE WHEN format='T20I' THEN batting_average END) AS t20_avg
            FROM player_batting_stats
            GROUP BY player_name
            HAVING (test_matches + odi_matches + t20_matches) >= 20;
        """,
        "21. Player performance ranking": """
            SELECT player_name,
                (runs * 0.01 + batting_average * 0.5 + strike_rate * 0.3) +
                (wickets * 2 + (50-bowling_average)*0.5 + (6-economy_rate)*2) +
                (catches + stumpings) AS total_points
            FROM player_stats
            ORDER BY total_points DESC
            LIMIT 20;
        """,
        "22. Head-to-head team analysis (last 3 years)": """
            SELECT team1, team2, COUNT(*) AS matches_played,
                SUM(CASE WHEN winner = team1 THEN 1 ELSE 0 END) AS team1_wins,
                SUM(CASE WHEN winner = team2 THEN 1 ELSE 0 END) AS team2_wins,
                AVG(CASE WHEN winner = team1 THEN victory_margin END) AS team1_avg_margin,
                AVG(CASE WHEN winner = team2 THEN victory_margin END) AS team2_avg_margin
            FROM matches
            WHERE match_date >= DATE_SUB(CURDATE(), INTERVAL 3 YEAR)
            GROUP BY team1, team2
            HAVING matches_played >= 5;
        """,
        "23. Recent player form & momentum": """
            SELECT player_name,
                AVG(CASE WHEN match_num > 5 THEN runs END) AS avg_last_10,
                AVG(CASE WHEN match_num <= 5 THEN runs END) AS avg_last_5,
                SUM(CASE WHEN runs >= 50 THEN 1 ELSE 0 END) AS scores_50plus,
                STDDEV(runs) AS consistency_score
            FROM (
                SELECT player_name, runs, ROW_NUMBER() OVER (PARTITION BY player_name ORDER BY match_date DESC) AS match_num
                FROM player_batting_stats
            ) AS recent
            WHERE match_num <= 10
            GROUP BY player_name;
        """,
        "24. Best batting partnerships": """
            SELECT p1.player_name AS batsman1, p2.player_name AS batsman2,
                AVG(partnership_runs) AS avg_runs,
                SUM(CASE WHEN partnership_runs > 50 THEN 1 ELSE 0 END) AS fifty_plus,
                MAX(partnership_runs) AS highest,
                COUNT(*) AS total_partnerships
            FROM partnerships
            JOIN player_info p1 ON partnerships.batsman1_id = p1.id
            JOIN player_info p2 ON partnerships.batsman2_id = p2.id
            WHERE ABS(partnerships.batsman1_pos - partnerships.batsman2_pos) = 1
            GROUP BY batsman1, batsman2
            HAVING total_partnerships >= 5
            ORDER BY avg_runs DESC;
        """,
        "25. Player performance time-series analysis": """
            SELECT player_name, 
                CONCAT(YEAR(match_date), '-Q', QUARTER(match_date)) AS quarter,
                AVG(runs) AS avg_runs, AVG(strike_rate) AS avg_sr, COUNT(*) AS matches
            FROM player_batting_stats
            GROUP BY player_name, quarter
            HAVING COUNT(*) >= 3
            ORDER BY player_name, quarter;
        """
    }
    query_choice = st.selectbox("🔍 Choose a query to run:", list(queries.keys()))
if st.button("▶ Run Query"):
    df = run_query(queries[query_choice])

    if df.empty:
        st.warning("No results found for this query.")
    else:
        st.markdown("""
        <div style="background:#ffffff;
                    padding:15px;
                    border-radius:12px;
                    box-shadow:0 2px 10px rgba(0,0,0,0.1);
                    margin-bottom:15px;">
            <h4 style="color:#0B6623;">📊 Query Results</h4>
        </div>
        """, unsafe_allow_html=True)

        st.dataframe(df)

        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()

        if len(numeric_cols) > 0:
            st.subheader("📈 Quick Visualization")

            # ✅ Choose the right index safely
            index_col = 'player_name' if 'player_name' in df.columns else df.columns[0]

            try:
                # Don't reuse 'player_id' as both index and column
                st.bar_chart(df.set_index(index_col)[numeric_cols[0]])
            except Exception as e:
                st.error(f"⚠️ Could not render chart: {e}")


# ------------------ CRUD OPERATIONS ------------------
elif page == "CRUD Operations":
    st.header("⚙️ CRUD Operations (Admin Panel)")

    tables = [
        "icc_ranks",
        "live_match_info",
        "live_venues",
        "live_teams",
        "live_batting_stats",
        "live_bowling_stats",
        "live_scorecard_metadata",
        "live_commentary",
        "live_series",
        "player_info",
        "player_stats",
        "recent_matches",
        "schedules",
        "series_list",
        "team_results",
        "team_standings",
        "venues"
    ]
    crud_table = st.selectbox("Select Table", tables)
    action = st.radio("Action", ["Create", "Read", "Update", "Delete"], key="crud_action")

    # Helper: Get primary key of selected table
    def get_primary_key(table_name):
        conn = get_mysql_conn()
        cursor = conn.cursor()
        cursor.execute(f"SHOW KEYS FROM `{table_name}` WHERE Key_name = 'PRIMARY'")
        result = cursor.fetchone()
        conn.close()
        if result:
            return result[4]   # Column name of PK
        return None

    # READ
    if action == "Read":
        st.subheader(f"📖 Data from `{crud_table}`")
        df = get_table_data(crud_table)
        st.dataframe(df)

    # CREATE
    elif action == "Create":
        with st.expander("➕ Insert New Row"):
            st.write(f"Insert new row into `{crud_table}`")
            new_values = st.text_area("Enter comma-separated values:")

            if st.button("Insert Row"):
                conn = get_mysql_conn()
                cursor = conn.cursor()
                cursor.execute(f"DESCRIBE `{crud_table}`;")
                col_count = len(cursor.fetchall())
                conn.close()

                placeholders = ",".join(["%s"] * col_count)
                values = tuple([v.strip() for v in new_values.split(",")])

                try:
                    modify_query(f"INSERT INTO `{crud_table}` VALUES ({placeholders})", values)
                    st.success("✅ Row inserted successfully!")
                except Exception as e:
                    st.error(f"❌ Insert failed: {e}")

    # UPDATE
    elif action == "Update":
        with st.expander("✏️ Update Existing Row"):
            pk_col = get_primary_key(crud_table)
            if not pk_col:
                st.error(f"⚠️ No primary key found for `{crud_table}`. Cannot update.")
            else:
                conn = get_mysql_conn()
                cursor = conn.cursor()
                cursor.execute(f"DESCRIBE `{crud_table}`;")
                valid_columns = [col[0] for col in cursor.fetchall()]
                conn.close()

                record_id = st.text_input(f"Enter {pk_col} of row to update:")
                column = st.selectbox("Column to update:", valid_columns)
                new_value = st.text_input("New value:")


                if st.button("Update Row"):
                    try:
                        modify_query(
                            f"UPDATE `{crud_table}` SET `{column}`=%s WHERE `{pk_col}`=%s",
                            (new_value, record_id)
                        )
                        st.success("✅ Row updated successfully!")
                    except Exception as e:
                        st.error(f"❌ Update failed: {e}")

    # DELETE
    elif action == "Delete":
        with st.expander("🗑️ Delete Row"):
            pk_col = get_primary_key(crud_table)
            if not pk_col:
                st.error(f"⚠️ No primary key found for `{crud_table}`. Cannot delete.")
            else:
                record_id = st.text_input(f"Enter {pk_col} of row to delete:")
                if st.button("Delete Row"):
                    try:
                        modify_query(
                            f"DELETE FROM `{crud_table}` WHERE `{pk_col}`=%s",
                            (record_id,)
                        )
                        st.success("✅ Row deleted successfully!")
                    except Exception as e:
                        st.error(f"❌ Delete failed: {e}")
# ========== FOOTER ==========
st.markdown("---")
st.markdown("""
<div style='text-align: center; padding: 20px;'>
    <p style='color: #667eea; font-size: 1.2em; font-weight: 600;'>
        🏏 Mizaru's Live Cricket Info🏏
    </p>
    <p style='color: var(--text-secondary); font-size: 0.9em;'>
        Real-time Cricket Analytics & Statistics Platform
    </p>
</div>
""", unsafe_allow_html=True)
