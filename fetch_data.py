"""
@author: Brian Leon

"""

from dotenv import load_dotenv
import os
import psycopg2
import pandas as pd
import asyncio

def connect_to_supabase():
    """
    Connects to the Supabase database using environment variables.
    Returns the connection and cursor objects.
    """
    load_dotenv()
    supabase_user = os.getenv("SUPABASE_USER")
    supabase_password = os.getenv("SUPABASE_PASSWORD")
    supabase_host = os.getenv("SUPABASE_HOST")

    # Connecting to the remote Supabase database
    conn = psycopg2.connect(
        user=supabase_user,
        password=supabase_password,
        host=supabase_host,
        port=5432,
        database="postgres"
    )
    cursor = conn.cursor()
    return conn, cursor

def getEvents():
    """
    Retrieves events data from the Supabase database and returns it as a DataFrame.
    """
    conn, cursor = connect_to_supabase()
    cursor.execute(""" 
        SELECT minute, second, team_id, is_shot, is_goal,
                type, outcome_type, period, match_id   
        FROM events; 
    """)
    records = cursor.fetchall()
    events = pd.DataFrame(records, columns=[desc[0] for desc in cursor.description])
    conn.close()
    return events


def getMatches():
    """
    Retrieves matches data from the Supabase database and returns it as a DataFrame.
    """
    conn, cursor = connect_to_supabase()
    cursor.execute(""" 
        SELECT hometeam, awayteam, fthg, ftag 
        FROM football_matches;        
    """)
    records = cursor.fetchall()
    matches = pd.DataFrame(records, columns=[desc[0] for desc in cursor.description])
    conn.close()
    return matches

def getStandings():
    """
    Retrieves standings data from the Supabase database and returns it as a DataFrame.
    """
    conn, cursor = connect_to_supabase()
    cursor.execute(""" 
        SELECT pts, gd  
        FROM soccer_data; 
    """)
    records = cursor.fetchall()
    standings = pd.DataFrame(records, columns=[desc[0] for desc in cursor.description])
    conn.close()
    return standings
