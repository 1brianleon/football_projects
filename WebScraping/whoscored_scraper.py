#Import Libraries 
import json 
import time 
import sys
import numpy as np
import pandas as pd 
from bs4 import BeautifulSoup
from pydantic import BaseModel
from typing import List, Optional 
from selenium import webdriver
from supabase import create_client, Client

#Define Pydantic models
class Events(BaseModel):
    event_id: int
    match_id: int
    minute: int
    second: Optional[float] = None
    expanded_minute: int
    team_id: int
    player_id: int
    related_player_id: Optional[float] = None
    x: float
    y: float
    end_x: Optional[float] = None
    end_y: Optional[float] = None
    qualifiers: List[dict]
    is_touch: bool
    blocked_x: Optional[float] = None
    blocked_y: Optional[float] = None
    goal_mouth_z: Optional[float] = None
    goal_mouth_y: Optional[float] = None
    is_shot: bool
    card_type: bool
    is_goal: bool
    type: str
    outcome_type: str
    period: str

class Player(BaseModel):
    player_id: int
    shirt_no: int
    name: str
    age: int
    height: int
    weight: int
    team_id: int 

class Matches(BaseModel):
    match_id: int
    match_date: str
    home_score: int
    away_score: int
    home_team_name: str
    away_team_name: str
    match_minutes: int
    match_minutes_expanded: int

class Lineups(BaseModel):
    match_id: int
    team_id: int
    player_id: int
    player_name: str
    player_position: str
    field: str
    first_eleven: bool
    subbed_in_player_id: Optional[float] = None
    subbed_out_period: Optional[str] = None
    subbed_out_expanded_min: Optional[float] = None
    subbed_in_period: Optional[str] = None
    subbed_in_expanded_min: Optional[float] = None
    subbed_out_player_id: Optional[float] = None


#Define database insertion functions 
def insert_events(df, supabase):
    all_events = [
        Events(**x).dict()
        for x in df.to_dict(orient='records')
    ]
    execution = supabase.table('events').upsert(all_events).execute()

def insert_players(team_info, supabase):
    players = []
    for team in team_info:
        for player in team['players']:
            players.append({
                'player_id': player['playerId'],
                'team_id': team['team_id'],
                'shirt_no': player['shirtNo'],
                'name': player['name'],
                'age': player['age'],
                'height': player['height'],
                'weight': player['weight'],
            })
    execution = supabase.table('players').upsert(players).execute()

def insert_matches(match_info, supabase):
    matches = []
    for match in match_info:
        matches.append({
            'match_id': match['match_id'],
            'match_date': match['match_date'],
            'home_score': match['home_score'],
            'away_score': match['away_score'],
            'home_team_name': match['home_team_name'],
            'away_team_name': match['away_team_name'],
            'match_minutes': match['match_minutes'],
            'match_minutes_expanded': match['match_minutes_expanded'],
        })
    execution = supabase.table('matches').upsert(matches).execute()

def insert_lineups(df, supabase):
    all_lineups = [
        Lineups(**x).dict()
        for x in df.to_dict(orient='records')
    ]
    execution = supabase.table('lineups').upsert(all_lineups).execute()


#Helper function to extract match period 
def extract_period(x):
    if isinstance(x, dict):
        return x.get('displayName', np.nan)
    else: 
        return x
    

#Function to scrape data from WhoScored
def scrape_whoscored_data(whoscored_url, driver):
    # Opening the URL in the browser
    driver.get(whoscored_url)

    match_id = int(whoscored_url.split("/")[-3])
    
    # Creating a BeautifulSoup object to parse the HTML
    soup = BeautifulSoup(driver.page_source, 'html.parser')

    # Selecting an element using BeautifulSoup
    element = soup.select_one('script:-soup-contains("matchCentreData")')

    # Check if the element is found before proceeding
    if element is not None:
        try:
            # Extracting and parsing JSON data from the selected element
            matchDict = json.loads(element.text.split("matchCentreData: ")[1].split(',\n')[0])
        
            match_events = matchDict['events']
            
            df = pd.DataFrame(match_events)
        
            df.dropna(subset='playerId', inplace=True)
        
            df = df.where(pd.notnull(df), None)
        
            df = df.drop('eventId', axis=1)
        
            df['match_id'] = match_id
        
            df = df.rename(
                {
                    'id': 'event_id',
                    'expandedMinute': 'expanded_minute',
                    'outcomeType': 'outcome_type',
                    'isTouch': 'is_touch',
                    'playerId': 'player_id',
                    'teamId': 'team_id',
                    'endX': 'end_x',
                    'endY': 'end_y',
                    'blockedX': 'blocked_x',
                    'blockedY': 'blocked_y',
                    'goalMouthZ': 'goal_mouth_z',
                    'goalMouthY': 'goal_mouth_y',
                    'isShot': 'is_shot',
                    'cardType': 'card_type',  
                    'relatedPlayerId' : 'related_player_id',    
                },
                axis=1
            )
        
            df['period'] = df['period'].apply(lambda x: x['displayName'])
            df['type'] = df['type'].apply(lambda x: x['displayName'])
            df['outcome_type'] = df['outcome_type'].apply(lambda x: x['displayName'])
        
            if 'is_goal' not in df.columns:
                df['is_goal'] = False
        
            if 'is_card' not in df.columns:
                df['is_card'] = False
                df['card_type'] = False

            df = df[~(df['type'] == "OffsideGiven")]
        
            df = df[[
                'event_id', 'match_id', 'minute', 'second', 'expanded_minute', 'team_id', 'player_id', 'related_player_id', 'x', 'y', 'end_x', 'end_y',
                'qualifiers', 'is_touch', 'blocked_x', 'blocked_y', 'goal_mouth_z', 'goal_mouth_y', 'is_shot',
                'card_type', 'is_goal', 'type', 'outcome_type', 'period'
            ]]
        
            df[['event_id', 'match_id', 'minute', 'team_id', 'player_id', 'expanded_minute']] = df[['event_id', 'match_id', 'minute', 'team_id', 'player_id', 'expanded_minute']].astype(int)
            df[['second', 'x', 'y', 'end_x', 'end_y', 'related_player_id']] = df[['second', 'x', 'y', 'end_x', 'end_y', 'related_player_id']].astype(float)
            df[['is_shot', 'is_goal', 'card_type']] = df[['is_shot', 'is_goal', 'card_type']].astype(bool)
        
            df['is_goal'] = df['is_goal'].fillna(False)
            df['is_shot'] = df['is_shot'].fillna(False)
        
            for column in df.columns:
                if df[column].dtype == np.float64 or df[column].dtype == np.float32:
                    df[column] = np.where(
                        np.isnan(df[column]),
                        None,
                        df[column]
                    )

            insert_events(df, supabase)
        
            team_info = []
            team_info.append({
                'team_id': matchDict['home']['teamId'],
                'name': matchDict['home']['name'],
                'country_name': matchDict['home']['countryName'],
                'manager_name': matchDict['home']['managerName'],
                'players': matchDict['home']['players'],
            })
            
            team_info.append({
                'team_id': matchDict['away']['teamId'],
                'name': matchDict['away']['name'],
                'country_name': matchDict['away']['countryName'],
                'manager_name': matchDict['away']['managerName'],
                'players': matchDict['away']['players'],
            })
        
            insert_players(team_info, supabase)

            match_date = matchDict['startDate'][:10]
    
            home_score, away_score = map(int, matchDict['score'].split(' : '))
        
            match_info = []
            match_info.append({
                'match_id': match_id,
                'match_date': match_date,
                'home_score': home_score,
                'away_score': away_score,
                'home_team_name': matchDict['home']['name'],
                'away_team_name': matchDict['away']['name'],
                'match_minutes': matchDict['maxMinute'],
                'match_minutes_expanded': matchDict['expandedMaxMinute']
            })
        
            insert_matches(match_info, supabase)

            lineups = []
            for match in match_info:
                for team in team_info:
                    for player in team['players']:
                        lineups.append({
                            'match_id': match['match_id'],
                            'team_id': team['team_id'],
                            'player_id': player['playerId'],
                            'player_name': player['name'],
                            'player_position': player['position'],
                            'field': player['field'],
                            'first_eleven': player.get('isFirstEleven', None),  # Handling missing key
                            'subbed_in_player_id': player.get('subbedInPlayerId', None),  # Handling missing key
                            'subbed_out_period': player.get('subbedOutPeriod', None),  # Handling missing key
                            'subbed_out_expanded_min': player.get('subbedOutExpandedMinute', None),  # Handling missing key
                            'subbed_in_period': player.get('subbedInPeriod', None),  # Handling missing key
                            'subbed_in_expanded_min': player.get('subbedInExpandedMinute', None),  # Handling missing key
                            'subbed_out_player_id': player.get('subbedOutPlayerId', None),  # Handling missing key
                        })
            
            lineup_df = pd.DataFrame(lineups)
            lineup_df = lineup_df.where(pd.notnull(lineup_df), None)
            lineup_df['first_eleven'] = lineup_df['first_eleven'].fillna(False).astype(bool)
            lineup_df['subbed_out_period'] = lineup_df['subbed_out_period'].apply(extract_period)
            lineup_df['subbed_in_period'] = lineup_df['subbed_in_period'].apply(extract_period)

            for column in lineup_df.columns:
                if lineup_df[column].dtype == np.float64 or lineup_df[column].dtype == np.float32:
                    lineup_df[column] = np.where(
                        np.isnan(lineup_df[column]),
                        None,
                        lineup_df[column]
                    )

            insert_lineups(lineup_df, supabase)
            
            return print('Success')
        except (KeyError, IndexError, ValueError, json.JSONDecodeError) as e:
            print(f"Error processing data for URL {whoscored_url}: {str(e)}")
    else:
        print(f"No Match Centre Data found for URL: {whoscored_url}")
    

#Function to get all individual match URLs from the fixtures page 
def get_match_urls(whoscored_url, driver):
    driver.get(whoscored_url)
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    all_urls = soup.select('a[href*="\/Live\/"]')
    return list(set(['https://www.whoscored.com' + x.attrs['href'] for x in all_urls]))


#Get Supabase client
supabase_password = 'elde5y6u7ycHuiGV'
project_url = 'https://kacepynzaervoccjqxxz.supabase.co'
api_key = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImthY2VweW56YWVydm9jY2pxeHh6Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3MDUxMDE1NDYsImV4cCI6MjAyMDY3NzU0Nn0.mjN4MTyfX5GygOeoZaUV-IigQf47fzuQYeERkmcCmRI'
supabase = create_client(project_url, api_key)

#Set up the Selenium driver 
driver = webdriver.Chrome()

#Ask user for the WhoScored team fixtures URL 
whoscored_url = input("Enter the WhoScored team fixtures URL: ")

#Get all individual match URLs 
match_urls = get_match_urls(whoscored_url, driver)

#Scrape data for each match URL 
for url in match_urls:
    print(url)
    scrape_whoscored_data(url, driver)
    sys.stdout.flush()  # Flush output buffer
    time.sleep(2)


#Clean up 
driver.quit()

# Print completion message
print("Script execution completed.")