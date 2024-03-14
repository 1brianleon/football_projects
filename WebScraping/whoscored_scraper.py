#Import Libraries 
import json 
import time 
import sys
import re
import os
from dotenv import load_dotenv
import numpy as np
import pandas as pd 
from bs4 import BeautifulSoup
from pydantic import BaseModel
from typing import List, Optional 
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, WebDriverException
from selenium.webdriver.common.by import By
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
    region: str
    competition: str
    season: str

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
        Events(**x).model_dump()
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
            'region': match['region'],
            'season': match['season'],
            'competition': match['competition'],
        })
    execution = supabase.table('matches').upsert(matches).execute()

def insert_lineups(df, supabase):
    all_lineups = [
        Lineups(**x).model_dump()
        for x in df.to_dict(orient='records')
    ]
    execution = supabase.table('lineups').upsert(all_lineups).execute()


#Helper function to extract match period 
def extract_period(x):
    if isinstance(x, dict):
        return x.get('displayName', np.nan)
    else: 
        return x
    

#Helper Function to extract region, competition, and season from a given URL
def extract_info_from_url(whoscored_url):
    # Define a regular expression pattern to match the desired portions
    pattern = r'/([^/]+)-([^/]+)-(\d{4}-\d{4})-'

    # Search for the pattern in the URL
    match = re.search(pattern, whoscored_url)

    if match:
        # Extract the matched portions
        region = match.group(1)
        competition = match.group(2)
        season = match.group(3)
        return region, competition, season
    else:
        print("Pattern not found in the URL:", whoscored_url)
        return None, None, None
    

#Helper function to format and prepend match URLs
def prepend_base_url(match_urls):
    return ['https://www.whoscored.com' + url['url'] for url in match_urls]


#Function to get all League URLs from WhoScored.com
def getLeagueUrls(minimize_window=True):
    
    driver = webdriver.Chrome()
    
    if minimize_window:
        driver.minimize_window()
        
    driver.get(main_url)
    league_names = []
    league_urls = []
    n_tournaments = len(BeautifulSoup(driver.find_element(By.ID, 'popular-tournaments-list').get_attribute('innerHTML')).findAll('li'))
    for i in range(n_tournaments):
        league_name = driver.find_element(By.XPATH, '//*[@id="popular-tournaments-list"]/li['+str(i+1)+']/a').text
        league_link = driver.find_element(By.XPATH, '//*[@id="popular-tournaments-list"]/li['+str(i+1)+']/a').get_attribute('href')
        league_names.append(league_name)
        league_urls.append(league_link)
        
    for link in league_urls:
        if 'Russia' in link:
            r_index = league_urls.index(link)
            
    league_names[r_index] = 'Russian Premier League'
    
    leagues = {}
    for name,link in zip(league_names,league_urls):
        leagues[name] = link
    driver.close()
    return leagues


#Function to get all internal URL data 
def getUrlData(driver):

    matches_ls = []
    while True:
        table_rows = driver.find_elements(By.CLASS_NAME, 'divtable-row')
        if len(table_rows) == 0:
            if('is-disabled' in driver.find_element(By.XPATH, '//*[@id="date-controller"]/a[1]').get_attribute('class').split()):
                break
            else:
                driver.find_element(By.XPATH, '//*[@id="date-controller"]/a[1]').click()
        for row in table_rows:
            match_dict = {}
            element = BeautifulSoup(row.get_attribute('innerHTML'), features='lxml')
            link_tag = element.find("a", {"class":"result-1 rc"})
            if type(link_tag) is not type(None):
                match_dict['url'] = link_tag.get("href")
            matches_ls.append(match_dict)
                
        prev_month = driver.find_element(By.XPATH, '//*[@id="date-controller"]/a[1]').click()
        time.sleep(2)
        if driver.find_element(By.XPATH, '//*[@id="date-controller"]/a[1]').get_attribute('title') == 'No data for previous week':
            table_rows = driver.find_elements(By.CLASS_NAME, 'divtable-row')
            for row in table_rows:
                match_dict = {}
                element = BeautifulSoup(row.get_attribute('innerHTML'), features='lxml')
                link_tag = element.find("a", {"class":"result-1 rc"})
                if type(link_tag) is not type(None):
                    match_dict['url'] = link_tag.get("href")
                matches_ls.append(match_dict)
            break
    
    matches_ls = list(filter(None, matches_ls))

    return matches_ls


#Function to get all Match URLs
def getMatchUrls(comp_urls, competition, season, maximize_window=True):

    driver = webdriver.Chrome()
    
    if maximize_window:
        driver.maximize_window()
    
    comp_url = comp_urls[competition]
    driver.get(comp_url)
    time.sleep(5)
    
    seasons = driver.find_element(By.XPATH, '//*[@id="seasons"]').get_attribute('innerHTML').split(sep='\n')
    seasons = [i for i in seasons if i]
    
    
    for i in range(1, len(seasons)+1):
        if driver.find_element(By.XPATH, '//*[@id="seasons"]/option['+str(i)+']').text == season:
            driver.find_element(By.XPATH, '//*[@id="seasons"]/option['+str(i)+']').click()
            
            time.sleep(5)
            try:
                stages = driver.find_element(By.XPATH, '//*[@id="stages"]').get_attribute('innerHTML').split(sep='\n')
                stages = [i for i in stages if i]
                
                all_urls = []
            
                for i in range(1, len(stages)+1):
                    if competition == 'Champions League' or competition == 'Europa League':
                        if 'Group Stages' in driver.find_element(By.XPATH, '//*[@id="stages"]/option['+str(i)+']').text or 'Final Stage' in driver.find_element_by_xpath('//*[@id="stages"]/option['+str(i)+']').text:
                            driver.find_element(By.XPATH, '//*[@id="stages"]/option['+str(i)+']').click()
                            time.sleep(5)
                            
                            driver.execute_script("window.scrollTo(0, 400)") 
                            
                            match_urls = getUrlData(driver)
                            
                            all_urls += match_urls
                        else:
                            continue
                    
                    elif competition == 'Major League Soccer':
                        if 'Grp. ' not in driver.find_element(By.XPATH, '//*[@id="stages"]/option['+str(i)+']').text: 
                            driver.find_element(By.XPATH, '//*[@id="stages"]/option['+str(i)+']').click()
                            time.sleep(5)
                        
                            driver.execute_script("window.scrollTo(0, 400)")
                            
                            match_urls = getUrlData(driver)
                            
                            all_urls += match_urls
                        else:
                            continue
                        
                    else:
                        driver.find_element(By.XPATH, '//*[@id="stages"]/option['+str(i)+']').click()
                        time.sleep(5)
                    
                        driver.execute_script("window.scrollTo(0, 400)")
                        
                        match_urls = getUrlData(driver)
                        
                        all_urls += match_urls
                
            except NoSuchElementException:
                all_urls = []
                
                driver.execute_script("window.scrollTo(0, 400)")
                
                match_urls = getUrlData(driver)
                
                all_urls += match_urls
            
            # Remove duplicates from all_urls
            remove_dup = [dict(t) for t in {tuple(sorted(d.items())) for d in all_urls}]
            remove_dup = list(filter(None, remove_dup))
            
            driver.close() 
    
            return remove_dup
     
    season_names = [re.search(r'\>(.*?)\<',season).group(1) for season in seasons]
    driver.close() 
    print('Seasons available: {}'.format(season_names))
    raise('Season Not Found.')


#Function to scrape data from WhoScored
def scrape_whoscored_data(whoscored_url, driver):
    # Opening the URL in the browser
    driver.get(whoscored_url)

    match_id = int(whoscored_url.split("/")[-3])

    # Extracting region, competition, and season from the URL
    region, competition, season = extract_info_from_url(whoscored_url)
    
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
                'match_minutes_expanded': matchDict['expandedMaxMinute'],
                'region': region,
                'competition': competition,
                'season': season,
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
    

#Get Supabase client
load_dotenv()
api_key = os.getenv("SUPABASE_KEY")
project_url = os.getenv("SUPABASE_URL")
supabase = create_client(project_url, api_key)

#Set up the Selenium driver 
driver = webdriver.Chrome()

#defining our main URL
main_url = 'https://www.whoscored.com/'

#Obtaining all the league URLs
league_urls = getLeagueUrls()

#Obtaining all the match URLs from a specific season and competition
match_urls = getMatchUrls(comp_urls=league_urls, competition='Premier League', season='2023/2024')

#Format all match URLs
formatted_urls = prepend_base_url(match_urls)

#Scrape data for each match URL 
for url in formatted_urls:
    print(url)
    scrape_whoscored_data(url, driver)
    sys.stdout.flush()  # Flush output buffer
    time.sleep(2)


#Clean up 
driver.quit()

# Print completion message
print("Script execution completed.")