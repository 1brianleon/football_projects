"""
@author: Brian Leon

"""

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
    """
    Pydantic model representing Events data.

    Attributes:
        event_id (int): The unique identifier for the event.
        match_id (int): The identifier for the match.
        minute (int): The minute of the event.
        second (Optional[float]): The second of the event (optional).
        expanded_minute (int): The expanded minute of the event.
        team_id (int): The identifier for the team associated with the event.
        player_id (int): The identifier for the player associated with the event.
        related_player_id (Optional[float]): The identifier for the related player (optional).
        x (float): The x-coordinate of the event.
        y (float): The y-coordinate of the event.
        end_x (Optional[float]): The ending x-coordinate of the event (optional).
        end_y (Optional[float]): The ending y-coordinate of the event (optional).
        qualifiers (List[dict]): The list of qualifiers associated with the event.
        is_touch (bool): Indicates whether the event is a touch or not.
        blocked_x (Optional[float]): The blocked x-coordinate of the event (optional).
        blocked_y (Optional[float]): The blocked y-coordinate of the event (optional).
        goal_mouth_z (Optional[float]): The goal mouth z-coordinate of the event (optional).
        goal_mouth_y (Optional[float]): The goal mouth y-coordinate of the event (optional).
        is_shot (bool): Indicates whether the event is a shot or not.
        card_type (bool): Indicates the type of card associated with the event.
        is_goal (bool): Indicates whether the event is a goal or not.
        type (str): The type of the event.
        outcome_type (str): The outcome type of the event.
        period (str): The period of the event.
    """
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
    """
    Pydantic model representing Player data.

    Attributes:
        player_id (int): The unique identifier for the player.
        shirt_no (int): The shirt number of the player.
        name (str): The name of the player.
        age (int): The age of the player.
        height (int): The height of the player.
        weight (int): The weight of the player.
        team_id (int): The identifier for the team associated with the player.
    """
    player_id: int
    shirt_no: int
    name: str
    age: int
    height: int
    weight: int
    team_id: int 

class Matches(BaseModel):
    """
    Pydantic model representing Match data.

    Attributes:
        match_id (int): The unique identifier for the match.
        match_date (str): The date of the match.
        home_score (int): The score of the home team.
        away_score (int): The score of the away team.
        home_team_name (str): The name of the home team.
        away_team_name (str): The name of the away team.
        match_minutes (int): The total minutes of the match.
        match_minutes_expanded (int): The expanded minutes of the match.
        region (str): The region where the match took place.
        competition (str): The competition in which the match occurred.
        season (str): The season of the match.
    """
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
    """
    Pydantic model representing Lineup data.

    Attributes:
        match_id (int): The unique identifier for the match.
        team_id (int): The identifier for the team associated with the lineup.
        player_id (int): The identifier for the player in the lineup.
        player_name (str): The name of the player in the lineup.
        player_position (str): The position of the player in the lineup.
        field (str): The field designation for the player in the lineup.
        first_eleven (bool): Indicates whether the player is in the starting eleven or not.
        subbed_in_player_id (Optional[float]): The identifier for the player who was subbed in (optional).
        subbed_out_period (Optional[str]): The period when the player was subbed out (optional).
        subbed_out_expanded_min (Optional[float]): The expanded minute when the player was subbed out (optional).
        subbed_in_period (Optional[str]): The period when the player was subbed in (optional).
        subbed_in_expanded_min (Optional[float]): The expanded minute when the player was subbed in (optional).
        subbed_out_player_id (Optional[float]): The identifier for the player who was subbed out (optional).
    """
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
    """
    Inserts events into a Supabase table.

    Args:
        df (DataFrame): The DataFrame containing events data.
        supabase: The Supabase client.
    """
    all_events = [
        Events(**x).model_dump()
        for x in df.to_dict(orient='records')
    ]
    execution = supabase.table('events').upsert(all_events).execute()

def insert_players(team_info, supabase):
    """
    Inserts players into a Supabase table.

    Args:
        team_info (list): A list of dictionaries containing team and player information.
        supabase: The Supabase client.
    """
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
    """
    Inserts matches into a Supabase table.

    Args:
        match_info (list): A list of dictionaries containing match information.
        supabase: The Supabase client.
    """
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
    """
    Inserts lineups into a Supabase table.

    Args:
        df (DataFrame): The DataFrame containing lineup data.
        supabase: The Supabase client.
    """
    all_lineups = [
        Lineups(**x).model_dump()
        for x in df.to_dict(orient='records')
    ]
    execution = supabase.table('lineups').upsert(all_lineups).execute()


def extract_period(x):
    """
    Helper function to extract match period.

    Args:
        x: The input data, which can be a dictionary or another type.

    Returns:
        str: The display name of the match period if available, otherwise NaN.
    """
    if isinstance(x, dict):
        return x.get('displayName', np.nan)
    else: 
        return x
    

def extract_info_from_url(whoscored_url):
    """
    Helper function to extract region, competition, and season from a given URL.

    Args:
        whoscored_url (str): The URL from which information needs to be extracted.

    Returns:
        tuple: A tuple containing region, competition, and season extracted from the URL.
    """
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
    

def prepend_base_url(match_urls):
    """
    Helper function to format and prepend base URL to match URLs.

    Args:
        match_urls (list): A list of dictionaries containing match URLs.

    Returns:
        list: A list of formatted match URLs with base URL prepended.
    """
    return ['https://www.whoscored.com' + url['url'] for url in match_urls]


def getLeagueUrls(minimize_window=True):
    """
    Function to get all League URLs from WhoScored.com.

    Args:
        minimize_window (bool): Whether to minimize the browser window (default is True).

    Returns:
        dict: A dictionary containing league names as keys and their corresponding URLs as values.
    """
    # Initialize a Chrome webdriver instance
    driver = webdriver.Chrome()
    # Minimize the browser window if specified
    if minimize_window:
        driver.minimize_window()

    # Navigate to the main URL
    driver.get(main_url)

    # Initialize lists to store league names and URLs
    league_names = []
    league_urls = []

    # Find the number of tournaments listed
    n_tournaments = len(BeautifulSoup(driver.find_element(By.ID, 'popular-tournaments-list').get_attribute('innerHTML')).findAll('li'))
    
    # Iterate through each tournament to extract its name and URL
    for i in range(n_tournaments):
        league_name = driver.find_element(By.XPATH, '//*[@id="popular-tournaments-list"]/li['+str(i+1)+']/a').text
        league_link = driver.find_element(By.XPATH, '//*[@id="popular-tournaments-list"]/li['+str(i+1)+']/a').get_attribute('href')
        league_names.append(league_name)
        league_urls.append(league_link)
    
    # Special handling for the Russian Premier League
    for link in league_urls:
        if 'Russia' in link:
            r_index = league_urls.index(link)
            
    league_names[r_index] = 'Russian Premier League'
    
    # Create a dictionary mapping league names to their URLs
    leagues = {}
    for name,link in zip(league_names,league_urls):
        leagues[name] = link
    # Close the webdriver
    driver.close()
    return leagues


def getUrlData(driver):
    """
    Function to extract match URLs from a webpage.

    Args:
        driver: The WebDriver instance.

    Returns:
        list: A list of dictionaries containing match URLs.
    """
    matches_ls = []
    while True:
        # Find all table rows containing match data
        table_rows = driver.find_elements(By.CLASS_NAME, 'divtable-row')
        # Check if there are no more table rows
        if len(table_rows) == 0:
            # If the next button is disabled, break the loop
            if('is-disabled' in driver.find_element(By.XPATH, '//*[@id="date-controller"]/a[1]').get_attribute('class').split()):
                break
            # Click on the next button to load more matches
            else:
                driver.find_element(By.XPATH, '//*[@id="date-controller"]/a[1]').click()
        # Iterate through each table row to extract match URLs
        for row in table_rows:
            match_dict = {}
            element = BeautifulSoup(row.get_attribute('innerHTML'), features='lxml')
            link_tag = element.find("a", {"class":"result-1 rc"})
            if type(link_tag) is not type(None):
                match_dict['url'] = link_tag.get("href")
            matches_ls.append(match_dict)

        # Click on the previous month button to navigate to the previous month's matches
        prev_month = driver.find_element(By.XPATH, '//*[@id="date-controller"]/a[1]').click()
        time.sleep(2)

        # Check if there are no more matches available
        if driver.find_element(By.XPATH, '//*[@id="date-controller"]/a[1]').get_attribute('title') == 'No data for previous week':
            # Get the remaining matches from the current month
            table_rows = driver.find_elements(By.CLASS_NAME, 'divtable-row')
            for row in table_rows:
                match_dict = {}
                element = BeautifulSoup(row.get_attribute('innerHTML'), features='lxml')
                link_tag = element.find("a", {"class":"result-1 rc"})
                if type(link_tag) is not type(None):
                    match_dict['url'] = link_tag.get("href")
                matches_ls.append(match_dict)
            break
    # Filter out any None values and return the list of match URLs
    matches_ls = list(filter(None, matches_ls))

    return matches_ls


def getMatchUrls(comp_urls, competition, season, maximize_window=True):
    """
    Function to get match URLs for a specific competition and season.

    Args:
        comp_urls (dict): A dictionary containing competition names as keys and their URLs as values.
        competition (str): The name of the competition.
        season (str): The season for which match URLs are required.
        maximize_window (bool): Whether to maximize the browser window (default is True).

    Returns:
        list: A list of dictionaries containing match URLs.
    """
    # Initialize a Chrome webdriver instance
    driver = webdriver.Chrome()
    # Maximize the browser window if specified
    if maximize_window:
        driver.maximize_window()
    # Get the URL for the specified competition
    comp_url = comp_urls[competition]
    driver.get(comp_url)
    time.sleep(5)
    # Extract available seasons
    seasons = driver.find_element(By.XPATH, '//*[@id="seasons"]').get_attribute('innerHTML').split(sep='\n')
    seasons = [i for i in seasons if i]
    
    # Loop through each season to find the desired one
    for i in range(1, len(seasons)+1):
        if driver.find_element(By.XPATH, '//*[@id="seasons"]/option['+str(i)+']').text == season:
            # Click on the desired season
            driver.find_element(By.XPATH, '//*[@id="seasons"]/option['+str(i)+']').click()
            
            time.sleep(5)
            try:
                # Extract available stages
                stages = driver.find_element(By.XPATH, '//*[@id="stages"]').get_attribute('innerHTML').split(sep='\n')
                stages = [i for i in stages if i]
                
                all_urls = []
                # Loop through each stage to extract match URLs
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
    # If the desired season is not found, raise an exception
    season_names = [re.search(r'\>(.*?)\<',season).group(1) for season in seasons]
    driver.close() 
    print('Seasons available: {}'.format(season_names))
    raise('Season Not Found.')


def scrape_whoscored_data(whoscored_url, driver):
    """
    Function to scrape data from WhoScored.com.

    Args:
        whoscored_url (str): The URL of the match.
        driver: The WebDriver instance.

    Returns:
        str: Success message if scraping is successful.
    """
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

# Load environment variables       
load_dotenv()

# API key for Supabase
api_key = os.getenv("SUPABASE_KEY")

# URL for Supabase project
project_url = os.getenv("SUPABASE_URL")

# Create Supabase client instance
supabase = create_client(project_url, api_key)

#Set up the Selenium driver 
driver = webdriver.Chrome()

#defining our main URL
main_url = 'https://www.whoscored.com/'

#Obtaining all the league URLs
league_urls = getLeagueUrls()

#Obtaining all the match URLs from a specific season and competition
match_urls = getMatchUrls(comp_urls=league_urls, competition='Bundesliga', season='2023/2024')

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