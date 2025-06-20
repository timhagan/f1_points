import fastf1
import pandas as pd
import datetime
import os

def get_past_race_event_names(today=datetime.datetime.now(datetime.timezone.utc)):
    """
    Get all past event names from the FastF1 cache.
    Args:
        today (datetime): The current date and time. Default is the current UTC time.
    Returns:
        pd.Series: Series containing all past event names.    """
    # Set cache path relative to project root
    cache_path = os.path.join(os.path.dirname(__file__), '..', '..', '.cache', 'event_points')
    fastf1.Cache.enable_cache(cache_path)

    # Get all events from the cache
    sessions_path = os.path.join(os.path.dirname(__file__), '..', '..', 'data', f'sessions_{today.year}.csv')
    sessions_df = pd.read_csv(sessions_path)
    races_df    = sessions_df[sessions_df['SessionName'] == 'Race'].copy()

    # Convert EventDate to EventDateUtc
    races_df['EventDateUtc'] = pd.to_datetime(races_df['EventDate'], errors='coerce', utc=True)

    # Set to end of day (23:59:59.999999)
    races_df['EventDateUtc'] = races_df['EventDateUtc'].dt.normalize() + pd.Timedelta(days=1) - pd.Timedelta(microseconds=1)

    # Filter out future events
    races_df['EventDateUtc'] = pd.to_datetime(races_df['EventDateUtc'], errors='coerce', utc=True)
    races_df = races_df[races_df['EventDateUtc'] < today]

    return races_df['EventName'].unique()


def get_most_recent_session_df(sessions, session_type="Race", today=datetime.datetime.now(datetime.timezone.utc), year=datetime.datetime.now(datetime.timezone.utc).year):
    """
    Get the most recent session of a given type (e.g Race, Sprint, Qualifying, etc.).
    Args:
        sessions (pd.DataFrame): DataFrame containing session information retrieved using .
        session_type (str): The type of session to retrieve. Options include 'Practice 1', 'Practice 2', 'Practice 3', 'Sprint', 'Sprint Shootout', 'Sprint Qualifying', 'Qualifying', 'Race'. Note that the old 'sprint' event format from 2021 and 2022 originally used the name 'Sprint Qualifying' before renaming these sessions to just 'Sprint'. The official schedule for 2021 now lists all these sessions as 'Sprint' and FastF1 will therefore return all these session as 'Sprint'. When querying for a specific session, FastF1 will also accept the 'Sprint Qualifying'/'SQ' identifier instead of only 'Sprint'/'S' for backwards compatibility.
        today (datetime): The current date and time. Default is the current UTC time.
        year (int): The current year. Default is the current year.
    """
    sessions_limited = sessions[sessions['SessionName']==session_type].copy()
    # Convert 'SessionDateUtc' to datetime objects, handling potential errors by setting invalid parsing to NaT
    sessions_limited['SessionDateUtc'] = pd.to_datetime(sessions_limited['SessionDateUtc'], errors='coerce', utc=True)

    # Filter out any rows where SessionDateUtc could not be parsed (NaT)
    sessions_limited = sessions_limited.dropna(subset=['SessionDateUtc'])

    # Ensure 'today' is timezone-aware (UTC) to match 'SessionDateUtc'
    today_utc = today.replace(tzinfo=datetime.timezone.utc)

    # Filter sessions that have already occurred
    past_sessions = sessions_limited[sessions_limited['SessionDateUtc'] < today_utc]

    # Get the most recent session from the past sessions
    if not past_sessions.empty:
        # Select the most recent row as a DataFrame (not Series)
        most_recent_idx = past_sessions['SessionDateUtc'].idxmax()
        most_recent_session_df = past_sessions.loc[[most_recent_idx]]
        print(f"Most recent {session_type} session:")
        print(most_recent_session_df)
    else:
        print(f"No {session_type} sessions for year {year} found before today.")

    return most_recent_session_df

def get_session_df(sessions, event_name=None, session_type="Race", today=datetime.datetime.now(datetime.timezone.utc), year=datetime.datetime.now(datetime.timezone.utc).year):
    """
    Get the most recent session of a given type (e.g Race, Sprint, Qualifying, etc.).
    Args:
        sessions (pd.DataFrame): DataFrame containing session information retrieved using .
        session_type (str): The type of session to retrieve. Options include 'Practice 1', 'Practice 2', 'Practice 3', 'Sprint', 'Sprint Shootout', 'Sprint Qualifying', 'Qualifying', 'Race'. Note that the old 'sprint' event format from 2021 and 2022 originally used the name 'Sprint Qualifying' before renaming these sessions to just 'Sprint'. The official schedule for 2021 now lists all these sessions as 'Sprint' and FastF1 will therefore return all these session as 'Sprint'. When querying for a specific session, FastF1 will also accept the 'Sprint Qualifying'/'SQ' identifier instead of only 'Sprint'/'S' for backwards compatibility.
        today (datetime): The current date and time. Default is the current UTC time.
        year (int): The current year. Default is the current year.
    """
    if event_name is not None:
        sessions_limited    = sessions[sessions['SessionName']==session_type].copy()
        selected_session_df = sessions_limited[sessions_limited['EventName'] == event_name].copy()
    elif event_name is None:
        selected_session_df = get_most_recent_session_df(sessions, session_type=session_type, today=today, year=year)
    else:
        raise ValueError("Either provide a valid event name or use the default to get the most recent session.")
    return selected_session_df

def get_session_types_list(event_format):
    if event_format == "conventional":
        return ["Qualifying", "Race"]
    elif event_format == "sprint_qualifying":
        return ["Sprint", "Qualifying", "Race"]
    else:
        raise ValueError(f"Event format '{event_format}' is not supported. Supported formats are 'conventional' and 'sprint_qualifying'.")

def calculate_teammate_race_points(session_results, session_type):
    session_results[f'Teammate{session_type}Points'] = 0

    for index, row in session_results.iterrows():
        driver_id = row['DriverId']
        team_id = row['TeamId']
        
        # Get the teammate's position
        teammate_row = session_results.loc[(session_results['DriverId'] != driver_id) & (session_results['TeamId'] == team_id)]
        
        if not teammate_row.empty:
            teammate_position = teammate_row.iloc[0]['Position']
            
            # If the current driver outqualifies their teammate, assign 5 points
            if row['Position'] < teammate_position:
                session_results.at[index, f'Teammate{session_type}Points'] = 5

    return session_results

def calculate_teammate_quali_points(session_results):
    session_results[f'TeammateQualiPoints'] = 0

    for index, row in session_results.iterrows():
        driver_id = row['DriverId']
        team_id = row['TeamId']
        
        # Get the teammate's position
        teammate_row = session_results.loc[(session_results['DriverId'] != driver_id) & (session_results['TeamId'] == team_id)]
        
        if not teammate_row.empty:
            teammate_position = teammate_row.iloc[0]['GridPosition']
            
            # If the current driver outqualifies their teammate, assign 5 points
            if row['GridPosition'] < teammate_position:
                session_results.at[index, f'TeammateQualiPoints'] = 5

    return session_results


def calculate_places_gained_points(session_results, session_type, multiplier=2):
    """
    Calculate the points for places gained in a session.
    Args:
        session_results (pd.DataFrame): DataFrame containing session results.
        multiplier (int): The multiplier for points per place gained. Default is 2.
    """
    session_results[f"PlacesGained{session_type}"]       = session_results['GridPosition'] - session_results['Position']
    session_results[f"PlacesGained{session_type}Floor"]  = session_results[f"PlacesGained{session_type}"].clip(lower=0)
    session_results[f"PlacesGained{session_type}Points"] = session_results[f"PlacesGained{session_type}Floor"] * multiplier
    return session_results


def calculate_pole_points(session_results):
    session_results['PolePoints'] = 0
    session_results.loc[session_results['Position'] == 1.0, 'PolePoints'] = 10
    return session_results


def calculate_intermediate_driver_points(session_results, session_type):
    if session_type == "Sprint":
        session_results[f"{session_type}Points"] = session_results["Points"]
        session_results = calculate_places_gained_points(session_results, session_type, 1)
        session_results = calculate_teammate_race_points(session_results, session_type)
    elif session_type == "Qualifying":
        session_results = calculate_pole_points(session_results)
    elif session_type == "Race":
        session_results[f"{session_type}Points"] = session_results["Points"]
        session_results = calculate_places_gained_points(session_results, session_type, 2)
        session_results = calculate_teammate_race_points(session_results, session_type)
        session_results = calculate_teammate_quali_points(session_results) # This adds points for beating your teammate in qualifying
    else:
        raise ValueError(f"Session type '{session_type}' is not supported for point calculation in session provided.")
    return session_results


def merge_points_dataframes(dictionary_of_dfs, merge_key='DriverId'):
    """
    Merge multiple DataFrames in a dictionary into a single DataFrame.
    Args:
        dictionary_of_dfs (dict): Dictionary where keys are DataFrame names and values are DataFrames.
    Returns:
        pd.DataFrame: Merged DataFrame containing all the data.
    """
    # Merge all driver points dataframes on DriverId and select only DriverId and points columns
    merged_points_df = None

    # Iterate through each DataFrame in the dictionary
    for session_type, df in dictionary_of_dfs.items():
        # Get the points column name (it varies by session type)
        points_cols = [col for col in df.columns if 'Points' in col and col != merge_key]
        
        # Select DriverId and the points column(s)
        session_df = df[[merge_key] + points_cols]

        if merged_points_df is None:
            merged_points_df = session_df
        else:
            merged_points_df = merged_points_df.merge(session_df, on=merge_key, how='outer')

    # Fill NaN values with 0 for drivers who didn't participate in all sessions
    if merged_points_df is not None:
        merged_points_df = merged_points_df.fillna(0)
    return merged_points_df

def calculate_final_driver_points(session_results, event_format):
    if event_format == "sprint_qualifying":
        
        session_results["TotalSprintRacePoints"] = (
            session_results['SprintPoints'] + 
            session_results[f'PlacesGainedSprintPoints'] +
            session_results[f'TeammateSprintPoints'])
        
        session_results["TotalRaceQualifyingPoints"] = (
            session_results['PolePoints'] +
            session_results[f'TeammateQualiPoints'])
        
        session_results["TotalRacePoints"] = (
            session_results['RacePoints'] + 
            session_results[f'PlacesGainedRacePoints'] +
            session_results[f'TeammateRacePoints']) 
        
        session_results["TotalDriverPoints"] = (
            session_results["TotalSprintRacePoints"] +
            session_results["TotalRaceQualifyingPoints"] +
            session_results["TotalRacePoints"])

    elif event_format == "conventional":
        
        session_results["TotalRaceQualifyingPoints"] = (
            session_results['PolePoints'] +
            session_results[f'TeammateQualiPoints'] 
        )
        
        session_results["TotalRacePoints"] = (
            session_results['RacePoints'] + 
            session_results[f'PlacesGainedRacePoints'] +
            session_results[f'TeammateRacePoints'])
        
        session_results["TotalDriverPoints"] = (
            session_results["TotalRaceQualifyingPoints"] +
            session_results["TotalRacePoints"])
    else:
        raise ValueError(f"Event format '{event_format}' is not supported for point calculation in session provided.")
    return session_results

def slim_driver_points_df(session_results, event_format):
    """
    Slim the driver points DataFrame to only include relevant columns for the session type.
    Args:
        session_results (pd.DataFrame): DataFrame containing session results.
        event_format (str): The type of event. Options include 'sprint_qualifying' or 'conventional'
    """
    if event_format == "sprint_qualifying":
        return session_results[['DriverId',
                                'TotalDriverPoints',
                                'TotalSprintRacePoints',
                                'SprintPoints',
                                'PlacesGainedSprintPoints',
                                'TeammateSprintPoints',
                                'TotalRaceQualifyingPoints',
                                'PolePoints',
                                'TeammateQualiPoints',
                                'TotalRacePoints',
                                'RacePoints',
                                'PlacesGainedRacePoints',
                                'TeammateRacePoints']].copy()
    elif event_format == "conventional":
        return session_results[['DriverId',
                                'TotalDriverPoints',
                                'TotalRaceQualifyingPoints',
                                'PolePoints',
                                'TeammateQualiPoints',
                                'TotalRacePoints',
                                'RacePoints',
                                'PlacesGainedRacePoints',
                                'TeammateRacePoints']].copy()
    else:
        raise ValueError(f"Event format '{event_format}' is not supported for point calculation in session provided.")

def calculate_constructor_finishing_points(session_results, session_type):
    """Calculate the constructor finishing points based on the session type. If one car finishes the race, the constructor gets 2 points. If both cars finish the race, the constructor gets 5 points."""
    session_results[f'Constructor{session_type}FinishingPoints'] = 0
    if session_type == "Sprint":
        
        # Group by TeamId and count how many cars finished
        finished_counts = session_results[session_results['Status'] != 'Retired'].groupby('TeamId').size()
        
        for team_id, count in finished_counts.items():
            if count == 1:
                session_results.loc[session_results['TeamId'] == team_id, 'ConstructorSprintFinishingPoints'] = 2
            elif count >= 2:
                session_results.loc[session_results['TeamId'] == team_id, 'ConstructorSprintFinishingPoints'] = 5

        # Get unique constructor finishing points by team
        constructor_finishing_df = session_results.groupby('TeamId')['ConstructorSprintFinishingPoints'].first().reset_index()

    elif session_type == "Race":
        # Group by TeamId and count how many cars finished
        finished_counts = session_results[session_results['Status'] != 'Retired'].groupby('TeamId').size()

        for team_id, count in finished_counts.items():
            if count == 1:
                session_results.loc[session_results['TeamId'] == team_id, 'ConstructorRaceFinishingPoints'] = 2
            elif count >= 2:
                session_results.loc[session_results['TeamId'] == team_id, 'ConstructorRaceFinishingPoints'] = 5

        # Get unique constructor finishing points by team
        constructor_finishing_df = session_results.groupby('TeamId')['ConstructorRaceFinishingPoints'].first().reset_index()

    else:
        raise ValueError(f"Session type '{session_type}' is not supported for constructor finishing point calculation.")
    return constructor_finishing_df


def get_aggregated_results(session_results, session_type):
    """
    Get the aggregated results for the constructor points and places gained points.
    Args:
        session_results (pd.DataFrame): DataFrame containing session results.
        session_type (str): The type of session to retrieve. Options include 'Sprint' or 'Race'.
    """
    # Calculate the constructor points based on the session type
    if session_type == "Sprint":
        team_aggregated_results = session_results.groupby('TeamId').agg(
            TotalSprintPoints=pd.NamedAgg(column='Points', aggfunc='sum'),
            PlacesGainedSprintPoints=pd.NamedAgg(column='PlacesGainedSprintFloor', aggfunc='sum')
        ).reset_index()
    elif session_type == "Race":
        team_aggregated_results = session_results.groupby('TeamId').agg(
            TotalRacePoints=pd.NamedAgg(column='Points', aggfunc='sum'),
            PlacesGainedRacePoints=pd.NamedAgg(column='PlacesGainedRaceFloor', aggfunc='sum')
        ).reset_index()
    team_aggregated_results[f'Constructor{session_type}Points'] = team_aggregated_results[f'Total{session_type}Points'] + team_aggregated_results[f'PlacesGained{session_type}Points']
    return team_aggregated_results

def slim_constructor_points_df(constructor_points_df, event_format):
    """
    Slim the constructor points DataFrame to only include relevant columns for the session type.
    Args:
        constructor_points_df (pd.DataFrame): DataFrame containing constructor points.
        event_format (str): The type of session to retrieve. Options include 'Sprint' or 'Race'.
    """
    if event_format == "sprint_qualifying":
        constructor_points_df['TotalConstructorPoints'] = (
            constructor_points_df['TotalConstructorSprintPoints'] + 
            constructor_points_df['TotalConstructorRacePoints'])
        return constructor_points_df[['TeamId', 
                                      'TotalConstructorPoints',
                                      'TotalConstructorSprintPoints',
                                      'ConstructorSprintPoints', 
                                      'TotalSprintPoints', 
                                      'PlacesGainedSprintPoints',
                                      'ConstructorSprintFinishingPoints',
                                      'TotalConstructorRacePoints',
                                      'ConstructorRacePoints',
                                      'TotalRacePoints', 
                                      'PlacesGainedRacePoints',
                                      'ConstructorRaceFinishingPoints']].copy()
    elif event_format == "conventional":
        constructor_points_df['TotalConstructorPoints'] = (
            constructor_points_df['TotalConstructorRacePoints'])
        return constructor_points_df[['TeamId', 
                                      'TotalConstructorPoints',
                                      'TotalConstructorRacePoints',
                                      'ConstructorRacePoints',
                                      'TotalRacePoints', 
                                      'PlacesGainedRacePoints',
                                      'ConstructorRaceFinishingPoints']].copy()
    else:
        raise ValueError(f"Session type '{event_format}' is not supported for constructor points DataFrame.")

def calculate_constructor_points(session_results, session_type):
    if session_type in ["Sprint", "Race"]:
        constructor_finishing_df        = calculate_constructor_finishing_points(session_results, session_type=session_type)
        team_aggregated_results         = get_aggregated_results(session_results, session_type)
        constructor_points_df           = constructor_finishing_df.merge(team_aggregated_results, on='TeamId', how='left')
        
        constructor_points_df[f"TotalConstructor{session_type}Points"] = constructor_points_df[f'Constructor{session_type}FinishingPoints'] + constructor_points_df[f'Constructor{session_type}Points']
    else:
        raise ValueError(f"Session type '{session_type}' is not supported for constructor point calculation.")
    return constructor_points_df


def get_event_points(event_name=None, year=datetime.datetime.now(datetime.timezone.utc).year, return_dfs=False):
    """
    Get the event points for a specific event in a specific year.
    Args:
        year (int): The year of the event.
        event_name (str): The name of the event.
    """
    import fastf1
    import pandas as pd
    import datetime
    from src.data_prep import functions
    from importlib import reload

    reload(functions)    
    today = datetime.datetime.now(datetime.timezone.utc)
    year  = today.year

    # Set cache path relative to project root
    cache_path = os.path.join(os.path.dirname(__file__), '..', '..', '.cache', 'event_points')
    fastf1.Cache.enable_cache(cache_path)

    sessions_path = os.path.join(os.path.dirname(__file__), '..', '..', 'data', f'sessions_{year}.csv')
    sessions_df = pd.read_csv(sessions_path)

    selected_session_df   = functions.get_session_df(sessions_df, event_name=event_name)

    SELECTED_EVENT_NAME   = selected_session_df['EventName'].values[0]
    SELECTED_EVENT_FORMAT = selected_session_df['EventFormat'].values[0]

    session_types = functions.get_session_types_list(SELECTED_EVENT_FORMAT)

    session_dfs = {}

    for session_type in session_types:
        f1_session      = fastf1.get_session(year, SELECTED_EVENT_NAME, session_type) 
        f1_session.load(laps=False, telemetry=False, weather=False, messages=False) 
        
        event_results             = f1_session.results
        session_dfs[session_type] = event_results

    driver_points_dfs       = {}
    constructor_points_dfs  = {}

    for session_type in session_types:
        if session_type in ["Qualifying"]:
            session_driver_points = functions.calculate_intermediate_driver_points(session_dfs[session_type], session_type)
            driver_points_dfs[session_type] = session_driver_points
        elif session_type in ["Race", "Sprint"]:
            session_driver_points = functions.calculate_intermediate_driver_points(session_dfs[session_type], session_type)
            session_constructor_points = functions.calculate_constructor_points(session_dfs[session_type], session_type)
            driver_points_dfs[session_type] = session_driver_points
            constructor_points_dfs[session_type] = session_constructor_points
        else:
            raise ValueError(f"Unknown session type: {session_type}")
        
    merged_driver_points        = functions.merge_points_dataframes(driver_points_dfs, merge_key="DriverId")
    merged_constructor_points   = functions.merge_points_dataframes(constructor_points_dfs, merge_key="TeamId")

    driver_points_df = functions.calculate_final_driver_points(merged_driver_points, SELECTED_EVENT_FORMAT)

    driver_points_df_slim       = functions.slim_driver_points_df(driver_points_df, event_format=SELECTED_EVENT_FORMAT)
    constructor_points_df_slim  = functions.slim_constructor_points_df(merged_constructor_points, event_format=SELECTED_EVENT_FORMAT)

    driver_points_df_slim.loc[:,"EventName"]      = SELECTED_EVENT_NAME
    constructor_points_df_slim.loc[:,"EventName"] = SELECTED_EVENT_NAME

    driver_points_df_slim.to_csv(f"..\\f1_points\\.data\\driver_points_{year}_{SELECTED_EVENT_NAME}.csv", index=False)
    constructor_points_df_slim.to_csv(f"..\\f1_points\\.data\\constructor_points_{year}_{SELECTED_EVENT_NAME}.csv", index=False)

    if return_dfs:
        return driver_points_df_slim, constructor_points_df_slim

