import fastf1
import pandas as pd
import datetime
from utils import get_most_recent_session_event_name, get_session_by_name_session_type_year, calculate_driver_points, calculate_constructor_points

session_types       = ["Race", "Sprint", "Qualifying", "Sprint Qualifying"]
# session_type_lower  = session_type.lower()
today               = datetime.datetime.now(datetime.timezone.utc)
year                = today.year

# fastf1.Cache.enable_cache(f'C:\\Users\\timot\\OneDrive\\Documents\\GitHub\\f1_points\\.cache\\{session_type_lower}_points')
fastf1.Cache.enable_cache(f'C:\\Users\\timot\\OneDrive\\Documents\\GitHub\\f1_points\\.cache\\all_points')

sessions         = pd.read_csv(f"C:\\Users\\timot\\OneDrive\\Documents\\GitHub\\f1_points\\.data\\sessions_{year}.csv")

point_dfs = []

for session_type in session_types:
    most_recent_session_event_name = get_most_recent_session_event_name(sessions, session_type, today=today, year=year)
    most_recent_session_results    = get_session_by_name_session_type_year(most_recent_session_event_name, session_type, year=year)
    if session_type in ["Sprint Qualifying", "Qualifying"]:
        session_driver_points = calculate_driver_points(most_recent_session_results, session_type)
        point_dfs.append(session_driver_points)
    elif session_type in ["Race", "Sprint"]:
        session_driver_points, session_constructor_points = calculate_driver_points(most_recent_session_results, session_type)
        point_dfs.append(session_driver_points)
        point_dfs.append(session_constructor_points)

