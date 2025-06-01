import fastf1
import pandas as pd
import datetime
from utils import calculate_driver_points, get_most_recent_session

session_type        = "Sprint Qualifying"
session_type_lower  = session_type.lower()
today               = datetime.datetime.now(datetime.timezone.utc)
year                = today.year

fastf1.Cache.enable_cache(f'C:\\Users\\timot\\OneDrive\\Documents\\GitHub\\f1_points\\.cache\\{session_type_lower}_points')

sessions         = pd.read_csv(f"C:\\Users\\timot\\OneDrive\\Documents\\GitHub\\f1_points\\.data\\sessions_{year}.csv")
sessions_limited = sessions[sessions['SessionName']==session_type]

most_recent_session_results         = get_most_recent_session(sessions_limited, session_type, today=today, year=year)
most_recent_session_driver_points   = calculate_driver_points(most_recent_session_results, session_type)

most_recent_session_driver_points.to_csv(f'C:\\Users\\timot\\OneDrive\\Documents\\GitHub\\f1_points\\.data\\most_recent_{session_type_lower}_driver_points.csv', index=False)
