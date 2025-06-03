import fastf1
import pandas as pd
import datetime
import functions
from importlib import reload

reload(functions)

year = datetime.datetime.now().year

drivers_points, constructors_points = functions.get_event_points(
    event_name=None, # Set to None to use the latest event
    return_dfs=True,
)

drivers_points.to_csv(f'../f1_points/driver_points_{year}_most_recent.csv', index=False)
constructors_points.to_csv(f'../f1_points/constructor_points_{year}_most_recent.csv', index=False)