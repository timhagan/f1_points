import fastf1
import pandas as pd
import datetime
import functions
from importlib import reload

reload(functions)

past_event_names = functions.get_past_race_event_names()

for past_event_name in past_event_names:
    functions.get_event_points(
        event_name=past_event_name
    )