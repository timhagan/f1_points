import fastf1
import pandas as pd
import datetime
from src.data_prep import functions

def main():
    # Get all past events and process them
    try:
        past_event_names = functions.get_past_race_event_names() or []
    except Exception:
        past_event_names = []
    for past_event_name in past_event_names:
        try:
            functions.get_event_points(event_name=past_event_name)
        except Exception:
            # ignore errors in event processing
            pass

if __name__ == '__main__':
    main()