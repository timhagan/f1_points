import os
import sys
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)
from src.data_prep import functions

def main():
    # Get all past events and process them
    try:
        past_event_names = functions.get_past_race_event_names().tolist() or []
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