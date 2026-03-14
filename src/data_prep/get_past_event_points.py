import os
import sys
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)
from src.data_prep import functions

def main():
    # Get all past events and process them
    try:
        past_event_names = functions.get_past_race_event_names().tolist() or []
    except Exception as exc:
        print(f"Failed to load past events: {exc}")
        past_event_names = []

    successful_events = 0
    failed_events = 0

    for past_event_name in past_event_names:
        try:
            functions.get_event_points(event_name=past_event_name)
            successful_events += 1
        except Exception as exc:
            failed_events += 1
            print(f"Skipping {past_event_name}: {exc}")

    if past_event_names:
        print(
            f"Processed {len(past_event_names)} past events "
            f"({successful_events} succeeded, {failed_events} failed)."
        )
    else:
        print("No past events found to process.")

if __name__ == '__main__':
    main()
