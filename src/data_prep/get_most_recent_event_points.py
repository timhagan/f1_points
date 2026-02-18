import sys
import os
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)
import datetime
from src.data_prep import functions

def main():
    # Determine the year and fetch points DataFrames
    year = functions.get_latest_sessions_year(preferred_year=datetime.datetime.now().year)
    if year is None:
        print("No sessions file available; skipping most recent event point extraction.")
        return

    result = functions.get_event_points(
        event_name=None,  # Use the most recent event
        year=year,
        return_dfs=True
    )
    if result is not None:
        drivers_points, constructors_points = result
        # Write the outputs
        drivers_points.to_csv(f'data/driver_points_{year}_most_recent.csv', index=False)
        constructors_points.to_csv(f'data/constructor_points_{year}_most_recent.csv', index=False)
    else:
        print("No event points data returned.")

if __name__ == '__main__':
    main()
