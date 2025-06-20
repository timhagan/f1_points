import sys
import os
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)
import datetime
from src.data_prep import functions

def main():
    # Determine the year and fetch points DataFrames
    year = datetime.datetime.now().year
    result = functions.get_event_points(
        event_name=None,  # Use the most recent event
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