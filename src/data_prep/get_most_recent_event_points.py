import fastf1
import pandas as pd
import datetime
from src.data_prep import functions

def main():
    # Determine the year and fetch points DataFrames
    year = datetime.datetime.now().year
    drivers_points, constructors_points = functions.get_event_points(
        event_name=None,  # Use the most recent event
        return_dfs=True
    )
    # Write the outputs
    drivers_points.to_csv(f'../f1_points/driver_points_{year}_most_recent.csv', index=False)
    constructors_points.to_csv(f'../f1_points/constructor_points_{year}_most_recent.csv', index=False)

if __name__ == '__main__':
    main()