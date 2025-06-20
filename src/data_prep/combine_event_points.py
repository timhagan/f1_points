import pandas as pd
import datetime
import os
import sys
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)
from src.data_prep import functions

def main():
    # Reload functions in case of dynamic updates
    # Get past event names
    past_event_names = functions.get_past_race_event_names()
    # Prepare accumulators
    driver_points_df = pd.DataFrame()
    constructor_points_df = pd.DataFrame()
    year = datetime.datetime.now().year
    
    for past_event_name in past_event_names:
        driver_csv_path = os.path.join(os.path.dirname(__file__), '..', '..', 'data', f'driver_points_{year}_{past_event_name}.csv')
        try:
            driver_points_df_slim = pd.read_csv(driver_csv_path)
            if not driver_points_df_slim.empty:
                driver_points_df = pd.concat([driver_points_df, driver_points_df_slim], ignore_index=True)
        except FileNotFoundError:
            continue
            
        constructor_csv_path = os.path.join(os.path.dirname(__file__), '..', '..', 'data', f'constructor_points_{year}_{past_event_name}.csv')
        try:
            constructor_points_df_slim = pd.read_csv(constructor_csv_path)
            if not constructor_points_df_slim.empty:
                constructor_points_df = pd.concat([constructor_points_df, constructor_points_df_slim], ignore_index=True)
        except FileNotFoundError:
            continue
            
    driver_points_df.reset_index(drop=True, inplace=True)
    constructor_points_df.reset_index(drop=True, inplace=True)
    
    # Save to data directory relative to project root
    driver_output_path = os.path.join(os.path.dirname(__file__), '..', '..', 'data', f'driver_points_{year}_current.csv')
    constructor_output_path = os.path.join(os.path.dirname(__file__), '..', '..', 'data', f'constructor_points_{year}_current.csv')
    
    driver_points_df.to_csv(driver_output_path, index=False)
    constructor_points_df.to_csv(constructor_output_path, index=False)

if __name__ == '__main__':
    main()