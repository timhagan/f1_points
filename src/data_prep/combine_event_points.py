import pandas as pd
import datetime
import os
import sys
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)
from src.data_prep import functions

def main():
    # Get past event names
    past_event_names = functions.get_past_race_event_names()
    print(f"🏁 Found {len(past_event_names)} past events: {list(past_event_names)}")
    
    # Prepare accumulators
    driver_points_df = pd.DataFrame()
    constructor_points_df = pd.DataFrame()
    YEAR = functions.resolve_season_year(require_past_races=True)

    if len(past_event_names) == 0:
        print(f"No past race events found for {YEAR}. Skipping current standings file generation.")
        return
    
    for past_event_name in past_event_names:
        SELECTED_EVENT_ROUND = functions.get_round_number_from_event_name(past_event_name, year=YEAR)
        driver_csv_path = os.path.join(os.path.dirname(__file__), '..', '..', 'data', f'driver_points_{YEAR}_{SELECTED_EVENT_ROUND}_{past_event_name}.csv')
        print(f"🔍 Looking for driver file: {driver_csv_path}")
        try:
            driver_points_df_slim = pd.read_csv(driver_csv_path)
            if not driver_points_df_slim.empty:
                driver_points_df = pd.concat([driver_points_df, driver_points_df_slim], ignore_index=True)
                print(f"✅ Loaded {len(driver_points_df_slim)} driver records from {past_event_name}")
            else:
                print(f"⚠️  Driver file for {past_event_name} is empty")
        except FileNotFoundError:
            print(f"❌ Driver file not found: {driver_csv_path}")
            continue

        constructor_csv_path = os.path.join(os.path.dirname(__file__), '..', '..', 'data', f'constructor_points_{YEAR}_{SELECTED_EVENT_ROUND}_{past_event_name}.csv')
        print(f"🔍 Looking for constructor file: {constructor_csv_path}")
        try:
            constructor_points_df_slim = pd.read_csv(constructor_csv_path)
            if not constructor_points_df_slim.empty:
                constructor_points_df = pd.concat([constructor_points_df, constructor_points_df_slim], ignore_index=True)
                print(f"✅ Loaded {len(constructor_points_df_slim)} constructor records from {past_event_name}")
            else:
                print(f"⚠️  Constructor file for {past_event_name} is empty")
        except FileNotFoundError:
            print(f"❌ Constructor file not found: {constructor_csv_path}")
            continue
            
    driver_points_df.reset_index(drop=True, inplace=True)
    constructor_points_df.reset_index(drop=True, inplace=True)
    
    print(f"📊 Final driver DataFrame has {len(driver_points_df)} records")
    print(f"📊 Final constructor DataFrame has {len(constructor_points_df)} records")
    
    # Save to data directory relative to project root
    driver_output_path = os.path.join(os.path.dirname(__file__), '..', '..', 'data', f'driver_points_{YEAR}_current.csv')
    constructor_output_path = os.path.join(os.path.dirname(__file__), '..', '..', 'data', f'constructor_points_{YEAR}_current.csv')

    if driver_points_df.empty and constructor_points_df.empty:
        print(f"No event point files were available for {YEAR}. Skipping current standings file generation.")
        return

    driver_points_df.to_csv(driver_output_path, index=False)
    constructor_points_df.to_csv(constructor_output_path, index=False)
    
    print(f"💾 Saved driver data to: {driver_output_path}")
    print(f"💾 Saved constructor data to: {constructor_output_path}")

if __name__ == '__main__':
    main()
