import fastf1
import pandas as pd
import datetime
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
        driver_points_df_slim = pd.read_csv(f"C:\\Users\\timot\\OneDrive\\Documents\\GitHub\\f1_points\\.data\\driver_points_{year}_{past_event_name}.csv")
        if driver_points_df_slim.empty:
            continue
        driver_points_df = pd.concat([driver_points_df, driver_points_df_slim], ignore_index=True)
        constructor_points_df_slim = pd.read_csv(f"C:\\Users\\timot\\OneDrive\\Documents\\GitHub\\f1_points\\.data\\constructor_points_{year}_{past_event_name}.csv")
        if constructor_points_df_slim.empty:
            continue
        constructor_points_df = pd.concat([constructor_points_df, constructor_points_df_slim], ignore_index=True)
    driver_points_df.reset_index(drop=True, inplace=True)
    constructor_points_df.reset_index(drop=True, inplace=True)
    driver_points_df.to_csv(f"C:\\Users\\timot\\OneDrive\\Documents\\GitHub\\f1_points\\.data\\driver_points_{year}_current.csv", index=False)
    constructor_points_df.to_csv(f"C:\\Users\\timot\\OneDrive\\Documents\\GitHub\\f1_points\\.data\\constructor_points_{year}_current.csv", index=False)

if __name__ == '__main__':
    main()