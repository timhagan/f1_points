import fastf1
import pandas as pd
import datetime
import functions
from importlib import reload

reload(functions)

past_event_names = functions.get_past_race_event_names()

# Initialize an empty DataFrame to hold all driver points data
driver_points_df = pd.DataFrame()
# Initialize an empty DataFrame to hold all constructor points data
constructor_points_df = pd.DataFrame()

### Read in points data for each past event and union them into a single DataFrame
year = datetime.datetime.now().year
for past_event_name in past_event_names:
    # Load the driver and constructor points data for the past event
    driver_points_df_slim = pd.read_csv(f"C:\\Users\\timot\\OneDrive\\Documents\\GitHub\\f1_points\\.data\\driver_points_{year}_{past_event_name}.csv")
    # If the file does not exist, skip to the next iteration
    if driver_points_df_slim.empty:
        continue
    # Append the data to the main DataFrame
    driver_points_df = pd.concat([driver_points_df, driver_points_df_slim], ignore_index=True)

    constructor_points_df_slim = pd.read_csv(f"C:\\Users\\timot\\OneDrive\\Documents\\GitHub\\f1_points\\.data\\constructor_points_{year}_{past_event_name}.csv")
    # If the file does not exist, skip to the next iteration
    if constructor_points_df_slim.empty:
        continue
    # Append the data to the main DataFrame
    constructor_points_df = pd.concat([constructor_points_df, constructor_points_df_slim], ignore_index=True)
# Reset the index of the DataFrame
driver_points_df.reset_index(drop=True, inplace=True)
constructor_points_df.reset_index(drop=True, inplace=True)

driver_points_df.to_csv(f"C:\\Users\\timot\\OneDrive\\Documents\\GitHub\\f1_points\\.data\\driver_points_{year}_current.csv", index=False)
constructor_points_df.to_csv(f"C:\\Users\\timot\\OneDrive\\Documents\\GitHub\\f1_points\\.data\\constructor_points_{year}_current.csv", index=False)