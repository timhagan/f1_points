import fastf1
import datetime
import pandas as pd
import os
cache_path = os.path.join(os.path.dirname(__file__), '..', '..', '.cache', 'event_schedule')
fastf1.Cache.enable_cache(cache_path)

year                    = datetime.date.today().year
event_schedule          = fastf1.get_event_schedule(year)
event_schedule.columns  = ['RoundNumber', 
                          'Country', 
                          'Location', 
                          'OfficialEventName', 
                          'EventDate',
                          'EventName', 
                          'EventFormat', 
                          'Session1', 
                          'SessionDate1',
                          'SessionDateUtc1', 
                          'Session2', 
                          'SessionDate2', 
                          'SessionDateUtc2',
                          'Session3', 
                          'SessionDate3', 
                          'SessionDateUtc3', 
                          'Session4',
                          'SessionDate4', 
                          'SessionDateUtc4', 
                          'Session5', 
                          'SessionDate5', 
                          'SessionDateUtc5', 
                          'F1ApiSupport']

event_schedule_melted = pd.wide_to_long(
    event_schedule,
    stubnames=['Session', 'SessionDate', 'SessionDateUtc'], 
    i=['RoundNumber', 'Country', 'Location', 'OfficialEventName', 'EventDate', 'EventName', 'EventFormat', 'F1ApiSupport'],
    j='SessionNumber',
    sep='',  
    suffix=r'\d+'
).reset_index()

event_schedule_melted = event_schedule_melted.rename(
    columns={
        'Session': 'SessionName',
        'SessionDate': 'SessionDate',
        'SessionDateUtc': 'SessionDateUtc'
    }
)

event_schedule_melted = event_schedule_melted.sort_values(['RoundNumber', 'SessionDate'])
event_schedule_melted = event_schedule_melted.dropna(subset=['SessionName'])

# Save to data directory relative to project root
output_path = os.path.join(os.path.dirname(__file__), '..', '..', 'data', f'sessions_{year}.csv')
event_schedule_melted.to_csv(output_path, index=False)