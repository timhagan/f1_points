import fastf1
import datetime
import pandas as pd

fastf1.Cache.enable_cache('..\\f1_points\\.cache\\event_schedule')

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

event_schedule_melted.to_csv(f'data/sessions_{year}.csv', index=False)