import datetime
import os

import fastf1
import pandas as pd


CACHE_PATH = os.path.join(os.path.dirname(__file__), '..', '..', '.cache', 'event_schedule')
DATA_DIR = os.path.join(os.path.dirname(__file__), '..', '..', 'data')


def fetch_event_schedule(year):
    fastf1.Cache.enable_cache(CACHE_PATH)
    return fastf1.get_event_schedule(year)


def main():
    target_year = datetime.date.today().year
    schedule_year = None
    event_schedule = None

    for candidate_year in [target_year, target_year - 1]:
        try:
            event_schedule = fetch_event_schedule(candidate_year)
            schedule_year = candidate_year
            break
        except ValueError:
            continue

    if event_schedule is None:
        existing_files = sorted([name for name in os.listdir(DATA_DIR) if name.startswith("sessions_") and name.endswith(".csv")])
        if existing_files:
            print(f"Unable to fetch schedule from API; keeping existing schedule file: {existing_files[-1]}")
            return
        raise ValueError(f"Failed to load event schedule for {target_year} and fallback year {target_year - 1}.")

    event_schedule.columns = [
        'RoundNumber',
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
        'F1ApiSupport',
    ]

    event_schedule_melted = pd.wide_to_long(
        event_schedule,
        stubnames=['Session', 'SessionDate', 'SessionDateUtc'],
        i=['RoundNumber', 'Country', 'Location', 'OfficialEventName', 'EventDate', 'EventName', 'EventFormat', 'F1ApiSupport'],
        j='SessionNumber',
        sep='',
        suffix=r'\d+',
    ).reset_index()

    event_schedule_melted = event_schedule_melted.rename(
        columns={
            'Session': 'SessionName',
            'SessionDate': 'SessionDate',
            'SessionDateUtc': 'SessionDateUtc',
        }
    )

    event_schedule_melted = event_schedule_melted.sort_values(['RoundNumber', 'SessionDate'])
    event_schedule_melted = event_schedule_melted.dropna(subset=['SessionName'])

    output_path = os.path.join(DATA_DIR, f'sessions_{schedule_year}.csv')
    event_schedule_melted.to_csv(output_path, index=False)


if __name__ == '__main__':
    main()
