import pytest
import datetime
import pandas as pd
import os
import sys

# Add the project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from src.data_prep import functions


def test_calculate_places_gained_points():
    """Test places gained calculation with floor function (no negative points)"""
    df = pd.DataFrame({
        'GridPosition': [5, 3],
        'Position': [3, 4]
    })
    res = functions.calculate_places_gained_points(df.copy(), 'Race', multiplier=2)
    assert res['PlacesGainedRace'].tolist() == [2, -1]
    assert res['PlacesGainedRaceFloor'].tolist() == [2, 0]
    assert res['PlacesGainedRacePoints'].tolist() == [4, 0]


def test_calculate_pole_points():
    """Test pole position points calculation (10 points for P1)"""
    df = pd.DataFrame({'Position': [1.0, 2.0, 3.0]})
    res = functions.calculate_pole_points(df.copy())
    assert res['PolePoints'].tolist() == [10, 0, 0]


def test_merge_points_dataframes():
    """Test DataFrame merging with proper NaN handling"""
    df1 = pd.DataFrame({'DriverId': [1, 2], 'RacePoints': [10, 20]})
    df2 = pd.DataFrame({'DriverId': [1, 3], 'SprintPoints': [5, 15]})
    merged = functions.merge_points_dataframes({'Race': df1, 'Sprint': df2}, merge_key='DriverId')

    if merged is None:
        pytest.fail("Merged DataFrame is None")
    else:
        assert set(merged['DriverId']) == {1, 2, 3}
        assert merged['RacePoints'].tolist() == [10, 20, 0]
        assert merged['SprintPoints'].tolist() == [5, 0, 15]


def test_get_most_recent_session_df_handles_no_past_sessions():
    now = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
    sessions = pd.DataFrame(
        {
            'SessionName': ['Race'],
            'SessionDateUtc': ['2026-12-01T10:00:00Z'],
            'EventName': ['Future GP'],
        }
    )

    result = functions.get_most_recent_session_df(sessions, session_type='Race', today=now, year=2026)

    assert isinstance(result, pd.DataFrame)
    assert result.empty
