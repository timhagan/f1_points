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
    df1 = pd.DataFrame({'DriverId': [1,2], 'RacePoints': [10,20]})
    df2 = pd.DataFrame({'DriverId': [1,3], 'SprintPoints': [5,15]})
    merged = functions.merge_points_dataframes({'Race': df1, 'Sprint': df2}, merge_key='DriverId')
    # Should have DriverId 1,2,3
    if merged is None:
        pytest.fail("Merged DataFrame is None")
    else:
        assert set(merged['DriverId']) == {1,2,3}
        # Check points columns exist and NaN filled with 0
        assert merged['RacePoints'].tolist() == [10,20,0]
        assert merged['SprintPoints'].tolist() == [5,0,15]


def test_get_most_recent_session_df_returns_empty_when_no_past_sessions():
    """No past sessions should return an empty DataFrame instead of raising errors."""
    sessions = pd.DataFrame({
        'SessionName': ['Race'],
        'SessionDateUtc': ['2099-01-01T10:00:00Z'],
        'EventName': ['Future GP'],
        'EventFormat': ['conventional'],
        'RoundNumber': [1]
    })

    result = functions.get_most_recent_session_df(
        sessions=sessions,
        session_type='Race',
        today=datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc),
        year=2026
    )

    assert result.empty


def test_resolve_season_year_falls_back_to_latest_available_with_past_races():
    """When current season has no past races, fallback should use latest eligible prior season."""
    year = functions.resolve_season_year(
        today=datetime.datetime(2026, 2, 1, tzinfo=datetime.timezone.utc),
        require_past_races=True
    )
    assert year == 2025


def test_get_round_number_from_event_name_with_no_year_uses_available_schedule():
    """Round lookup should work without explicitly passing a year."""
    round_number = functions.get_round_number_from_event_name('Australian Grand Prix')
    assert round_number == 1
