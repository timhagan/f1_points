import pandas as pd

from src.data_prep.get_fantasygp_prices import (
    combine_prices_for_ranking,
    extract_driver_constructor_prices,
    parse_price_value,
)


def test_parse_price_value_handles_currency_and_commas():
    assert parse_price_value("$30,500,000") == 30500000.0
    assert parse_price_value("31.5") == 31.5


def test_extract_driver_constructor_prices_parses_typed_tables_without_fixed_real_values():
    html = """
    <html><body>
      <table>
        <thead><tr><th>Driver</th><th>Price</th></tr></thead>
        <tbody>
          <tr><td>Driver One</td><td>$30,000,000</td></tr>
          <tr><td>Driver Two</td><td>$25,500,000</td></tr>
        </tbody>
      </table>
      <table>
        <thead><tr><th>Constructor</th><th>Price</th></tr></thead>
        <tbody>
          <tr><td>Team Alpha</td><td>$28,000,000</td></tr>
          <tr><td>Team Beta</td><td>$24,000,000</td></tr>
        </tbody>
      </table>
    </body></html>
    """

    driver_df, constructor_df = extract_driver_constructor_prices(html)

    expected_columns = ["EntityType", "Name", "NameKey", "Price", "ScrapedAtUtc"]
    assert list(driver_df.columns) == expected_columns
    assert list(constructor_df.columns) == expected_columns

    assert set(driver_df["EntityType"]) == {"driver"}
    assert set(constructor_df["EntityType"]) == {"constructor"}

    assert driver_df["Price"].dtype.kind in {"f", "i"}
    assert constructor_df["Price"].dtype.kind in {"f", "i"}
    assert (driver_df["Price"] > 0).all()
    assert (constructor_df["Price"] > 0).all()

    assert pd.to_datetime(driver_df["ScrapedAtUtc"], utc=True, errors="coerce").notna().all()
    assert pd.to_datetime(constructor_df["ScrapedAtUtc"], utc=True, errors="coerce").notna().all()


def test_combine_prices_for_ranking_returns_both_entity_types():
    driver_df = pd.DataFrame(
        {
            "EntityType": ["driver"],
            "Name": ["Driver X"],
            "NameKey": ["driver_x"],
            "Price": [1000000.0],
            "ScrapedAtUtc": ["2026-01-01T00:00:00+00:00"],
        }
    )
    constructor_df = pd.DataFrame(
        {
            "EntityType": ["constructor"],
            "Name": ["Team Y"],
            "NameKey": ["team_y"],
            "Price": [1200000.0],
            "ScrapedAtUtc": ["2026-01-01T00:00:00+00:00"],
        }
    )

    combined = combine_prices_for_ranking(driver_df, constructor_df)

    assert set(combined["EntityType"]) == {"driver", "constructor"}
    assert len(combined) == 2
