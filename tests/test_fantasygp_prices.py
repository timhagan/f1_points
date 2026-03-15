import pandas as pd

from src.data_prep.get_fantasygp_prices import (
    _discover_login_form,
    combine_prices_for_ranking,
    fetch_authenticated_html,
    extract_driver_constructor_prices,
    parse_price_value,
)


def test_parse_price_value_handles_currency_and_commas():
    assert parse_price_value("$30,500,000") == 30500000.0
    assert parse_price_value("31.5") == 31.5


def test_parse_price_value_handles_suffixes_and_decimal_comma():
    assert parse_price_value("$30.5M") == 30500000.0
    assert parse_price_value("€30,5M") == 30500000.0
    assert parse_price_value("12k") == 12000.0


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


def test_discover_login_form_prefers_form_with_password_field():
    html = """
    <html><body>
      <form action="/search"><input name="q" type="text" /></form>
      <form action="/login">
        <input type="hidden" name="csrf" value="abc" />
        <input type="text" name="user_email" />
        <input type="password" name="user_pass" />
      </form>
    </body></html>
    """

    action_url, payload, username_key, password_key = _discover_login_form(html, "https://fantasygp.com/")

    assert action_url == "https://fantasygp.com/login"
    assert payload == {"csrf": "abc"}
    assert username_key == "user_email"
    assert password_key == "user_pass"


class _FakeResponse:
    def __init__(self, text, url):
        self.text = text
        self.url = url

    def raise_for_status(self):
        return None


def test_fetch_authenticated_html_retries_without_proxy(monkeypatch):
    import requests

    class FailingProxySession:
        def __init__(self):
            self.trust_env = True

        def get(self, *args, **kwargs):
            raise requests.exceptions.ProxyError("proxy blocked")

    class DirectSession:
        def __init__(self):
            self.trust_env = True

        def get(self, url, **kwargs):
            return _FakeResponse("<html><body>No login required</body></html>", url)

    sessions = [FailingProxySession(), DirectSession()]

    def fake_session_factory():
        return sessions.pop(0)

    monkeypatch.setattr("src.data_prep.get_fantasygp_prices.requests.Session", fake_session_factory)

    html = fetch_authenticated_html("https://fantasygp.com/drivers-cars/", "u", "p")

    assert "No login required" in html


def test_fetch_authenticated_html_raises_clear_error_when_proxy_and_direct_paths_fail(monkeypatch):
    import pytest
    import requests

    class FailingProxySession:
        def __init__(self):
            self.trust_env = True

        def get(self, *args, **kwargs):
            raise requests.exceptions.ProxyError("proxy blocked")

    class FailingDirectSession:
        def __init__(self):
            self.trust_env = True

        def get(self, *args, **kwargs):
            raise requests.exceptions.ConnectionError("network down")

    sessions = [FailingProxySession(), FailingDirectSession()]

    def fake_session_factory():
        return sessions.pop(0)

    monkeypatch.setattr("src.data_prep.get_fantasygp_prices.requests.Session", fake_session_factory)

    with pytest.raises(RuntimeError, match="Unable to reach FantasyGP"):
        fetch_authenticated_html("https://fantasygp.com/drivers-cars/", "u", "p")
