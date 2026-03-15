import pandas as pd

from src.data_prep.get_fantasygp_prices import (
    _candidate_ajax_actions,
    _contains_password_field,
    _debug_log,
    _discover_ajax_context,
    _discover_login_form,
    _looks_like_loading_page,
    _looks_like_login_page,
    _summarize_payload_text,
    _wait_for_page_readiness,
    _write_debug_html_snapshot,
    combine_prices_for_ranking,
    fetch_prices_via_ajax,
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


def test_extract_driver_constructor_prices_parses_card_layout_without_tables():
    html = """
    <html><body>
      <div id="allDriversAndCars" class="row">
        <div id="car1">
          <div class="carlist">
            <h3>Red Bull</h3>
            <h6 class="badge carprice">$32.0M</h6>
          </div>
          <div class="driverlist">
            <h6 class="d-none d-sm-block">Verstappen</h6>
            <strong>$30.5M</strong>
          </div>
          <div class="driverlist">
            <h6 class="d-none d-sm-block">Perez</h6>
            <strong>$23.0M</strong>
          </div>
        </div>
      </div>
    </body></html>
    """

    driver_df, constructor_df = extract_driver_constructor_prices(html)

    assert set(driver_df["Name"]) == {"Verstappen", "Perez"}
    assert set(driver_df["Price"]) == {30500000.0, 23000000.0}
    assert list(constructor_df["Name"]) == ["Red Bull"]
    assert list(constructor_df["Price"]) == [32000000.0]


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


def test_contains_password_field_detects_password_input():
    assert _contains_password_field('<input type="password" name="pwd" />')
    assert not _contains_password_field('<input type="text" name="user" />')


def test_looks_like_login_page_detects_wordpress_login_markers():
    html = """
    <html><body>
      <form action="/wp-login.php">
        <input type="text" name="log" />
        <input type="password" name="pwd" />
      </form>
      <a href="/wp-login.php?action=lostpassword">Lost your password?</a>
    </body></html>
    """

    assert _looks_like_login_page(html)


def test_looks_like_login_page_does_not_flag_prices_page_with_password_column():
    html = """
    <html><body>
      <table>
        <tr><th>Driver</th><th>Price</th></tr>
        <tr><td>Driver One</td><td>$30.0M</td></tr>
      </table>
      <div>Prices are now live, no login required.</div>
    </body></html>
    """

    assert not _looks_like_login_page(html)


def test_looks_like_loading_page_detects_loading_markers():
    assert _looks_like_loading_page("<html><body>Loading... please wait</body></html>")
    assert not _looks_like_loading_page("<html><body>drivers and cars</body></html>")


def test_fetch_authenticated_html_attempts_wordpress_login_when_password_field_present_but_form_unknown(monkeypatch):
    class WordPressFallbackSession:
        def __init__(self):
            self.trust_env = True
            self.post_calls = []

        def get(self, url, **kwargs):
            if "wp-login.php" in url:
                return _FakeResponse("<html><body>wp login</body></html>", url)
            if self.post_calls:
                return _FakeResponse("<html><body>drivers data page</body></html>", url)
            return _FakeResponse('<html><body><input type="password" name="pwd" /></body></html>', url)

        def post(self, url, data=None, **kwargs):
            self.post_calls.append((url, data))
            return _FakeResponse("<html><body>logged in</body></html>", url)

    session = WordPressFallbackSession()
    monkeypatch.setattr("src.data_prep.get_fantasygp_prices.requests.Session", lambda: session)

    html = fetch_authenticated_html("https://fantasygp.com/drivers-cars/", "user", "pass")

    assert "drivers data page" in html
    assert session.post_calls
    _, payload = session.post_calls[0]
    assert payload["log"] == "user"
    assert payload["pwd"] == "pass"


def test_fetch_authenticated_html_handles_password_only_form(monkeypatch):
    class PasswordOnlyFormSession:
        def __init__(self):
            self.trust_env = True
            self.login_posts = []

        def get(self, url, **kwargs):
            if self.login_posts:
                return _FakeResponse("<html><body>drivers prices content</body></html>", url)
            return _FakeResponse(
                """
                <html><body>
                  <form action=\"/password-protected\">
                    <input type=\"hidden\" name=\"token\" value=\"abc\" />
                    <input type=\"password\" name=\"post_password\" />
                  </form>
                </body></html>
                """,
                url,
            )

        def post(self, url, data=None, **kwargs):
            self.login_posts.append((url, data))
            return _FakeResponse("<html><body>ok</body></html>", url)

    session = PasswordOnlyFormSession()
    monkeypatch.setattr("src.data_prep.get_fantasygp_prices.requests.Session", lambda: session)

    html = fetch_authenticated_html("https://fantasygp.com/drivers-cars/", "ignored", "secret")

    assert "drivers prices content" in html
    _, payload = session.login_posts[0]
    assert payload["post_password"] == "secret"
    assert payload["token"] == "abc"


def test_wait_for_page_readiness_polls_until_loading_clears(monkeypatch):
    class ReadySession:
        def __init__(self):
            self.calls = 0

        def get(self, url, **kwargs):
            self.calls += 1
            if self.calls == 1:
                return _FakeResponse("<html><body>Loading...</body></html>", url)
            return _FakeResponse("<html><body>Driver table ready</body></html>", url)

    monkeypatch.setenv("FANTASYGP_READY_CHECK_ATTEMPTS", "3")
    monkeypatch.setenv("FANTASYGP_READY_CHECK_DELAY_SECONDS", "0")

    html = _wait_for_page_readiness(
        ReadySession(),
        "https://fantasygp.com/drivers-cars/",
        {"User-Agent": "ua"},
        "<html><body>Loading...</body></html>",
    )

    assert "Driver table ready" in html


def test_fetch_prices_via_ajax_extracts_prices_from_json_html_payload():
    class FakeResponse:
        def __init__(self, text):
            self.text = text

        def raise_for_status(self):
            return None

    class FakeSession:
        def __init__(self):
            self.post_calls = 0

        def get(self, url, **kwargs):
            assert "alldriverscars.js" in url
            return FakeResponse("$.post(MyAjax.ajaxurl,{action:'getdriversandcars',security:MyAjax.security},function(){});")

        def post(self, url, data=None, **kwargs):
            self.post_calls += 1
            assert data["action"] in {"getdriversandcars", "driversandcars", "get_drivers_and_cars", "allDriversAndCars"}
            payload = {
                "success": True,
                "data": {
                    "html": (
                        "<div id='car1'><div class='carlist'><h3>Red Bull</h3><h6 class='badge carprice'>$32.0M</h6></div>"
                        "<div class='driverlist'><h6>Verstappen</h6><strong>$30.5M</strong></div>"
                        "<div class='driverlist'><h6>Perez</h6><strong>$23.0M</strong></div></div>"
                    )
                },
            }
            import json

            return FakeResponse(json.dumps(payload))

    html = """
    <script id="alldriverscars-js-js-extra">var MyAjax = {"ajaxurl":"https://fantasygp.com/wp-admin/admin-ajax.php","security":"token123"};</script>
    <script id="alldriverscars-js-js" src="https://fantasygp.com/wp-content/plugins/fantasy-gp/js/alldriverscars.js?ver=202602"></script>
    """

    driver_df, constructor_df = fetch_prices_via_ajax(
        FakeSession(),
        html,
        "https://fantasygp.com/drivers-cars/",
        {"User-Agent": "ua"},
    )

    assert set(driver_df["Name"]) == {"Verstappen", "Perez"}
    assert list(constructor_df["Name"]) == ["Red Bull"]


def test_discover_ajax_context_accepts_single_quotes_and_unquoted_keys():
    html = """
    <script>
      const MyAjax = {ajaxurl:'https://fantasygp.com/wp-admin/admin-ajax.php',security:'token123'};
    </script>
    <script id="alldriverscars-js-js" src="/wp-content/plugins/fantasy-gp/js/alldriverscars.js"></script>
    """

    ajax_url, security, script_url = _discover_ajax_context(html, "https://fantasygp.com/drivers-cars/")

    assert ajax_url == "https://fantasygp.com/wp-admin/admin-ajax.php"
    assert security == "token123"
    assert script_url == "https://fantasygp.com/wp-content/plugins/fantasy-gp/js/alldriverscars.js"


def test_fetch_prices_via_ajax_extracts_prices_from_structured_json_payload_without_html():
    class FakeResponse:
        def __init__(self, text):
            self.text = text

        def raise_for_status(self):
            return None

    class FakeSession:
        def get(self, url, **kwargs):
            return FakeResponse("$.post(MyAjax.ajaxurl,{action:'getdriversandcars',security:MyAjax.security},function(){});")

        def post(self, url, data=None, **kwargs):
            import json

            payload = {
                "success": True,
                "data": {
                    "drivers": [
                        {"driver_name": "Verstappen", "price": "$30.5M"},
                        {"driver_name": "Perez", "price": "$23.0M"},
                    ],
                    "constructors": [{"team": "Red Bull", "price": "$32.0M"}],
                },
            }
            return FakeResponse(json.dumps(payload))

    html = """
    <script id="alldriverscars-js-js-extra">var MyAjax = {'ajaxurl':'https://fantasygp.com/wp-admin/admin-ajax.php','security':'token123'};</script>
    <script id="alldriverscars-js-js" src="https://fantasygp.com/wp-content/plugins/fantasy-gp/js/alldriverscars.js?ver=202602"></script>
    """

    driver_df, constructor_df = fetch_prices_via_ajax(
        FakeSession(),
        html,
        "https://fantasygp.com/drivers-cars/",
        {"User-Agent": "ua"},
    )

    assert set(driver_df["Name"]) == {"Verstappen", "Perez"}
    assert list(constructor_df["Name"]) == ["Red Bull"]


def test_fetch_prices_via_ajax_uses_default_action_candidates_when_script_has_no_action_literal():
    class FakeResponse:
        def __init__(self, text):
            self.text = text

        def raise_for_status(self):
            return None

    class FakeSession:
        def __init__(self):
            self.actions = []

        def get(self, url, **kwargs):
            return FakeResponse("console.log('minified bundle without explicit action object');")

        def post(self, url, data=None, **kwargs):
            self.actions.append(data["action"])
            if data["action"] != "getdriversandcars":
                return FakeResponse('{"success":false}')

            import json

            payload = {
                "success": True,
                "data": {
                    "drivers": [{"driver": "Verstappen", "price": "$30.5M"}],
                    "constructors": [{"constructor": "Red Bull", "price": "$32.0M"}],
                },
            }
            return FakeResponse(json.dumps(payload))

    html = """
    <script id="alldriverscars-js-js-extra">var MyAjax = {'ajaxurl':'https://fantasygp.com/wp-admin/admin-ajax.php','security':'token123'};</script>
    <script id="alldriverscars-js-js" src="https://fantasygp.com/wp-content/plugins/fantasy-gp/js/alldriverscars.js?ver=202602"></script>
    """

    fake_session = FakeSession()
    driver_df, constructor_df = fetch_prices_via_ajax(
        fake_session,
        html,
        "https://fantasygp.com/drivers-cars/",
        {"User-Agent": "ua"},
    )

    assert "getdriversandcars" in fake_session.actions
    assert list(driver_df["Name"]) == ["Verstappen"]
    assert list(constructor_df["Name"]) == ["Red Bull"]


def test_candidate_ajax_actions_merges_discovered_and_default_actions_without_duplicates(monkeypatch):
    monkeypatch.setenv("FANTASYGP_AJAX_ACTIONS", "customAction,getdriversandcars")

    actions = _candidate_ajax_actions("$.post(MyAjax.ajaxurl,{action:'getdriversandcars'},function(){});")

    assert actions[0] == "getdriversandcars"
    assert "customAction" in actions
    assert len(actions) == len(set(actions))


def test_summarize_payload_text_condenses_whitespace_and_truncates():
    long_payload = "{\n  \"x\": 1,\n  \"html\": \"" + ("a" * 400) + "\"\n}"
    summary = _summarize_payload_text(long_payload, max_len=80)

    assert "\n" not in summary
    assert summary.endswith("...")
    assert len(summary) == 83


def test_write_debug_html_snapshot_writes_file(monkeypatch, tmp_path):
    debug_file = tmp_path / "debug" / "page.html"
    monkeypatch.setenv("FANTASYGP_DEBUG_HTML_PATH", str(debug_file))

    _write_debug_html_snapshot("<html><body>blocked</body></html>", "Could not parse")

    assert debug_file.exists()
    text = debug_file.read_text(encoding="utf-8")
    assert "extraction_error: Could not parse" in text
    assert "<html><body>blocked</body></html>" in text


def test_write_debug_html_snapshot_uses_current_dir_when_no_parent(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("FANTASYGP_DEBUG_HTML_PATH", "snapshot.html")

    _write_debug_html_snapshot("<html></html>", "error")

    assert (tmp_path / "snapshot.html").exists()


def test_debug_log_emits_only_when_enabled(monkeypatch, caplog):
    monkeypatch.delenv("FANTASYGP_DEBUG", raising=False)
    caplog.clear()
    _debug_log("hidden")
    assert not caplog.records

    monkeypatch.setenv("FANTASYGP_DEBUG", "true")
    _debug_log("visible")
    assert any("visible" in record.message for record in caplog.records)
