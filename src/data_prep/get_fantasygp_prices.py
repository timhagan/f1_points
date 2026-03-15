import datetime
import html
import json
import os
import re
from html.parser import HTMLParser

import pandas as pd
import requests
from requests import exceptions as requests_exceptions

TARGET_URL = os.environ.get("FANTASYGP_TARGET_URL", "https://fantasygp.com/drivers-cars/")
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data")
DEFAULT_AJAX_ACTIONS = ["getdriversandcars", "get_drivers_and_cars", "driversandcars"]


def _is_debug_enabled():
    return os.environ.get("FANTASYGP_DEBUG", "").strip().lower() in {"1", "true", "yes", "on"}


def _debug_log(message):
    if _is_debug_enabled():
        print(f"[fantasygp_prices] {message}")


def _get_default_ajax_actions():
    configured = os.environ.get("FANTASYGP_AJAX_ACTIONS", "").strip()
    if not configured:
        return DEFAULT_AJAX_ACTIONS

    actions = [action.strip() for action in configured.split(",") if action.strip()]
    return actions or DEFAULT_AJAX_ACTIONS


class _SimpleTableParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.tables = []
        self._in_table = False
        self._in_row = False
        self._in_cell = False
        self._current_table = []
        self._current_row = []
        self._current_cell = []

    def handle_starttag(self, tag, attrs):
        if tag == "table":
            self._in_table = True
            self._current_table = []
        elif self._in_table and tag == "tr":
            self._in_row = True
            self._current_row = []
        elif self._in_row and tag in {"td", "th"}:
            self._in_cell = True
            self._current_cell = []

    def handle_data(self, data):
        if self._in_cell:
            self._current_cell.append(data)

    def handle_endtag(self, tag):
        if tag in {"td", "th"} and self._in_cell:
            text = "".join(self._current_cell).strip()
            self._current_row.append(text)
            self._in_cell = False
        elif tag == "tr" and self._in_row:
            if self._current_row:
                self._current_table.append(self._current_row)
            self._in_row = False
        elif tag == "table" and self._in_table:
            if self._current_table:
                self.tables.append(self._current_table)
            self._in_table = False


class _DriversCarsParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.cards = []
        self._depth = 0
        self._current_card = None
        self._card_depth = None
        self._current_driver = None
        self._driver_depth = None
        self._collect_team = False
        self._collect_car_price = False
        self._collect_driver_name = False

    def handle_starttag(self, tag, attrs):
        self._depth += 1
        attrs_dict = {k.lower(): v for k, v in attrs}
        class_list = (attrs_dict.get("class") or "").split()
        element_id = (attrs_dict.get("id") or "").strip()

        if tag == "div" and re.fullmatch(r"car\d+", element_id):
            self._current_card = {"team": [], "car_price": [], "drivers": []}
            self._card_depth = self._depth
            return

        if self._current_card is None:
            return

        if tag == "div" and "driverlist" in class_list:
            self._current_driver = {"name_parts": [], "text_parts": []}
            self._driver_depth = self._depth
            return

        if tag == "h3" and self._current_driver is None:
            self._collect_team = True
        elif tag == "h6" and "carprice" in class_list and self._current_driver is None:
            self._collect_car_price = True
        elif tag in {"h6", "h5"} and self._current_driver is not None:
            self._collect_driver_name = True

    def handle_data(self, data):
        text = data.strip()
        if not text:
            return

        if self._current_card is not None and self._collect_team:
            self._current_card["team"].append(text)

        if self._current_card is not None and self._collect_car_price:
            self._current_card["car_price"].append(text)

        if self._current_driver is not None:
            self._current_driver["text_parts"].append(text)
            if self._collect_driver_name:
                self._current_driver["name_parts"].append(text)

    def handle_endtag(self, tag):
        if tag == "h3":
            self._collect_team = False
        elif tag == "h6":
            self._collect_car_price = False
            self._collect_driver_name = False
        elif tag == "h5":
            self._collect_driver_name = False

        if tag == "div" and self._current_driver is not None and self._driver_depth == self._depth:
            self._current_card["drivers"].append(self._current_driver)
            self._current_driver = None
            self._driver_depth = None

        if tag == "div" and self._current_card is not None and self._card_depth == self._depth:
            self.cards.append(self._current_card)
            self._current_card = None
            self._card_depth = None

        self._depth -= 1


def _normalize_column_name(name):
    return re.sub(r"\s+", " ", str(name)).strip().lower()


def _normalize_name_key(name):
    normalized = re.sub(r"[^a-z0-9]+", "_", str(name).strip().lower())
    return normalized.strip("_")


def parse_price_value(value):
    if pd.isna(value):
        return pd.NA

    text = str(value).strip()
    if not text:
        return pd.NA

    multiplier = 1.0
    lowered = text.lower()
    suffix_match = re.search(r"([kmb])\s*$", lowered)
    if suffix_match:
        suffix = suffix_match.group(1)
        if suffix == "b":
            multiplier = 1_000_000_000.0
        elif suffix == "m":
            multiplier = 1_000_000.0
        elif suffix == "k":
            multiplier = 1_000.0

    cleaned = re.sub(r"[^0-9.,-]", "", text)
    if not cleaned:
        return pd.NA

    # Support common locale/currency formats like 30,5 (decimal comma) and
    # 30,500,000 (thousands separators).
    if "," in cleaned and "." not in cleaned:
        if cleaned.count(",") == 1:
            cleaned = cleaned.replace(",", ".")
        else:
            cleaned = cleaned.replace(",", "")
    else:
        cleaned = cleaned.replace(",", "")

    try:
        return float(cleaned) * multiplier
    except ValueError:
        return pd.NA


def _html_to_tables(html):
    parser = _SimpleTableParser()
    parser.feed(html)

    dataframes = []
    for raw_table in parser.tables:
        if len(raw_table) < 2:
            continue

        header = raw_table[0]
        rows = raw_table[1:]
        width = len(header)
        normalized_rows = [row[:width] + [""] * max(0, width - len(row)) for row in rows]
        dataframes.append(pd.DataFrame(normalized_rows, columns=header))

    return dataframes


def _classify_table(table):
    normalized_columns = [_normalize_column_name(c) for c in table.columns]

    if any("driver" in col for col in normalized_columns):
        return "driver"
    if any(token in col for col in normalized_columns for token in ["constructor", "team", "car"]):
        return "constructor"

    return None


def _pick_price_tables(tables):
    candidates = []
    for table in tables:
        lowered = {_normalize_column_name(c): c for c in table.columns}
        entity_type = _classify_table(table)
        name_col = None
        price_col = None

        for key, original in lowered.items():
            if any(token in key for token in ["driver", "constructor", "car", "team", "name"]):
                name_col = original
                break

        for key, original in lowered.items():
            if "price" in key or "cost" in key or "$" in key:
                price_col = original
                break

        if name_col and price_col:
            slim = table[[name_col, price_col]].copy()
            slim.columns = ["Name", "Price"]
            slim = slim.dropna(subset=["Name", "Price"])
            slim["Name"] = slim["Name"].astype(str).str.strip()
            slim["Price"] = slim["Price"].apply(parse_price_value)
            slim = slim.dropna(subset=["Price"])
            slim = slim[slim["Name"] != ""]
            if not slim.empty:
                candidates.append((entity_type, slim))

    return candidates


def _prepare_price_dataframe(df, entity_type):
    prepared = df.copy()
    prepared["EntityType"] = entity_type
    prepared["NameKey"] = prepared["Name"].apply(_normalize_name_key)
    prepared["ScrapedAtUtc"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    return prepared[["EntityType", "Name", "NameKey", "Price", "ScrapedAtUtc"]]


def _extract_price_from_text(text):
    for token in re.findall(r"(?:[$€£]\s*)?\d[\d.,]*(?:\s*[kKmMbB])?", text):
        value = parse_price_value(token)
        if pd.notna(value):
            return value
    return pd.NA


def _extract_prices_from_cards(html):
    parser = _DriversCarsParser()
    parser.feed(html)

    driver_rows = []
    constructor_rows = []

    for card in parser.cards:
        team_name = " ".join(card["team"]).strip()
        car_price = _extract_price_from_text(" ".join(card["car_price"]))
        if team_name and pd.notna(car_price):
            constructor_rows.append({"Name": team_name, "Price": car_price})

        for driver in card["drivers"]:
            name_candidates = [part.strip() for part in driver["name_parts"] if part.strip()]
            if not name_candidates:
                continue
            driver_name = max(name_candidates, key=len)
            price = _extract_price_from_text(" ".join(driver["text_parts"]))
            if pd.notna(price):
                driver_rows.append({"Name": driver_name, "Price": price})

    if not driver_rows or not constructor_rows:
        return None, None

    return (
        _prepare_price_dataframe(pd.DataFrame(driver_rows), "driver"),
        _prepare_price_dataframe(pd.DataFrame(constructor_rows), "constructor"),
    )


def _extract_js_object_value(html, object_name, key):
    object_pattern = rf"(?:var|let|const)\s+{re.escape(object_name)}\s*=\s*\{{(.*?)\}}"
    object_match = re.search(object_pattern, html, flags=re.IGNORECASE | re.DOTALL)
    if not object_match:
        return None

    object_body = object_match.group(1)
    value_pattern = rf"(?:['\"]?{re.escape(key)}['\"]?)\s*:\s*['\"]([^'\"]+)['\"]"
    value_match = re.search(value_pattern, object_body, flags=re.IGNORECASE | re.DOTALL)
    return value_match.group(1) if value_match else None


def _discover_ajax_context(html, base_url):
    ajax_url = _extract_js_object_value(html, "MyAjax", "ajaxurl")
    security = _extract_js_object_value(html, "MyAjax", "security")

    script_match = re.search(
        r"<script[^>]+id=[\"']alldriverscars-js-js[\"'][^>]+src=[\"']([^\"']+)[\"']",
        html,
        flags=re.IGNORECASE,
    )
    script_url = script_match.group(1) if script_match else None

    if ajax_url:
        ajax_url = requests.compat.urljoin(base_url, ajax_url)
    if script_url:
        script_url = requests.compat.urljoin(base_url, script_url)

    return ajax_url, security, script_url


def _discover_ajax_actions(js_text):
    actions = []
    patterns = [
        r"action\s*:\s*['\"]([a-zA-Z0-9_-]+)['\"]",
        r"[?&]action=([a-zA-Z0-9_-]+)",
        r"\baction\s*=\s*['\"]([a-zA-Z0-9_-]+)['\"]",
    ]
    for pattern in patterns:
        matches = re.findall(pattern, js_text)
        for action in matches:
            if action not in actions:
                actions.append(action)

    for action in _get_default_ajax_actions():
        if action not in actions:
            actions.append(action)

    # Prioritize likely matches for this page.
    actions.sort(key=lambda x: ("driver" not in x.lower() and "car" not in x.lower(), x))
    return actions


def _extract_html_like_chunks(value):
    chunks = []
    if isinstance(value, str):
        unescaped = html.unescape(value)
        if "<" in unescaped and ">" in unescaped:
            chunks.append(unescaped)
    elif isinstance(value, dict):
        for v in value.values():
            chunks.extend(_extract_html_like_chunks(v))
    elif isinstance(value, list):
        for item in value:
            chunks.extend(_extract_html_like_chunks(item))
    return chunks


def _build_price_rows(items, name_keys):
    rows = []
    for item in items:
        if not isinstance(item, dict):
            continue

        name = None
        for key in name_keys:
            value = item.get(key)
            if value is not None and str(value).strip():
                name = str(value).strip()
                break

        if not name:
            continue

        raw_price = None
        for key in ["price", "cost", "value"]:
            if key in item:
                raw_price = item[key]
                break

        if raw_price is None:
            continue

        parsed_price = parse_price_value(raw_price)
        if pd.notna(parsed_price):
            rows.append({"Name": name, "Price": parsed_price})

    return rows


def _extract_prices_from_schema(payload):
    if not isinstance(payload, dict):
        return None, None

    candidate_containers = [payload]
    data = payload.get("data")
    if isinstance(data, dict):
        candidate_containers.append(data)

    for container in candidate_containers:
        drivers = container.get("drivers")
        constructors = container.get("constructors") or container.get("teams") or container.get("cars")
        if isinstance(drivers, list) and isinstance(constructors, list):
            driver_rows = _build_price_rows(drivers, ["driver_name", "driver", "name", "title"])
            constructor_rows = _build_price_rows(constructors, ["team", "constructor", "car", "name", "title"])
            if driver_rows and constructor_rows:
                return (
                    _prepare_price_dataframe(pd.DataFrame(driver_rows).drop_duplicates(), "driver"),
                    _prepare_price_dataframe(pd.DataFrame(constructor_rows).drop_duplicates(), "constructor"),
                )

    return None, None


def _extract_prices_from_json_payload(payload):
    driver_prices, constructor_prices = _extract_prices_from_schema(payload)
    if driver_prices is not None and constructor_prices is not None:
        return driver_prices, constructor_prices

    records = []

    def walk(value, context=""):
        if isinstance(value, dict):
            local_context = context.lower()
            name = None
            raw_price = None

            for key in ["name", "driver", "driver_name", "constructor", "team", "car", "title"]:
                if key in value and str(value[key]).strip():
                    name = str(value[key]).strip()
                    break

            for key in ["price", "cost", "value"]:
                if key in value:
                    raw_price = value[key]
                    break

            if name and raw_price is not None:
                parsed_price = parse_price_value(raw_price)
                if pd.notna(parsed_price):
                    if "driver" in local_context:
                        entity_type = "driver"
                    elif any(token in local_context for token in ["constructor", "team", "car"]):
                        entity_type = "constructor"
                    else:
                        entity_type = None
                    records.append((entity_type, name, parsed_price))

            for key, item in value.items():
                walk(item, f"{local_context} {key}".strip())

        elif isinstance(value, list):
            for item in value:
                walk(item, context)

    walk(payload)

    typed = [record for record in records if record[0] in {"driver", "constructor"}]
    if not typed:
        return None, None

    driver_rows = [{"Name": name, "Price": price} for entity, name, price in typed if entity == "driver"]
    constructor_rows = [
        {"Name": name, "Price": price} for entity, name, price in typed if entity == "constructor"
    ]

    if not driver_rows or not constructor_rows:
        return None, None

    return (
        _prepare_price_dataframe(pd.DataFrame(driver_rows).drop_duplicates(), "driver"),
        _prepare_price_dataframe(pd.DataFrame(constructor_rows).drop_duplicates(), "constructor"),
    )


def _extract_prices_from_ajax_payload(payload_text):
    html_candidates = [payload_text]
    try:
        json_payload = json.loads(payload_text)
        driver_prices, constructor_prices = _extract_prices_from_json_payload(json_payload)
        if driver_prices is not None and constructor_prices is not None:
            return driver_prices, constructor_prices
        html_candidates.extend(_extract_html_like_chunks(json_payload))
    except ValueError:
        pass

    for chunk in html_candidates:
        try:
            driver_prices, constructor_prices = extract_driver_constructor_prices(chunk)
            return driver_prices, constructor_prices
        except ValueError:
            continue

    _debug_log("AJAX payload could not be parsed as structured JSON or HTML price content.")
    return None, None


def fetch_prices_via_ajax(session, html, page_url, headers):
    ajax_url, security, script_url = _discover_ajax_context(html, page_url)
    if not ajax_url or not script_url or not security:
        _debug_log("Missing AJAX context (ajax_url, security token, or script_url).")
        return None, None

    try:
        js_response = session.get(script_url, headers=headers, timeout=30)
        js_response.raise_for_status()
    except requests_exceptions.RequestException:
        _debug_log(f"Unable to fetch AJAX script: {script_url}")
        return None, None

    actions = _discover_ajax_actions(js_response.text)
    if not actions:
        _debug_log("No AJAX action names discovered from script.")
        return None, None

    nonce_keys = ["security", "nonce", "_ajax_nonce"]
    for action in actions:
        for nonce_key in nonce_keys:
            payload = {"action": action, nonce_key: security}
            try:
                response = session.post(ajax_url, data=payload, headers=headers, timeout=30)
                response.raise_for_status()
            except requests_exceptions.RequestException:
                _debug_log(f"AJAX request failed for action={action}, nonce_key={nonce_key}")
                continue

            driver_prices, constructor_prices = _extract_prices_from_ajax_payload(response.text)
            if driver_prices is not None and constructor_prices is not None:
                _debug_log(f"AJAX extraction succeeded for action={action}, nonce_key={nonce_key}")
                return driver_prices, constructor_prices

    _debug_log("AJAX extraction exhausted all action/nonce combinations without success.")
    return None, None


def extract_driver_constructor_prices(html):
    tables = _html_to_tables(html)
    price_tables = _pick_price_tables(tables)

    if not price_tables:
        driver_prices, constructor_prices = _extract_prices_from_cards(html)
        if driver_prices is not None and constructor_prices is not None:
            return driver_prices, constructor_prices
        _debug_log("No price tables or cards could be parsed from page HTML.")
        raise ValueError("Could not identify price data in the page HTML.")

    by_type = {table_type: table for table_type, table in price_tables if table_type in {"driver", "constructor"}}

    if "driver" in by_type and "constructor" in by_type:
        driver_prices = _prepare_price_dataframe(by_type["driver"], "driver")
        constructor_prices = _prepare_price_dataframe(by_type["constructor"], "constructor")
        return driver_prices, constructor_prices

    if len(price_tables) >= 2:
        first = _prepare_price_dataframe(price_tables[0][1], "driver")
        second = _prepare_price_dataframe(price_tables[1][1], "constructor")
        return first, second

    raise ValueError(
        "Found only one untyped price table; could not safely split driver and constructor prices."
    )


def combine_prices_for_ranking(driver_prices, constructor_prices):
    combined = pd.concat([driver_prices, constructor_prices], ignore_index=True)
    combined = combined.sort_values(["EntityType", "Name"]).reset_index(drop=True)
    return combined


def _extract_attr(tag, attr_name):
    pattern = rf"{attr_name}=[\"\']([^\"\']+)[\"\']"
    match = re.search(pattern, tag, flags=re.IGNORECASE)
    return match.group(1) if match else None


def _discover_login_form(html, base_url):
    forms = list(re.finditer(r"<form[^>]*>(.*?)</form>", html, flags=re.IGNORECASE | re.DOTALL))
    if not forms:
        return None

    for form_match in forms:
        form_tag = form_match.group(0)
        form_body = form_match.group(1)

        action = _extract_attr(form_tag, "action") or base_url
        action_url = requests.compat.urljoin(base_url, action)

        payload = {}
        username_key = None
        password_key = None

        for input_match in re.finditer(r"<input[^>]*>", form_body, flags=re.IGNORECASE):
            input_tag = input_match.group(0)
            field_name = _extract_attr(input_tag, "name")
            if not field_name:
                continue

            field_type = (_extract_attr(input_tag, "type") or "text").lower()
            field_val = _extract_attr(input_tag, "value") or ""

            if field_type == "password":
                password_key = field_name
                continue

            lowered = field_name.lower()
            if any(token in lowered for token in ["user", "email", "log", "login"]):
                username_key = field_name
                continue

            if field_type in ["hidden", "submit"]:
                payload[field_name] = field_val

        if password_key:
            return action_url, payload, username_key, password_key

    return None


def fetch_authenticated_html(url, username, password):
    html, _, _ = fetch_authenticated_page(url, username, password)
    return html


def fetch_authenticated_page(url, username, password):
    headers = {"User-Agent": "Mozilla/5.0 (compatible; f1-points-bot/1.0)"}

    session = requests.Session()
    try:
        first_response = session.get(url, headers=headers, timeout=30)
        first_response.raise_for_status()
    except requests_exceptions.ProxyError:
        # Some environments inject an HTTPS proxy that blocks fantasygp.com.
        # Retry once without proxy env vars.
        session = requests.Session()
        session.trust_env = False
        try:
            first_response = session.get(url, headers=headers, timeout=30)
            first_response.raise_for_status()
        except requests_exceptions.RequestException as exc:
            raise RuntimeError(
                "Unable to reach FantasyGP. Proxy tunnel was rejected and direct connection also failed. "
                "Check network egress/proxy allowlist for fantasygp.com."
            ) from exc

    login_info = _discover_login_form(first_response.text, first_response.url)
    if login_info is None:
        return first_response.text, session, headers

    action_url, payload, username_key, password_key = login_info

    if username_key is None or password_key is None:
        raise ValueError("Could not determine login form field names for username/password.")

    payload[username_key] = username
    payload[password_key] = password

    login_response = session.post(action_url, data=payload, headers=headers, timeout=30)
    login_response.raise_for_status()

    final_response = session.get(url, headers=headers, timeout=30)
    final_response.raise_for_status()
    return final_response.text, session, headers


def save_prices(driver_prices, constructor_prices):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    today = datetime.datetime.now(datetime.timezone.utc).date().isoformat()

    combined_prices = combine_prices_for_ranking(driver_prices, constructor_prices)

    driver_current_path = os.path.join(OUTPUT_DIR, "fantasygp_driver_prices_current.csv")
    constructor_current_path = os.path.join(OUTPUT_DIR, "fantasygp_constructor_prices_current.csv")
    combined_current_path = os.path.join(OUTPUT_DIR, "fantasygp_prices_current.csv")

    driver_archive_path = os.path.join(OUTPUT_DIR, f"fantasygp_driver_prices_{today}.csv")
    constructor_archive_path = os.path.join(OUTPUT_DIR, f"fantasygp_constructor_prices_{today}.csv")
    combined_archive_path = os.path.join(OUTPUT_DIR, f"fantasygp_prices_{today}.csv")

    driver_prices.to_csv(driver_current_path, index=False)
    constructor_prices.to_csv(constructor_current_path, index=False)
    combined_prices.to_csv(combined_current_path, index=False)

    driver_prices.to_csv(driver_archive_path, index=False)
    constructor_prices.to_csv(constructor_archive_path, index=False)
    combined_prices.to_csv(combined_archive_path, index=False)

    print(f"Saved driver prices to: {driver_current_path} and {driver_archive_path}")
    print(f"Saved constructor prices to: {constructor_current_path} and {constructor_archive_path}")
    print(f"Saved combined prices to: {combined_current_path} and {combined_archive_path}")


def main():
    username = os.environ.get("FANTASYGP_USERNAME")
    password = os.environ.get("FANTASYGP_PASSWORD")

    if not username or not password:
        raise EnvironmentError(
            "Missing FantasyGP credentials. Set FANTASYGP_USERNAME and FANTASYGP_PASSWORD environment variables."
        )

    html, session, headers = fetch_authenticated_page(TARGET_URL, username, password)

    try:
        driver_prices, constructor_prices = extract_driver_constructor_prices(html)
    except ValueError:
        driver_prices, constructor_prices = fetch_prices_via_ajax(session, html, TARGET_URL, headers)
        if driver_prices is None or constructor_prices is None:
            raise

    save_prices(driver_prices, constructor_prices)


if __name__ == "__main__":
    main()
