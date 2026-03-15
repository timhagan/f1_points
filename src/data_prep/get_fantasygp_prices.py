import datetime
import os
import re
from html.parser import HTMLParser

import pandas as pd
import requests
from requests import exceptions as requests_exceptions

TARGET_URL = os.environ.get("FANTASYGP_TARGET_URL", "https://fantasygp.com/drivers-cars/")
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data")


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


def extract_driver_constructor_prices(html):
    tables = _html_to_tables(html)
    price_tables = _pick_price_tables(tables)

    if not price_tables:
        raise ValueError("Could not identify any price tables in the page HTML.")

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
        return first_response.text

    action_url, payload, username_key, password_key = login_info

    if username_key is None or password_key is None:
        raise ValueError("Could not determine login form field names for username/password.")

    payload[username_key] = username
    payload[password_key] = password

    login_response = session.post(action_url, data=payload, headers=headers, timeout=30)
    login_response.raise_for_status()

    final_response = session.get(url, headers=headers, timeout=30)
    final_response.raise_for_status()
    return final_response.text


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

    html = fetch_authenticated_html(TARGET_URL, username, password)
    driver_prices, constructor_prices = extract_driver_constructor_prices(html)
    save_prices(driver_prices, constructor_prices)


if __name__ == "__main__":
    main()
