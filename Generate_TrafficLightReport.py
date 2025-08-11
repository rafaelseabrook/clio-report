import json
import os
import requests
import webbrowser
import threading
import re
from flask import Flask, request
import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import PatternFill
from openpyxl.worksheet.table import Table, TableStyleInfo
from openpyxl.styles import NamedStyle
from openpyxl.styles import Font
from datetime import datetime
from urllib.parse import quote
import time
import msal

app = Flask(__name__)

# =========================
# Config / Environment
# =========================
CLIO_BASE = os.getenv("CLIO_BASE", "https://app.clio.com")
API_VERSION = "4"
CLIO_API = f"{CLIO_BASE}/api/v{API_VERSION}"
CLIO_TOKEN_URL = f"{CLIO_BASE}/oauth/token"

# Clio OAuth (tokens are expected to be pre-seeded in env on Render)
CLIENT_ID = os.getenv("CLIO_CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIO_CLIENT_SECRET")
REDIRECT_URI = os.getenv("CLIO_REDIRECT_URI")

# Token storage (env-based for Render)
def save_tokens_env(tokens):
    os.environ["CLIO_ACCESS_TOKEN"] = tokens['access_token']
    os.environ["CLIO_REFRESH_TOKEN"] = tokens['refresh_token']
    os.environ["CLIO_EXPIRES_IN"] = str(tokens['expires_in'])

def load_tokens_env():
    access_token = os.getenv("CLIO_ACCESS_TOKEN")
    refresh_token = os.getenv("CLIO_REFRESH_TOKEN")
    expires_in = os.getenv("CLIO_EXPIRES_IN")
    if access_token and refresh_token and expires_in:
        return {
            "access_token": access_token,
            "refresh_token": refresh_token,
            "expires_in": float(expires_in)
        }
    return None

PAGE_LIMIT = 50  # Clio hard max
GLOBAL_MIN_SLEEP = float(os.getenv("CLIO_GLOBAL_MIN_SLEEP_SEC", "1.25"))

session = requests.Session()
session.headers.update({"Accept": "application/json"})

# =========================
# Auth and Request Helpers
# =========================
def ensure_auth_headers():
    if "Authorization" not in session.headers or not session.headers.get("Authorization"):
        token = get_access_token()
        if token:
            session.headers["Authorization"] = f"Bearer {token}"
        else:
            raise RuntimeError("Authorization missing; cannot proceed.")

def get_access_token():
    tokens = load_tokens_env()
    if tokens:
        if datetime.now().timestamp() < tokens['expires_in']:
            return tokens['access_token']
        else:
            return refresh_access_token(tokens['refresh_token'])
    else:
        # On Render, we expect tokens to be pre-seeded.
        print("No tokens found in env. Run locally once to authorize and seed tokens.")
        return None

def refresh_access_token(refresh_token):
    resp = session.post(CLIO_TOKEN_URL, data={
        'grant_type': 'refresh_token',
        'refresh_token': refresh_token,
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET
    }, timeout=45)
    if resp.status_code == 200:
        tokens = resp.json()
        tokens['expires_in'] = datetime.now().timestamp() + tokens['expires_in']
        save_tokens_env(tokens)
        session.headers["Authorization"] = f"Bearer {tokens['access_token']}"
        return tokens['access_token']
    else:
        print(f"Failed to refresh access token: {resp.status_code}, {resp.text}")
        raise Exception('Failed to refresh access token.')

@app.route('/callback')
def callback():
    # Mostly unused on Render cron, but keep it intact
    auth_code = request.args.get('code')
    response = session.post(CLIO_TOKEN_URL, data={
        'grant_type': 'authorization_code',
        'code': auth_code,
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'redirect_uri': REDIRECT_URI
    }, timeout=45)
    if response.status_code == 200:
        tokens = response.json()
        tokens['expires_in'] = datetime.now().timestamp() + tokens['expires_in']
        # fix: save to env version
        save_tokens_env(tokens)
        fetch_and_process_data()
        return 'Authorization complete. Data processing initiated.'
    else:
        print(f"Authorization failed: {response.status_code}, {response.text}")
        return 'Authorization failed.'

def _sleep_with_floor(start_ts: float, retry_after: int | None = None):
    elapsed = time.time() - start_ts
    base_wait = max(0, GLOBAL_MIN_SLEEP - elapsed)
    wait = max(base_wait, retry_after or 0)
    if wait > 0:
        time.sleep(wait)

def _request(method, url, **kwargs) -> requests.Response:
    ensure_auth_headers()
    max_tries = kwargs.pop("max_tries", 7)
    backoff = 1
    for _ in range(max_tries):
        t0 = time.time()
        resp = session.request(method, url, timeout=45, **kwargs)

        if resp.status_code == 401:
            toks = load_tokens_env()
            if toks and toks.get("refresh_token"):
                print("401 Unauthorized. Refreshing token…")
                refresh_access_token(toks["refresh_token"])
                _sleep_with_floor(t0)
                continue

        if resp.status_code == 429:
            # Honor Retry-After header or parse body, fallback 30s
            ra = 30
            if resp.headers.get("Retry-After"):
                try:
                    ra = int(resp.headers["Retry-After"])
                except ValueError:
                    ra = 30
            else:
                try:
                    retry_msg = resp.json().get("error", {}).get("message", "")
                    m = re.search(r"Retry in (\d+)", retry_msg)
                    if m:
                        ra = int(m.group(1))
                except Exception:
                    pass
            print(f"[429] Rate limited. Waiting {ra}s …")
            _sleep_with_floor(t0, retry_after=ra)
            continue

        if 500 <= resp.status_code < 600:
            print(f"[{resp.status_code}] Server error. Retrying in {backoff}s …")
            _sleep_with_floor(t0, retry_after=backoff)
            backoff = min(backoff * 2, 60)
            continue

        _sleep_with_floor(t0)
        return resp
    return resp

# =========================
# Generic fetch with paging (limit=50 + page_token)
# =========================
def fetch_data(url, params):
    params = dict(params or {})
    params["limit"] = PAGE_LIMIT

    all_rows = []
    seen_tokens = set()
    page_token = None

    while True:
        if page_token:
            params["page_token"] = page_token
        resp = _request("GET", url, params=params)
        print(f"Fetching data from {url} with params {params}. Status code: {resp.status_code}")

        if resp.status_code != 200:
            print(f"Failed to fetch data: {resp.status_code}, {resp.text[:200]}")
            break

        body = resp.json() or {}
        rows = body.get("data", [])
        meta = body.get("meta", {}) if isinstance(body, dict) else {}
        next_token = meta.get("next_page_token") or meta.get("next_token")

        # accumulate
        all_rows.extend([r for r in rows if isinstance(r, dict)])

        # continue if token-based paging
        if next_token and next_token not in seen_tokens:
            seen_tokens.add(next_token)
            page_token = next_token
            continue

        # fallback stopping condition on count
        if len(rows) < PAGE_LIMIT or not next_token:
            break

    return all_rows, len(all_rows)

# =========================
# Clio fetchers
# =========================
def fetch_matters_with_balances():
    # First-level fields only (avoid 2nd-level nesting that 400s)
    url = f"{CLIO_API}/matters.json"
    params = {
        'fields': 'id,number,display_number,description,client{id,name},responsible_attorney{name},status,matter_stage{name},account_balances{balance}',
        'status': 'open,pending'
    }
    return fetch_data(url, params)[0]

# (client-level; used as a fallback if bills can't give us client balances)
def fetch_outstanding_balances():
    url = f"{CLIO_API}/outstanding_client_balances.json"
    params = {
        'fields': 'contact{id,name},total_outstanding_balance'
    }
    return fetch_data(url, params)[0]

# billable_matters is client-level; do not request nested matter here
def fetch_work_progress():
    url = f"{CLIO_API}/billable_matters.json"
    params = {
        'fields': 'unbilled_amount,unbilled_hours,client{id,name}'
    }
    matters, _ = fetch_data(url, params)
    print(f"Fetched {len(matters)} billable matters.")
    rows = []
    for w in matters or []:
        rows.append({
            'Client ID': (w.get('client') or {}).get('id'),
            'Client Name': (w.get('client') or {}).get('name'),
            'Unbilled Amount': w.get('unbilled_amount', 0) or 0,
            'Unbilled Hours': w.get('unbilled_hours', 0) or 0
        })
    return rows

def fetch_billable_hours(start_date, end_date):
    url = f"{CLIO_API}/activities.json"
    params = {
        'start_date': start_date,
        'end_date': end_date,
        'status': 'billable',
        'order': 'date(desc)',
        'fields': 'id,quantity,rounded_quantity,date,matter{id,display_number,number},user{name},type,note,total',
        'limit': PAGE_LIMIT,
        'type': 'TimeEntry'
    }

    matter_totals = {}
    page_token = None

    while True:
        if page_token:
            params['page_token'] = page_token
        response = _request("GET", url, params=params)
        if response.status_code != 200:
            print(f"API error: {response.status_code} - {response.text[:300]}")
            return {}

        data = response.json() or {}
        rows = data.get('data', [])
        meta = data.get('meta', {})
        page_token = meta.get('next_page_token')

        for entry in rows:
            if not isinstance(entry, dict):
                continue
            if entry.get('type') != 'TimeEntry':
                continue

            matter = entry.get('matter') or {}
            matter_number = matter.get('number') or matter.get('display_number')
            if not matter_number:
                continue

            # seconds -> hours
            rounded_seconds = entry.get('rounded_quantity') or entry.get('quantity') or 0
            try:
                hours = float(rounded_seconds) / 3600.0
            except Exception:
                hours = 0.0

            user_name = (entry.get('user') or {}).get('name', 'Unknown User')
            print(f"Debug: Processing time entry for matter {matter_number}, user {user_name}, hours {hours}")

            bucket = matter_totals.setdefault(matter_number, {'total_hours': 0.0, 'user_hours': {}})
            bucket['total_hours'] += hours
            bucket['user_hours'][user_name] = bucket['user_hours'].get(user_name, 0.0) + hours

        if not page_token or len(rows) < PAGE_LIMIT:
            break

    print(f"Debug: Total matters with billable hours: {len(matter_totals)}")
    for matter_number, d in list(matter_totals.items())[:3]:
        print(f"Matter {matter_number}: {d}")

    return matter_totals

# NEW: Bills → outstanding by MATTER and by CLIENT (for fallback)
def fetch_outstanding_from_bills():
    url = f"{CLIO_API}/bills.json"
    params = {
        'fields': 'id,state,balance,matters{number,display_number},contact{id,name}'
    }
    bills, _ = fetch_data(url, params)

    by_matter = {}
    by_client = {}
    for b in bills or []:
        state = (b.get('state') or '').lower()
        if state in ('paid', 'void'):
            continue
        try:
            bal = float(b.get('balance') or 0.0)
        except Exception:
            bal = 0.0

        # accumulate by client (contact) for fallback
        contact = b.get('contact') or {}
        cid = contact.get('id')
        if cid:
            by_client[cid] = by_client.get(cid, 0.0) + bal

        # accumulate by each matter on the bill, if any
        for m in (b.get('matters') or []):
            mnum = m.get('number') or m.get('display_number')
            if not mnum:
                continue
            by_matter[mnum] = by_matter.get(mnum, 0.0) + bal

    print(f"Computed outstanding from bills → by_matter: {len(by_matter)} matters, by_client: {len(by_client)} clients.")
    return by_matter, by_client

# =========================
# Custom Fields (returns Client ID too)
# =========================
def fetch_custom_fields():
    """Fetch all custom fields and log their details."""
    url = f'{CLIO_API}/custom_fields.json'
    params = {'fields': 'id,name,field_type,picklist_options'}
    custom_fields, _ = fetch_data(url, params)

    print("Custom Fields Retrieved:")
    for item in custom_fields:
        process_custom_field(item)
    return custom_fields

def process_custom_field(field):
    field_id = field.get('id', 'N/A')
    field_name = field.get('name', 'N/A')
    field_type = field.get('field_type', 'N/A')
    print(f"ID: {field_id}, Name: {field_name}, Type: {field_type}")
    if field_type == 'picklist' and isinstance(field.get('picklist_options'), list):
        print("Picklist Options:\n" + "\n".join(
            f"  Option ID: {opt.get('id','N/A')}, Value: {opt.get('option','N/A')}"
            for opt in field.get('picklist_options') or []
        ))
    else:
        print("  No picklist options available.")

def flatten_list(nested_list):
    for item in nested_list:
        if isinstance(item, list):
            yield from flatten_list(item)
        else:
            yield item

def fetch_open_matters_with_custom_fields(paralegal_field_ids, picklist_mappings, client_notes_id):
    url = f'{CLIO_API}/matters.json'
    params = {
        'fields': 'id,number,display_number,client{id,name},custom_field_values{id,field_name,field_type,value,picklist_option}',
        'status': 'open,pending'
    }
    matters, _ = fetch_data(url, params)

    processed_data = []
    desired_fields = {
        'Client Notes': client_notes_id,
        'Main Paralegal': paralegal_field_ids.get('Main Paralegal'),
        'Supporting Paralegal': paralegal_field_ids.get('Supporting Paralegal'),
        'Supporting Attorney': paralegal_field_ids.get('Supporting Attorney'),
        'CR ID': paralegal_field_ids.get('CR ID'),
        'Initial Client Goals': paralegal_field_ids.get('Initial Client Goals'),
        'Initial Strategy': paralegal_field_ids.get('Initial Strategy'),
        'Has strategy changed Describe': paralegal_field_ids.get('Has strategy changed Describe'),
        'Current action Items': paralegal_field_ids.get('Current action Items'),
        'Hearings': paralegal_field_ids.get('Hearings'),
        'Deadlines': paralegal_field_ids.get('Deadlines'),
        'DV situation description': paralegal_field_ids.get('DV situation description'),
        'Custody Visitation': paralegal_field_ids.get('Custody Visitation'),
        'CS Add ons Extracurricular': paralegal_field_ids.get('CS Add ons Extracurricular'),
        'Spousal Support': paralegal_field_ids.get('Spousal Support'),
        'PDDs': paralegal_field_ids.get('PDDs'),
        'Discovery': paralegal_field_ids.get('Discovery'),
        'Judgment Trial': paralegal_field_ids.get('Judgment Trial'),
        'Post Judgment': paralegal_field_ids.get('Post Judgment')
    }

    if isinstance(matters, dict):
        matters = [matters]
    matters = list(flatten_list(matters))

    for matter in matters:
        if not isinstance(matter, dict):
            continue

        matter_number = matter.get('number') or matter.get('display_number') or matter.get('id')
        if not matter_number:
            continue

        client = matter.get('client') or {}
        client_id = client.get('id')

        # Initialize with all fields
        matter_data = {
            'Matter Number': matter_number,
            'Client ID': client_id,
            'Main Paralegal': '',
            'Supporting Paralegal': '',
            'Supporting Attorney': '',
            'Client Notes': '',
            'CR ID': '',
            'Initial Client Goals': '',
            'Initial Strategy': '',
            'Has strategy changed Describe': '',
            'Current action Items': '',
            'Hearings': '',
            'Deadlines': '',
            'DV situation description': '',
            'Custody Visitation': '',
            'CS Add ons Extracurricular': '',
            'Spousal Support': '',
            'PDDs': '',
            'Discovery': '',
            'Judgment Trial': '',
            'Post Judgment': ''
        }

        cfields = matter.get('custom_field_values', [])
        if isinstance(cfields, list):
            for field in cfields:
                if not isinstance(field, dict):
                    continue
                field_name = field.get('field_name')
                if field_name not in desired_fields:
                    continue
                ftype = field.get('field_type')
                if ftype == 'picklist' and field.get('picklist_option'):
                    picklist_value = field['picklist_option'].get('option', '')
                    matter_data[field_name] = picklist_value
                else:
                    matter_data[field_name] = field.get('value') or ''

        processed_data.append(matter_data)

    columns = ['Matter Number', 'Client ID', 'Main Paralegal', 'Supporting Paralegal',
               'Supporting Attorney', 'Client Notes', 'CR ID',
               'Initial Client Goals', 'Initial Strategy', 'Has strategy changed Describe',
               'Current action Items', 'Hearings', 'Deadlines', 'DV situation description',
               'Custody Visitation', 'CS Add ons Extracurricular', 'Spousal Support',
               'PDDs', 'Discovery', 'Judgment Trial', 'Post Judgment']

    if not processed_data:
        return pd.DataFrame(columns=columns)
    df = pd.DataFrame(processed_data)
    for col in columns:
        if col not in df.columns:
            df[col] = ''
    return df[columns]

# =========================
# Aggregation helpers
# =========================
def get_billing_cycle_totals(matter_number, billing_data):
    if matter_number in billing_data:
        return {
            'total_hours': billing_data[matter_number]['total_hours'],
            'user_breakdown': billing_data[matter_number]['user_hours']
        }
    return {'total_hours': 0, 'user_breakdown': {}}

def get_current_month_totals():
    current_date = datetime.now()
    start_date = current_date.replace(day=1).strftime('%Y-%m-%dT00:00:00-08:00')
    end_date = current_date.strftime('%Y-%m-%dT23:59:59-08:00')
    billing_data = fetch_billable_hours(start_date, end_date)
    user_totals = {}
    for matter_data in billing_data.values():
        for user, hours in matter_data['user_hours'].items():
            user_totals[user] = user_totals.get(user, 0) + hours
    return user_totals

def get_user_totals(billing_data):
    user_totals = {}
    for matter_data in billing_data.values():
        for user, hours in matter_data.get('user_hours', {}).items():
            user_totals[user] = user_totals.get(user, 0) + hours
    return user_totals

def normalize_name(name):
    if not isinstance(name, str):
        return 'N/A'
    if ',' in name:
        last, first = name.split(',', 1)
        return f"{first.strip()} {last.strip()}"
    else:
        return name.strip()

# =========================
# Build dataframes (ID-based)
# =========================
def process_data():
    print("Fetching matters with balances...")
    matters = fetch_matters_with_balances()
    print(f"Fetched {len(matters)} matters with balances.")

    print("Fetching outstanding balances (client-level fallback)...")
    outstanding_clients = fetch_outstanding_balances() or []
    print(f"Fetched {len(outstanding_clients)} outstanding client balances.")

    print("Fetching work progress (client-level)...")
    work_progress = fetch_work_progress()
    print(f"Fetched {len(work_progress)} work progress items.")

    print("Computing outstanding by matter/client from Bills...")
    out_by_matter, out_by_client_bills = fetch_outstanding_from_bills()

    # ---- Matters / trust balances ----
    matter_trusts_rows = []
    for m in matters or []:
        acct = m.get('account_balances') or []
        total_trust_balance = 0.0
        for bal in acct:
            try:
                amount = float((bal or {}).get('balance', 0) or 0)
                total_trust_balance += amount
            except Exception:
                pass

        matter_stage = m.get('matter_stage')
        matter_stage_name = matter_stage.get('name', 'N/A') if isinstance(matter_stage, dict) else 'N/A'
        client = m.get('client') or {}

        matter_trusts_rows.append({
            'Matter Number': m.get('number') or m.get('display_number') or m.get('id'),
            'Client ID': client.get('id'),
            'Client Name': normalize_name(client.get('name', 'N/A')),
            'Trust Account Balance': total_trust_balance,
            'Responsible Attorney': (m.get('responsible_attorney') or {}).get('name', 'N/A'),
            'Status': m.get('status', 'N/A'),
            'Matter Stage': matter_stage_name
        })

    mt_cols = ['Matter Number', 'Client ID', 'Client Name', 'Trust Account Balance', 'Responsible Attorney', 'Status', 'Matter Stage']
    matter_trusts_df = pd.DataFrame(matter_trusts_rows, columns=mt_cols)

    # ---- Work progress (client) ----
    work_cols = ['Client ID', 'Client Name', 'Unbilled Amount', 'Unbilled Hours']
    work_progress_df = pd.DataFrame(work_progress, columns=work_cols) if work_progress else pd.DataFrame(columns=work_cols)

    # ---- Outstanding by matter (from bills) ----
    ocb_matter_df = pd.DataFrame(
        [{'Matter Number': k, 'Outstanding Balance (Matter)': v} for k, v in (out_by_matter or {}).items()],
        columns=['Matter Number', 'Outstanding Balance (Matter)']
    )

    # ---- Outstanding by client (from bills) ----
    ocb_client_bills_df = pd.DataFrame(
        [{'Client ID': k, 'Outstanding Balance (Client)': v} for k, v in (out_by_client_bills or {}).items()],
        columns=['Client ID', 'Outstanding Balance (Client)']
    )

    # ---- Outstanding by client (from outstanding_client_balances) as last fallback ----
    ocb_client_api = pd.DataFrame(
        [{
            'Client ID': (row.get('contact') or {}).get('id'),
            'Outstanding Balance (Client API)': row.get('total_outstanding_balance', 0) or 0
        } for row in outstanding_clients if isinstance(row, dict)],
        columns=['Client ID', 'Outstanding Balance (Client API)']
    )

    print("trusts:", matter_trusts_df.shape, list(matter_trusts_df.columns))
    print("ocb_matter:", ocb_matter_df.shape, list(ocb_matter_df.columns))
    print("ocb_client_bills:", ocb_client_bills_df.shape, list(ocb_client_bills_df.columns))
    print("ocb_client_api:", ocb_client_api.shape, list(ocb_client_api.columns))
    print("work (client-level):", work_progress_df.shape, list(work_progress_df.columns))

    return matter_trusts_df, ocb_matter_df, ocb_client_bills_df, ocb_client_api, work_progress_df

# =========================
# Merge and compute
# =========================
def merge_dataframes(matter_trusts_df, ocb_matter_df, ocb_client_bills_df, ocb_client_api_df, work_progress_df, billing_cycle_data, cycle_start_date=None, cycle_end_date=None):
    print("Merging dataframes...")

    # Billing window label
    billing_cycle_start = cycle_start_date or "07/16/25"
    billing_cycle_end = cycle_end_date or "07/29/25"
    billing_cycle_column = f"Billing Cycle Hours ({billing_cycle_start} - {billing_cycle_end})"

    combined_df = (
        matter_trusts_df
        # 1) matter-level outstanding if present
        .merge(ocb_matter_df, on='Matter Number', how='left')
        # 2) client-level outstanding from bills
        .merge(ocb_client_bills_df, on='Client ID', how='left')
        # 3) client-level outstanding from /outstanding_client_balances (fallback of fallback)
        .merge(ocb_client_api_df, on='Client ID', how='left')
        # 4) unbilled (client-level)
        .merge(work_progress_df[['Client ID', 'Unbilled Amount', 'Unbilled Hours']], on='Client ID', how='left')
    )

    # Choose outstanding: prefer matter-level, else bills-by-client, else API-by-client
    for c in ['Outstanding Balance (Matter)', 'Outstanding Balance (Client)', 'Outstanding Balance (Client API)']:
        if c not in combined_df.columns:
            combined_df[c] = 0.0

    combined_df['Outstanding Balance'] = combined_df['Outstanding Balance (Matter)']
    mask = (combined_df['Outstanding Balance'].isna()) | (combined_df['Outstanding Balance'] == 0)
    combined_df.loc[mask, 'Outstanding Balance'] = combined_df.loc[mask, 'Outstanding Balance (Client)']
    mask2 = (combined_df['Outstanding Balance'].isna()) | (combined_df['Outstanding Balance'] == 0)
    combined_df.loc[mask2, 'Outstanding Balance'] = combined_df.loc[mask2, 'Outstanding Balance (Client API)']

    # Clean up helpers
    combined_df.drop(columns=[col for col in ['Outstanding Balance (Matter)', 'Outstanding Balance (Client)', 'Outstanding Balance (Client API)'] if col in combined_df.columns], inplace=True)

    # Numeric coercion and fill
    numeric_columns = ['Trust Account Balance', 'Outstanding Balance', 'Unbilled Amount', 'Unbilled Hours']
    for col in numeric_columns:
        if col in combined_df.columns:
            combined_df[col] = pd.to_numeric(combined_df[col], errors='coerce').fillna(0.0)

    # Calculate net trust (now non-zero even when bills aren't matter-linked)
    combined_df['Net Trust Account Balance'] = (
        combined_df['Trust Account Balance'] -
        combined_df['Outstanding Balance'] -
        combined_df['Unbilled Amount']
    )

    # Billing totals
    combined_df[billing_cycle_column] = combined_df['Matter Number'].apply(
        lambda x: get_billing_cycle_totals(x, billing_cycle_data)['total_hours']
    )

    # User breakdown columns
    user_columns = set()
    for matter_data in billing_cycle_data.values():
        user_columns.update(matter_data.get('user_hours', {}).keys())

    for user in sorted(user_columns):
        combined_df[f'{user} Cycle Hours ({billing_cycle_start} - {billing_cycle_end})'] = combined_df['Matter Number'].apply(
            lambda x: get_billing_cycle_totals(x, billing_cycle_data)['user_breakdown'].get(user, 0)
        )

    # Columns to display
    calculation_columns = ['Trust Account Balance', 'Outstanding Balance', 'Unbilled Amount']
    display_columns_order = [
        'Matter Number', 'Client Name', 'CR ID', 'Net Trust Account Balance',
        'Status', 'Matter Stage', 'Responsible Attorney', 'Unbilled Hours',
        'Main Paralegal', 'Supporting Paralegal', 'Supporting Attorney', 'Client Notes'
    ]
    for col in display_columns_order:
        if col not in combined_df.columns:
            combined_df[col] = 0 if col in (calculation_columns + ['Unbilled Hours']) else ''

    # Billing cols
    billing_columns = [billing_cycle_column] + [col for col in combined_df.columns if 'Cycle Hours' in col and col != billing_cycle_column]

    # Additional custom fields after billing columns
    additional_fields = [
        'Initial Client Goals', 'Initial Strategy', 'Has strategy changed Describe',
        'Current action Items', 'Hearings', 'Deadlines', 'DV situation description',
        'Custody Visitation', 'CS Add ons Extracurricular', 'Spousal Support',
        'PDDs', 'Discovery', 'Judgment Trial', 'Post Judgment'
    ]

    final_columns = display_columns_order + billing_columns + additional_fields
    final_columns = [c for c in final_columns if c in combined_df.columns]

    # Prefer having Client Name populated
    if 'Client Name' in combined_df.columns:
        combined_df['Client Name'] = combined_df['Client Name'].fillna('N/A')

    return combined_df[final_columns].sort_values(by='Net Trust Account Balance', ascending=False)

# =========================
# Excel formatting + save
# =========================
def apply_conditional_and_currency_formatting_with_totals(previous_cycle_df, mid_cycle_df, 
                                                        mid_cycle_data, current_cycle_data,
                                                        mid_cycle_start_formatted,
                                                        mid_cycle_end_formatted,
                                                        current_date_formatted,
                                                        output_file):
    print(f"Applying formatting and saving to {output_file}...")
    
    # Create Excel writer and save main sheets
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        # Add totals row to each DataFrame before saving
        for df in [previous_cycle_df, mid_cycle_df]:
            time_columns = [col for col in df.columns if 'Cycle Hours' in col]
            totals = df[time_columns].sum(numeric_only=True) if time_columns else pd.Series(dtype=float)
            totals_row = pd.Series('', index=df.columns)
            totals_row['Matter Number'] = 'TOTALS'
            for col in time_columns:
                totals_row[col] = totals.get(col, 0.0)
            df_with_totals = pd.concat([df, pd.DataFrame([totals_row])], ignore_index=True)
            if df is previous_cycle_df:
                df_with_totals.to_excel(writer, sheet_name='Previous Billing Cycle', index=False)
            else:
                df_with_totals.to_excel(writer, sheet_name='Mid Cycle', index=False)
        
        # Totals sheet
        mid_cycle_totals = get_user_totals(mid_cycle_data)
        current_totals_df = pd.DataFrame([
            {'User': user, f'Cycle Running Total ({mid_cycle_start_formatted} - {current_date_formatted})': hours}
            for user, hours in sorted(get_user_totals(current_cycle_data).items(), key=lambda x: x[1], reverse=True)
        ])
        mid_totals_df = pd.DataFrame([
            {'User': user, f'Cycle Hours ({mid_cycle_start_formatted} - {mid_cycle_end_formatted})': hours}
            for user, hours in sorted(mid_cycle_totals.items(), key=lambda x: x[1], reverse=True)
        ])
        mid_totals_df.to_excel(writer, sheet_name='Billable Hour Totals', startrow=1, index=False)
        current_totals_df.to_excel(writer, sheet_name='Billable Hour Totals', startrow=mid_totals_df.shape[0] + 5, index=False)
    
    # Load workbook for formatting
    wb = load_workbook(output_file)
    
    # Define fill styles
    red_fill = PatternFill(start_color='FFC7CE', end_color='FFC7CE', fill_type='solid')
    yellow_fill = PatternFill(start_color='FFEB9C', end_color='FFEB9C', fill_type='solid')
    green_fill = PatternFill(start_color='C6EFCE', end_color='C6EFCE', fill_type='solid')
    total_row_fill = PatternFill(start_color='E7E6E6', end_color='E7E6E6', fill_type='solid')  # Gray background for totals row
    
    # Define styles
    currency_style = NamedStyle(name='currency', number_format='$#,##0.00')
    bold_font = Font(bold=True)
    
    # Format main sheets
    for sheet_name in ['Previous Billing Cycle', 'Mid Cycle']:
        ws = wb[sheet_name]
        last_row = ws.max_row
        
        net_balance_col = None
        time_cols = []
        for col_idx, cell in enumerate(ws[1], 1):
            if cell.value == 'Net Trust Account Balance':
                net_balance_col = col_idx
            if 'Cycle Hours' in str(cell.value):
                time_cols.append(col_idx)
        
        # Apply formatting (except totals row)
        for row in ws.iter_rows(min_row=2, max_row=last_row-1):
            for col_idx, cell in enumerate(row, 1):
                if ws[1][col_idx - 1].value in ['Trust Account Balance', 'Outstanding Balance', 'Unbilled Amount', 'Net Trust Account Balance']:
                    cell.style = currency_style
            if net_balance_col:
                net_cell = row[net_balance_col - 1]
                try:
                    val = float(net_cell.value or 0)
                    if val <= 0:
                        net_cell.fill = red_fill
                    elif 0 < val < 1000:
                        net_cell.fill = yellow_fill
                    else:
                        net_cell.fill = green_fill
                except ValueError:
                    pass
        
        # Totals row styling
        totals_row = ws[last_row]
        for cell in totals_row:
            cell.font = bold_font
            cell.fill = total_row_fill
            if cell.column in time_cols:
                cell.number_format = '#,##0.00'
        
        # Add table style (excluding totals row)
        table_ref = f"A1:{ws.cell(row=last_row-1, column=ws.max_column).coordinate}"
        table = Table(displayName=f"{sheet_name.replace(' ', '')}Table", ref=table_ref)
        style = TableStyleInfo(
            name="TableStyleMedium9",
            showFirstColumn=False,
            showLastColumn=False,
            showRowStripes=True,
            showColumnStripes=False
        )
        table.tableStyleInfo = style
        ws.add_table(table)
    
    # (Optional) further formatting for 'Billable Hour Totals' can be added here
    
    wb.save(output_file)
    print(f"File saved: {output_file}")

# =========================
# SharePoint upload (unchanged behavior)
# =========================
def upload_to_sharepoint(file_path, file_name):
    TENANT_ID = os.getenv("SHAREPOINT_TENANT_ID")
    CLIENT_ID = os.getenv("SHAREPOINT_CLIENT_ID")
    CLIENT_SECRET = os.getenv("SHAREPOINT_CLIENT_SECRET")
    SITE_ID = os.getenv("SHAREPOINT_SITE_ID")
    DRIVE_ID = os.getenv("SHAREPOINT_DRIVE_ID")
    LIBRARY_PATH = os.getenv("SHAREPOINT_DOC_LIB").strip('"')

    authority = f"https://login.microsoftonline.com/{TENANT_ID}"
    scopes = ["https://graph.microsoft.com/.default"]
    app_conf = msal.ConfidentialClientApplication(
        CLIENT_ID, authority=authority, client_credential=CLIENT_SECRET
    )
    result = app_conf.acquire_token_for_client(scopes=scopes)
    if "access_token" not in result:
        raise Exception(f"Graph auth failed: {result.get('error_description')}")

    headers = {
        "Authorization": f"Bearer {result['access_token']}",
        "Content-Type": "application/json"
    }

    current_year = datetime.now().strftime("%Y")
    current_month = datetime.now().strftime("%m %B %Y")
    folder_path = f"{LIBRARY_PATH}/{current_year}/{current_month}"

    ensure_folder(folder_path, headers, SITE_ID, DRIVE_ID)

    encoded_path = quote(f"{folder_path}/{file_name}")
    upload_url = f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}/drives/{DRIVE_ID}/root:/{encoded_path}:/content"

    with open(file_path, "rb") as f:
        upload_response = requests.put(upload_url, headers={"Authorization": f"Bearer {result['access_token']}"}, data=f)

    if upload_response.status_code not in [200, 201]:
        raise Exception(f"Upload failed: {upload_response.status_code} - {upload_response.text}")

    print(f"✅ Uploaded {file_name} to SharePoint at {folder_path}/")

def ensure_folder(path, headers, site_id, drive_id):
    segments = path.strip("/").split("/")
    parent_path = ""
    for segment in segments:
        full_path = f"{parent_path}/{segment}" if parent_path else segment
        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root:/{full_path}"
        res = requests.get(url, headers=headers)
        if res.status_code == 404:
            if parent_path:
                create_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root:/{parent_path}:/children"
            else:
                create_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root/children"
            create_res = requests.post(create_url, headers=headers, json={
                "name": segment,
                "folder": {},
                "@microsoft.graph.conflictBehavior": "replace"
            })
            create_res.raise_for_status()
        parent_path = full_path

# =========================
# Orchestrator
# =========================
def fetch_and_process_data():
    # Billing windows (your adapted ranges)
    previous_cycle_start = "2025-07-16T00:00:00-08:00"
    previous_cycle_end   = "2025-07-29T23:59:59-08:00"

    mid_cycle_start = "2025-07-30T00:00:00-08:00"
    mid_cycle_end   = "2025-08-12T23:59:59-08:00"

    current_date = datetime.now()
    current_cycle_start = mid_cycle_start
    current_cycle_end   = current_date.strftime('%Y-%m-%dT23:59:59-08:00')

    # Fetch billable hour buckets (with robust backoff/paging)
    previous_cycle_data = fetch_billable_hours(previous_cycle_start, previous_cycle_end)
    mid_cycle_data = fetch_billable_hours(mid_cycle_start, mid_cycle_end)
    current_cycle_data = fetch_billable_hours(current_cycle_start, current_cycle_end)
    
    # Process main data
    matter_trusts_df, ocb_matter_df, ocb_client_bills_df, ocb_client_api_df, work_progress_df = process_data()
    
    # Merge/report for both cycles
    previous_cycle_df = merge_dataframes(
        matter_trusts_df, ocb_matter_df, ocb_client_bills_df, ocb_client_api_df, work_progress_df, previous_cycle_data,
        "07/16/25", "07/29/25"
    )
    mid_cycle_df = merge_dataframes(
        matter_trusts_df, ocb_matter_df, ocb_client_bills_df, ocb_client_api_df, work_progress_df, mid_cycle_data,
        "07/29/25", "08/12/25"
    )
    
    # Save the report
    current_date_str = datetime.now().strftime("%Y-%m-%d %I%p").lstrip('0').replace('.0', '.')
    output_file = f'TLR {current_date_str}.xlsx'
    
    # Apply formatting and save with all sheets
    apply_conditional_and_currency_formatting_with_totals(
        previous_cycle_df, 
        mid_cycle_df, 
        mid_cycle_data,
        current_cycle_data,
        "07/29/25",
        "08/12/25",
        current_date.strftime("%m/%d/%y"),
        output_file
    )

    print(f"\nUploading {output_file} to SharePoint...")
    upload_to_sharepoint(output_file, output_file)

    # Optional: delete the file after upload
    if os.path.exists(output_file):
        os.remove(output_file)
        print(f"{output_file} deleted from local storage.")

# =========================
# Entrypoint
# =========================
if __name__ == '__main__':
    try:
        access_token = get_access_token()
        if access_token:
            print("Access token obtained, starting data processing...")
            
            # Fetch custom fields
            custom_fields = fetch_custom_fields() or []

            # Define paralegal field mappings
            def find_field_id(name):
                for f in custom_fields:
                    if isinstance(f, dict) and f.get('name','').lower() == name.lower():
                        return f.get('id')
                return None

            paralegal_field_ids = {
                'Main Paralegal': find_field_id('Main Paralegal'),
                'Supporting Paralegal': find_field_id('Supporting Paralegal'),
                'Supporting Attorney': find_field_id('Supporting Attorney'),
                'CR ID': find_field_id('CR ID'),
                'Initial Client Goals': find_field_id('Initial Client Goals'),
                'Initial Strategy': find_field_id('Initial Strategy'),
                'Has strategy changed Describe': find_field_id('Has strategy changed Describe'),
                'Current action Items': find_field_id('Current action Items'),
                'Hearings': find_field_id('Hearings'),
                'Deadlines': find_field_id('Deadlines'),
                'DV situation description': find_field_id('DV situation description'),
                'Custody Visitation': find_field_id('Custody Visitation'),
                'CS Add ons Extracurricular': find_field_id('CS Add ons Extracurricular'),
                'Spousal Support': find_field_id('Spousal Support'),
                'PDDs': find_field_id('PDDs'),
                'Discovery': find_field_id('Discovery'),
                'Judgment Trial': find_field_id('Judgment Trial'),
                'Post Judgment': find_field_id('Post Judgment')
            }

            client_notes_id = find_field_id('Client Notes')

            # Map picklist options
            picklist_mappings = {
                field_name: {
                    option['id']: option['option']
                    for field in custom_fields if field.get('id') == field_id
                    for option in (field.get('picklist_options') or [])
                    if isinstance(option, dict)
                }
                for field_name, field_id in paralegal_field_ids.items() if field_id
            }

            fetch_and_process_data()
        else:
            print("OAuth flow not seeded. Complete the authorization locally to store tokens in env.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")
