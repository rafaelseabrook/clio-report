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
import time
import msal

app = Flask(__name__)

# Clio API credentials
CLIENT_ID = os.getenv("CLIO_CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIO_CLIENT_SECRET")
REDIRECT_URI = os.getenv("CLIO_REDIRECT_URI")

# OneDrive local folder path for Reports
ONEDRIVE_REPORTS_FOLDER_PATH = r'C:\Users\Rafael\OneDrive - Seabrook Law Offices\Desktop'

# Token storage file
TOKEN_FILE = 'clio_tokens.json'

API_VERSION = '4'

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

def get_access_token():
    tokens = load_tokens_env()
    if tokens:
        if datetime.now().timestamp() < tokens['expires_in']:
            return tokens['access_token']
        else:
            return refresh_access_token(tokens['refresh_token'])
    else:
        print("No tokens found. Run the script locally to authorize.")
        return None

def refresh_access_token(refresh_token):
    token_url = 'https://app.clio.com/oauth/token'
    response = requests.post(token_url, data={
        'grant_type': 'refresh_token',
        'refresh_token': refresh_token,
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET
    })
    if response.status_code == 200:
        tokens = response.json()
        tokens['expires_in'] = datetime.now().timestamp() + tokens['expires_in']
        save_tokens_env(tokens)
        return tokens['access_token']
    else:
        print(f"Failed to refresh access token: {response.status_code}, {response.text}")
        raise Exception('Failed to refresh access token.')

@app.route('/callback')
def callback():
    auth_code = request.args.get('code')
    token_url = 'https://app.clio.com/oauth/token'
    response = requests.post(token_url, data={
        'grant_type': 'authorization_code',
        'code': auth_code,
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'redirect_uri': REDIRECT_URI
    })
    if response.status_code == 200:
        tokens = response.json()
        tokens['expires_in'] = datetime.now().timestamp() + tokens['expires_in']
        save_tokens(tokens)
        fetch_and_process_data()
        return 'Authorization complete. Data processing initiated.'
    else:
        print(f"Authorization failed: {response.status_code}, {response.text}")
        return 'Authorization failed.'

def fetch_data(url, params):
    headers = {'Authorization': f'Bearer {get_access_token()}'}
    response = requests.get(url, headers=headers, params=params)
    print(f"Fetching data from {url} with params {params}. Status code: {response.status_code}")
    import re  # At top of your file

def fetch_data(url, params):
    headers = {'Authorization': f'Bearer {get_access_token()}'}
    seen_pages = set()
    page = params.get('page', 1)
    all_data = []

    while True:
        params['page'] = page
        response = requests.get(url, headers=headers, params=params)
        print(f"Fetching data from {url} with params {params}. Status code: {response.status_code}")

        if response.status_code == 200:
            data = response.json().get('data', [])
            page_id = tuple(sorted(item.get('id') for item in data if item.get('id') is not None))

            if page_id in seen_pages:
                print(f"Repeating page detected at page {page}. Breaking loop.")
                break
            seen_pages.add(page_id)

            all_data.extend(data)
            if len(data) < params.get('limit', 200):
                break
            page += 1

        elif response.status_code == 429:
            try:
                retry_msg = response.json().get("error", {}).get("message", "")
                match = re.search(r"Retry in (\\d+)", retry_msg)
                wait_time = int(match.group(1)) if match else 30
            except Exception:
                wait_time = 30
            print(f"Rate limit hit. Retrying in {wait_time} seconds...")
            time.sleep(wait_time)

        else:
            print(f"Failed to fetch data: {response.status_code}, {response.text}")
            break

    return all_data, len(all_data)

def fetch_matters_with_balances():
    url = 'https://app.clio.com/api/v4/matters'
    params = {
        'fields': 'id,number,description,client{name},responsible_attorney{name},status,matter_stage{name},account_balances{balance}',
        'status': 'open,pending'
    }
    return fetch_data(url, params)[0]

def fetch_outstanding_balances():
    url = 'https://app.clio.com/api/v4/outstanding_client_balances.json'
    params = {
        'fields': 'contact{name},total_outstanding_balance',
        'limit': 200
    }
    return fetch_data(url, params)[0]

def fetch_work_progress():
    all_matters = []
    seen_ids = set()
    page = 1
    while True:
        url = f'https://app.clio.com/api/v4/billable_matters.json'
        params = {
            'fields': 'unbilled_amount,unbilled_hours,client{name}',
            'limit': 200,
            'page': page
        }
        matters, _ = fetch_data(url, params)

        # Detect if we've seen all data (duplicate page)
        new_ids = {m.get('id') for m in matters if m.get('id') is not None}
        if not new_ids - seen_ids:
            print(f"Duplicate or repeating data at page {page}, breaking loop.")
            break

        seen_ids.update(new_ids)
        all_matters.extend(matters)
        print(f"Page {page}: Fetched {len(matters)} matters.")

        if len(matters) < 200:
            break
        page += 1

    return all_matters

def fetch_billable_hours(start_date, end_date):
    url = 'https://app.clio.com/api/v4/activities'
    params = {
        'start_date': start_date,
        'end_date': end_date,
        'status': 'billable',
        'order': 'date(desc)',
        'limit': 50,
        'fields': 'id,quantity,rounded_quantity,date,matter{id,display_number,number},user{name},type,note,total'
    }

    headers = {
        'Authorization': f'Bearer {get_access_token()}',
        'Accept': 'application/json'
    }

    matter_totals = {}
    offset = 0

    while True:
        params['offset'] = offset
        response = requests.get(url, headers=headers, params=params)
        time.sleep(0.5)  # Rate limiting
        
        if response.status_code != 200:
            print(f"API error: {response.status_code} - {response.text}")
            return {}

        data = response.json()
        if not data or 'data' not in data:
            break

        for entry in data['data']:
            if (entry['type'] == 'TimeEntry' and 
                'matter' in entry and entry['matter'] and 
                'rounded_quantity' in entry):
                
                matter = entry['matter']
                matter_number = matter.get('number')  # Get the actual matter number
                
                if not matter_number:
                    continue

                hours = float(entry['rounded_quantity']) / 3600  # Convert seconds to hours
                user_name = entry['user'].get('name', 'Unknown User')

                print(f"Debug: Processing time entry for matter {matter_number}, user {user_name}, hours {hours}")

                if matter_number not in matter_totals:
                    matter_totals[matter_number] = {
                        'total_hours': 0,
                        'user_hours': {}
                    }

                matter_totals[matter_number]['total_hours'] += hours
                if user_name not in matter_totals[matter_number]['user_hours']:
                    matter_totals[matter_number]['user_hours'][user_name] = 0
                matter_totals[matter_number]['user_hours'][user_name] += hours

        if len(data['data']) < params['limit']:
            break
        offset += params['limit']

    print(f"Debug: Total matters with billable hours: {len(matter_totals)}")
    print("Debug: Sample of matter_totals:")
    for matter_number, data in list(matter_totals.items())[:3]:
        print(f"Matter {matter_number}: {data}")

    return matter_totals

def fetch_custom_fields():
    """Fetch all custom fields and log their details."""
    url = f'https://app.clio.com/api/v{API_VERSION}/custom_fields.json'
    params = {'fields': 'id,name,field_type,picklist_options', 'limit': 200}
    custom_fields, _ = fetch_data(url, params)
    
    print("Custom Fields Retrieved:")

    # Iterate over each item in the custom_fields list
    for item in custom_fields:
        # Handle lists nested within the response
        if isinstance(item, list):
            for field in item:  # Iterate over the nested list
                process_custom_field(field)
        elif isinstance(item, dict):  # Process individual field dictionaries
            process_custom_field(item)
        else:
            print(f"Unexpected data format for custom field: {item}. Skipping...")

    return custom_fields

def process_custom_field(field):
    """Process and print custom field details if the format is correct."""
    field_id = field.get('id', 'N/A')
    field_name = field.get('name', 'N/A')
    field_type = field.get('field_type', 'N/A')

    print(f"ID: {field_id}, Name: {field_name}, Type: {field_type}")

    # Handle picklist options
    if field_type == 'picklist' and isinstance(field.get('picklist_options'), list):
        print("Picklist Options:")
        for option in field['picklist_options']:
            if isinstance(option, dict):  # Ensure options are valid dictionaries
                option_id = option.get('id', 'N/A')
                option_value = option.get('option', 'N/A')
                print(f"  Option ID: {option_id}, Value: {option_value}")
    else:
        print("  No picklist options available.")

def flatten_list(nested_list):
    for item in nested_list:
        if isinstance(item, list):
            yield from flatten_list(item)
        else:
            yield item

def fetch_open_matters_with_custom_fields(paralegal_field_ids, picklist_mappings, client_notes_id):
    url = f'https://app.clio.com/api/v{API_VERSION}/matters.json'
    params = {
        'fields': 'id,number,description,client{name},custom_field_values{id,field_name,field_type,value,picklist_option}',
        'status': 'open,pending',
        'limit': 200
    }
    matters, _ = fetch_data(url, params)

    processed_data = []
    
    # Define the fields we want to keep
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

    # Ensure matters is a list
    if isinstance(matters, dict):
        matters = [matters]
    matters = list(flatten_list(matters))

    for matter in matters:
        if not isinstance(matter, dict):
            continue

        matter_number = matter.get('number', matter.get('id'))
        if not matter_number:
            continue

        # Initialize matter_data with all fields we want
        matter_data = {
            'Matter Number': matter_number,
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

        # Process only the desired custom fields
        custom_fields = matter.get('custom_field_values', [])
        if isinstance(custom_fields, list):
            for field in custom_fields:
                if not isinstance(field, dict):
                    continue
                    
                field_name = field.get('field_name')
                if field_name not in desired_fields:
                    continue

                field_type = field.get('field_type')
                field_value = field.get('value')
                
                # Handle picklist fields
                if field_type == 'picklist' and field.get('picklist_option'):
                    picklist_value = field['picklist_option'].get('option', '')
                    matter_data[field_name] = picklist_value
                else:
                    # Handle non-picklist fields
                    matter_data[field_name] = field_value or ''

        processed_data.append(matter_data)

    columns = ['Matter Number', 'Main Paralegal', 'Supporting Paralegal', 
              'Supporting Attorney', 'Client Notes', 'CR ID',
              'Initial Client Goals', 'Initial Strategy', 'Has strategy changed Describe',
              'Current action Items', 'Hearings', 'Deadlines', 'DV situation description',
              'Custody Visitation', 'CS Add ons Extracurricular', 'Spousal Support',
              'PDDs', 'Discovery', 'Judgment Trial', 'Post Judgment']
    
    if not processed_data:
        return pd.DataFrame(columns=columns)
    
    df = pd.DataFrame(processed_data)
    
    # Ensure all required columns exist
    for col in columns:
        if col not in df.columns:
            df[col] = ''
            
    return df[columns]

def get_billing_cycle_totals(matter_number, billing_data):
    """Get total hours and user breakdown for the billing cycle"""
    if matter_number in billing_data:
        return {
            'total_hours': billing_data[matter_number]['total_hours'],
            'user_breakdown': billing_data[matter_number]['user_hours']
        }
    return {
        'total_hours': 0,
        'user_breakdown': {}
    }

def get_current_month_totals():
    """Get total hours for current month per user"""
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
    """Calculate total hours per user from billing data"""
    user_totals = {}
    for matter_data in billing_data.values():
        for user, hours in matter_data.get('user_hours', {}).items():
            user_totals[user] = user_totals.get(user, 0) + hours
    return user_totals

def normalize_name(name):
    if ',' in name:
        last, first = name.split(',', 1)
        return f"{first.strip()} {last.strip()}"
    else:
        return name.strip()

def process_data():
    print("Fetching matters with balances...")
    matters = fetch_matters_with_balances()
    print(f"Fetched {len(matters)} matters with balances.")

    print("Fetching outstanding balances...")
    outstanding_balances = fetch_outstanding_balances()
    print(f"Fetched {len(outstanding_balances)} outstanding balances.")

    print("Fetching work progress...")
    work_progress = fetch_work_progress()
    print(f"Fetched {len(work_progress)} work progress items.")

    matter_trusts_data = []
    for matter in matters:
        account_balances = matter.get('account_balances', [])
        total_balance_amount = sum(balance.get('balance', 0) for balance in account_balances)

        matter_stage = matter.get('matter_stage')
        matter_stage_name = matter_stage.get('name', 'N/A') if matter_stage else 'N/A'

        matter_trusts_data.append({
            'Matter Number': matter.get('number', 'N/A'),
            'Client Name': normalize_name(matter.get('client', {}).get('name', 'N/A')),
            'Trust Account Balance': total_balance_amount,
            'Responsible Attorney': matter.get('responsible_attorney', {}).get('name', 'N/A'),
            'Status': matter.get('status', 'N/A'),
            'Matter Stage': matter_stage_name
        })

    outstanding_balances_data = [
        {
            'Client Name': normalize_name(balance.get('contact', {}).get('name', 'N/A')),
            'Outstanding Balance': balance.get('total_outstanding_balance', 0)
        }
        for balance in outstanding_balances
    ]

    work_progress_data = [
        {
            'Client Name': normalize_name(matter.get('client', {}).get('name', 'N/A')),
            'Unbilled Amount': matter.get('unbilled_amount', 0),
            'Unbilled Hours': matter.get('unbilled_hours', 0)
        }
        for matter in work_progress
        if isinstance(matter, dict)
    ]

    matter_trusts_df = pd.DataFrame(matter_trusts_data)
    outstanding_balances_df = pd.DataFrame(outstanding_balances_data)

    if not work_progress_data:
        work_progress_df = pd.DataFrame(columns=['Client Name', 'Unbilled Amount', 'Unbilled Hours'])
    else:
        work_progress_df = pd.DataFrame(work_progress_data)

    return matter_trusts_df, outstanding_balances_df, work_progress_df


def merge_dataframes(matter_trusts_df, outstanding_balances_df, work_progress_df, billing_cycle_data, cycle_start_date=None, cycle_end_date=None):
    print("Merging dataframes...")

    # Ensure 'Client Name' exists in all relevant DataFrames
    for df_name, df in [('matter_trusts_df', matter_trusts_df), ('outstanding_balances_df', outstanding_balances_df), ('work_progress_df', work_progress_df)]:
        if 'Client Name' not in df.columns:
            print(f"Missing 'Client Name' in {df_name}. Columns are: {df.columns.tolist()}")
            df['Client Name'] = 'N/A'

    # Format date range for column header
    if cycle_start_date is None or cycle_end_date is None:
        billing_cycle_start = "07/11/25"  # Default previous cycle start
        billing_cycle_end = "07/12/25"    # Default previous cycle end
    else:
        billing_cycle_start = cycle_start_date
        billing_cycle_end = cycle_end_date

    billing_cycle_column = f"Billing Cycle Hours ({billing_cycle_start} - {billing_cycle_end})"

    # Columns used for calculation but not displayed
    calculation_columns = ['Trust Account Balance', 'Outstanding Balance', 'Unbilled Amount']

    # Columns to display in the final Excel (in order)
    display_columns_order = [
        'Matter Number', 'Client Name', 'CR ID', 'Net Trust Account Balance',
        'Status', 'Matter Stage', 'Responsible Attorney', 'Unbilled Hours',
        'Main Paralegal', 'Supporting Paralegal', 'Supporting Attorney', 'Client Notes'
    ]

    # Perform merges
    combined_df = pd.merge(matter_trusts_df, outstanding_balances_df, on='Client Name', how='left')
    combined_df = pd.merge(combined_df, work_progress_df, on='Client Name', how='left')

    try:
        # Fetch and merge custom fields
        custom_fields_df = fetch_open_matters_with_custom_fields(paralegal_field_ids, picklist_mappings, client_notes_id)
        combined_df = pd.merge(combined_df, custom_fields_df, on='Matter Number', how='left')
    except Exception as e:
        print(f"Error merging custom fields: {str(e)}")
        # Ensure custom field columns exist even if merge fails
        all_fields = ['Main Paralegal', 'Supporting Paralegal', 'Supporting Attorney', 'Client Notes', 'CR ID',
                      'Initial Client Goals', 'Initial Strategy', 'Has strategy changed Describe',
                      'Current action Items', 'Hearings', 'Deadlines', 'DV situation description',
                      'Custody Visitation', 'CS Add ons Extracurricular', 'Spousal Support',
                      'PDDs', 'Discovery', 'Judgment Trial', 'Post Judgment']
        for col in all_fields:
            if col not in combined_df.columns:
                combined_df[col] = ''

    # Fill NaN values
    numeric_columns = calculation_columns + ['Unbilled Hours']
    for col in numeric_columns:
        if col in combined_df.columns:
            combined_df[col] = combined_df[col].fillna(0)

    # Calculate Net Trust Account Balance
    combined_df['Net Trust Account Balance'] = (
        combined_df['Trust Account Balance'] - 
        combined_df['Outstanding Balance'] - 
        combined_df['Unbilled Amount']
    )

    # Add billing cycle hours with date range in column name
    combined_df[billing_cycle_column] = combined_df['Matter Number'].apply(
        lambda x: get_billing_cycle_totals(x, billing_cycle_data)['total_hours']
    )

    # Add user breakdown columns for billing cycle
    user_columns = set()
    for matter_data in billing_cycle_data.values():
        user_columns.update(matter_data.get('user_hours', {}).keys())

    for user in sorted(user_columns):
        combined_df[f'{user} Cycle Hours ({billing_cycle_start} - {billing_cycle_end})'] = combined_df['Matter Number'].apply(
            lambda x: get_billing_cycle_totals(x, billing_cycle_data)['user_breakdown'].get(user, 0)
        )

    # Ensure all display columns exist
    for col in display_columns_order:
        if col not in combined_df.columns:
            combined_df[col] = 0 if col in numeric_columns else ''

    # Get billing cycle columns
    billing_columns = [billing_cycle_column] + [col for col in combined_df.columns if 'Cycle Hours' in col and col != billing_cycle_column]

    # Additional fields at the end
    additional_fields = [
        'Initial Client Goals', 'Initial Strategy', 'Has strategy changed Describe',
        'Current action Items', 'Hearings', 'Deadlines', 'DV situation description',
        'Custody Visitation', 'CS Add ons Extracurricular', 'Spousal Support',
        'PDDs', 'Discovery', 'Judgment Trial', 'Post Judgment'
    ]

    final_columns = display_columns_order + billing_columns + additional_fields

    return combined_df[final_columns].sort_values(by='Net Trust Account Balance', ascending=False)
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
            # Get columns that contain 'Cycle Hours'
            time_columns = [col for col in df.columns if 'Cycle Hours' in col]
            
            # Calculate totals for time columns
            totals = df[time_columns].sum()
            
            # Create a totals row
            totals_row = pd.Series('', index=df.columns)
            totals_row['Matter Number'] = 'TOTALS'
            for col in time_columns:
                totals_row[col] = totals[col]
            
            # Append totals row to DataFrame
            df_with_totals = pd.concat([df, pd.DataFrame([totals_row])], ignore_index=True)
            
            # Save to Excel
            if df is previous_cycle_df:
                df_with_totals.to_excel(writer, sheet_name='Previous Billing Cycle', index=False)
            else:
                df_with_totals.to_excel(writer, sheet_name='Mid Cycle', index=False)
        
        # Create totals DataFrames for third sheet
        mid_cycle_totals = get_user_totals(mid_cycle_data)
        current_cycle_totals = get_user_totals(current_cycle_data)
        
        # Convert to DataFrames and sort by hours
        mid_totals_df = pd.DataFrame([
            {'User': user, f'Cycle Hours ({mid_cycle_start_formatted} - {mid_cycle_end_formatted})': hours}
            for user, hours in sorted(mid_cycle_totals.items(), key=lambda x: x[1], reverse=True)
        ])
        
        current_totals_df = pd.DataFrame([
            {'User': user, f'Cycle Running Total ({mid_cycle_start_formatted} - {current_date_formatted})': hours}
            for user, hours in sorted(current_cycle_totals.items(), key=lambda x: x[1], reverse=True)
        ])
        
        # Write totals to third sheet
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
        
        # Get the last row number
        last_row = ws.max_row
        
        # Find the Net Trust Account Balance column
        net_balance_col = None
        time_cols = []  # To store columns containing time data
        
        for col_idx, cell in enumerate(ws[1], 1):
            if cell.value == 'Net Trust Account Balance':
                net_balance_col = col_idx
            if 'Cycle Hours' in str(cell.value):
                time_cols.append(col_idx)
        
        # Apply formatting to all rows except the last (totals) row
        for row in ws.iter_rows(min_row=2, max_row=last_row-1):
            # Apply currency formatting
            for col_idx, cell in enumerate(row, 1):
                if ws[1][col_idx - 1].value in ['Trust Account Balance', 'Outstanding Balance', 'Unbilled Amount', 'Net Trust Account Balance']:
                    cell.style = currency_style
            
            # Apply conditional formatting to Net Trust Account Balance
            if net_balance_col:
                net_balance_cell = row[net_balance_col - 1]
                try:
                    cell_value = float(net_balance_cell.value or 0)
                    if cell_value <= 0:
                        net_balance_cell.fill = red_fill
                    elif 0 < cell_value < 1000:
                        net_balance_cell.fill = yellow_fill
                    else:
                        net_balance_cell.fill = green_fill
                except ValueError:
                    pass
        
        # Format totals row
        totals_row = ws[last_row]
        for cell in totals_row:
            cell.font = bold_font
            cell.fill = total_row_fill
            
            # Format time columns in totals row
            col_idx = cell.column
            if col_idx in time_cols:
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
    
    # Format totals sheet (remaining code unchanged)
    totals_sheet = wb['Billable Hour Totals']
    # ... (keep existing totals sheet formatting)
    
    wb.save(output_file)
    print(f"File saved: {output_file}")

def upload_to_sharepoint(file_path, file_name):
    TENANT_ID = os.getenv("SHAREPOINT_TENANT_ID")
    CLIENT_ID = os.getenv("SHAREPOINT_CLIENT_ID")
    CLIENT_SECRET = os.getenv("SHAREPOINT_CLIENT_SECRET")
    SITE_ID = os.getenv("SHAREPOINT_SITE_ID")
    DRIVE_ID = os.getenv("SHAREPOINT_DRIVE_ID")
    LIBRARY_PATH = os.getenv("SHAREPOINT_DOC_LIB")

    authority = f"https://login.microsoftonline.com/{TENANT_ID}"
    scopes = ["https://graph.microsoft.com/.default"]
    app = msal.ConfidentialClientApplication(
        CLIENT_ID, authority=authority,
        client_credential=CLIENT_SECRET
    )
    result = app.acquire_token_for_client(scopes=scopes)
    if "access_token" not in result:
        raise Exception(f"Graph auth failed: {result.get('error_description')}")

    headers = {
        "Authorization": f"Bearer {result['access_token']}",
        "Content-Type": "application/json"
    }

    current_month = datetime.now().strftime("%m %B %Y")
    base_url = f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}/drives/{DRIVE_ID}/root:/General Management/Traffic Light Reports/{current_month}"

    # Ensure the folder exists or create it
    folder_check = requests.get(base_url, headers=headers)
    if folder_check.status_code == 404:
        create_url = f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}/drives/{DRIVE_ID}/root:/General Management/Traffic Light Reports/{current_month}:/children"
        requests.post(create_url, headers=headers, json={"name": current_month, "folder": {}, "@microsoft.graph.conflictBehavior": "replace"})

    upload_url = f"{base_url}/{file_name}:/content"
    with open(file_path, "rb") as f:
        upload_response = requests.put(upload_url, headers={"Authorization": f"Bearer {result['access_token']}"}, data=f)
    if upload_response.status_code not in [200, 201]:
        raise Exception(f"Upload failed: {upload_response.status_code} - {upload_response.text}")
    print(f"Uploaded {file_name} to SharePoint.")
    
def fetch_and_process_data():
    # Previous billing cycle dates
    previous_cycle_start = "2025-07-11T00:00:00-08:00"
    previous_cycle_end = "2025-7-12T23:59:59-08:00"

    # Mid cycle dates
    mid_cycle_start = "2025-07-11T00:00:00-08:00"
    mid_cycle_end = "2025-07-12T23:59:59-08:00"

    # Current cycle to date (starting from mid cycle start)
    current_cycle_start = mid_cycle_start  # Same as mid cycle start
    current_date = datetime.now()
    current_cycle_end = current_date.strftime('%Y-%m-%dT23:59:59-08:00')

    # Fetch data for all periods
    previous_cycle_data = fetch_billable_hours(previous_cycle_start, previous_cycle_end)
    mid_cycle_data = fetch_billable_hours(mid_cycle_start, mid_cycle_end)
    current_cycle_data = fetch_billable_hours(current_cycle_start, current_cycle_end)
    
    # Process main data
    matter_trusts_df, outstanding_balances_df, work_progress_df = process_data()
    
    # Create reports for both cycles
    previous_cycle_df = merge_dataframes(matter_trusts_df, outstanding_balances_df, 
                                       work_progress_df, previous_cycle_data,
                                       "07/11/25", "07/12/25")
    
    mid_cycle_df = merge_dataframes(matter_trusts_df, outstanding_balances_df, 
                                  work_progress_df, mid_cycle_data,
                                  "07/11/25", "07/12/25")
    
    # Save the report
    current_date_str = datetime.now().strftime("%Y-%m-%d %I%p").lstrip('0').replace('.0', '.')
    output_file = f'TLR {current_date_str}.xlsx'
    
    # Apply formatting and save with all sheets
    apply_conditional_and_currency_formatting_with_totals(
        previous_cycle_df, 
        mid_cycle_df, 
        mid_cycle_data,
        current_cycle_data,
        "07/11/25",  # mid_cycle_start_formatted
        "07/12/25",  # mid_cycle_end_formatted
        current_date.strftime("%m/%d/%y"),  # current_date_formatted
        output_file
    )
    
    # Move to OneDrive folder
    final_path = os.path.join(ONEDRIVE_REPORTS_FOLDER_PATH, output_file)
    os.rename(output_file, final_path)
    print(f"\nFile has been saved to: {final_path}")
    upload_to_sharepoint(final_path, output_file)

if __name__ == '__main__':
    try:
        access_token = get_access_token()
        if access_token:
            print("Access token obtained, starting data processing...")
            
            # Fetch custom fields before proceeding
            custom_fields = fetch_custom_fields()

            # Define paralegal field mappings
            paralegal_field_ids = {
                'Main Paralegal': next((field['id'] for field in custom_fields if field['name'].lower() == 'main paralegal'), None),
                'Supporting Paralegal': next((field['id'] for field in custom_fields if field['name'].lower() == 'supporting paralegal'), None),
                'Supporting Attorney': next((field['id'] for field in custom_fields if field['name'].lower() == 'supporting attorney'), None),
                'CR ID': next((field['id'] for field in custom_fields if field['name'].lower() == 'cr id'), None),
                'Initial Client Goals': next((field['id'] for field in custom_fields if field['name'].lower() == 'initial client goals'), None),
                'Initial Strategy': next((field['id'] for field in custom_fields if field['name'].lower() == 'initial strategy'), None),
                'Has strategy changed Describe': next((field['id'] for field in custom_fields if field['name'].lower() == 'has strategy changed describe'), None),
                'Current action Items': next((field['id'] for field in custom_fields if field['name'].lower() == 'current action items'), None),
                'Hearings': next((field['id'] for field in custom_fields if field['name'].lower() == 'hearings'), None),
                'Deadlines': next((field['id'] for field in custom_fields if field['name'].lower() == 'deadlines'), None),
                'DV situation description': next((field['id'] for field in custom_fields if field['name'].lower() == 'dv situation description'), None),
                'Custody Visitation': next((field['id'] for field in custom_fields if field['name'].lower() == 'custody visitation'), None),
                'CS Add ons Extracurricular': next((field['id'] for field in custom_fields if field['name'].lower() == 'cs add ons extracurricular'), None),
                'Spousal Support': next((field['id'] for field in custom_fields if field['name'].lower() == 'spousal support'), None),
                'PDDs': next((field['id'] for field in custom_fields if field['name'].lower() == 'pdds'), None),
                'Discovery': next((field['id'] for field in custom_fields if field['name'].lower() == 'discovery'), None),
                'Judgment Trial': next((field['id'] for field in custom_fields if field['name'].lower() == 'judgment trial'), None),
                'Post Judgment': next((field['id'] for field in custom_fields if field['name'].lower() == 'post judgment'), None)
            }

            client_notes_id = next((field['id'] for field in custom_fields if field['name'].lower() == 'client notes'), None)

            # Map picklist options
            picklist_mappings = {
                field_name: {
                    option['id']: option['option']
                    for field in custom_fields if field['id'] == field_id
                    for option in field.get('picklist_options', [])
                }
                for field_name, field_id in paralegal_field_ids.items() if field_id
            }

            # Now call fetch_and_process_data() after variables are set
            fetch_and_process_data()
        else:
            print("OAuth flow initiated, complete the authorization process in the browser.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")
