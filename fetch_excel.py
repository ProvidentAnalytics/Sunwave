"""Download MASTER_Sunwave_New_PowerQuerry.xlsx from SharePoint via Microsoft
Graph API using client-credentials flow (no user sign-in). Then generate
report_data.json so build_combined.py can run.

Env vars required:
  AZURE_CLIENT_ID       - Azure AD app client ID
  AZURE_CLIENT_SECRET   - Azure AD app client secret
  AZURE_TENANT_ID       - Tenant (directory) ID

The Azure AD app must have the following Application permissions on Microsoft
Graph (admin-consented): Files.Read.All, Sites.Read.All.
"""
import os, json, sys
import requests
import pandas as pd

HOST      = 'gshealthcarellc.sharepoint.com'
SITE_PATH = 'sites/Provident'
FILE_NAME = 'MASTER_Sunwave_New_PowerQuerry.xlsx'

SKIP_SHEETS = {'Table of Contents', 'Realms', 'Bedboard', 'Payment Summary'}

def get_token():
    cid = os.environ['AZURE_CLIENT_ID']
    sec = os.environ['AZURE_CLIENT_SECRET']
    tid = os.environ['AZURE_TENANT_ID']
    r = requests.post(
        f'https://login.microsoftonline.com/{tid}/oauth2/v2.0/token',
        data={'client_id': cid, 'client_secret': sec,
              'grant_type': 'client_credentials',
              'scope': 'https://graph.microsoft.com/.default'},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()['access_token']

def download_workbook(token):
    h = {'Authorization': f'Bearer {token}'}
    site = requests.get(f'https://graph.microsoft.com/v1.0/sites/{HOST}:/{SITE_PATH}',
                        headers=h, timeout=30)
    site.raise_for_status()
    site_id = site.json()['id']

    search = requests.get(
        f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root/search(q='{FILE_NAME}')",
        headers=h, timeout=30,
    )
    search.raise_for_status()
    files = search.json().get('value', [])
    f = next((x for x in files if x['name'] == FILE_NAME), files[0] if files else None)
    if not f:
        sys.exit(f'File not found in SharePoint: {FILE_NAME}')

    print(f"Downloading {FILE_NAME} (id={f['id']}, size={f.get('size','?')} bytes)...")
    dl = requests.get(
        f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/items/{f['id']}/content",
        headers=h, stream=True, timeout=300,
    )
    dl.raise_for_status()
    with open(FILE_NAME, 'wb') as out:
        for chunk in dl.iter_content(64*1024):
            out.write(chunk)
    print(f"Saved {FILE_NAME}: {os.path.getsize(FILE_NAME)/1024/1024:.1f} MB")

def build_report_data():
    """Generate report_data.json the same shape build_combined.py expects."""
    xl = pd.ExcelFile(FILE_NAME)
    out = {}
    for sheet in xl.sheet_names:
        if sheet in SKIP_SHEETS:
            continue
        df = pd.read_excel(xl, sheet_name=sheet)
        # Convert datetime columns to mm/dd/yyyy strings
        for c in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[c]):
                df[c] = df[c].dt.strftime('%m/%d/%Y').fillna('')
        df = df.where(pd.notna(df), '')
        out[sheet] = {
            'columns': [str(c) for c in df.columns],
            'rows': df.astype(str).values.tolist(),
        }
    with open('report_data.json', 'w', encoding='utf-8') as f:
        json.dump(out, f, ensure_ascii=False, default=str)
    print(f"Wrote report_data.json: {os.path.getsize('report_data.json')/1024/1024:.1f} MB")

if __name__ == '__main__':
    token = get_token()
    download_workbook(token)
    build_report_data()
    print('Done.')
