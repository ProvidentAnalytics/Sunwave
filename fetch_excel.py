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
    tok = r.json()['access_token']
    # Decode JWT payload (no signature check, just for diagnostics)
    import base64, json as _json
    try:
        payload_b64 = tok.split('.')[1]
        payload_b64 += '=' * (-len(payload_b64) % 4)
        payload = _json.loads(base64.urlsafe_b64decode(payload_b64))
        roles = payload.get('roles', [])
        print(f"Token app id (appid): {payload.get('appid','?')}")
        print(f"Token tenant (tid):   {payload.get('tid','?')}")
        print(f"Token APPLICATION permissions (roles): {roles or '(none — permissions not granted!)'}")
        if not roles:
            print("⚠️  No 'roles' claim in token. The Azure AD app has no admin-consented")
            print("    Application permissions. Add Files.Read.All + Sites.Read.All under")
            print("    'Application permissions' (not 'Delegated') and click 'Grant admin consent'.")
    except Exception as e:
        print(f"Could not decode token for diagnostics: {e}")
    return tok

def find_file_in_folder(site_id, h, parent='root', depth=0, max_depth=4):
    """Recursively walk folders to find FILE_NAME. App-only safe (no /search)."""
    if depth > max_depth:
        return None
    url = f'https://graph.microsoft.com/v1.0/sites/{site_id}/drive/{parent}/children?$select=id,name,file,folder&$top=200'
    while url:
        r = requests.get(url, headers=h, timeout=30)
        r.raise_for_status()
        data = r.json()
        for item in data.get('value', []):
            if 'file' in item and item.get('name') == FILE_NAME:
                return item
        # Recurse into folders only after scanning current level
        for item in data.get('value', []):
            if 'folder' in item:
                found = find_file_in_folder(site_id, h, parent=f"items/{item['id']}",
                                            depth=depth+1, max_depth=max_depth)
                if found:
                    return found
        url = data.get('@odata.nextLink')
    return None

def download_workbook(token):
    h = {'Authorization': f'Bearer {token}'}
    site = requests.get(f'https://graph.microsoft.com/v1.0/sites/{HOST}:/{SITE_PATH}',
                        headers=h, timeout=30)
    site.raise_for_status()
    site_id = site.json()['id']
    print(f'Site id: {site_id}')

    # Try direct path lookup at root first (fast path).
    f = None
    direct = requests.get(
        f'https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:/{FILE_NAME}',
        headers=h, timeout=30,
    )
    if direct.status_code == 200:
        f = direct.json()
        print(f'Found via direct path: {f["id"]}')
    else:
        print(f'Direct lookup returned {direct.status_code}; walking folders...')
        f = find_file_in_folder(site_id, h)
        if not f:
            sys.exit(f'File not found anywhere in site drive: {FILE_NAME}')
        print(f'Found via walk: {f["id"]}')

    print(f"Downloading {FILE_NAME} (size={f.get('size','?')} bytes)...")
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
