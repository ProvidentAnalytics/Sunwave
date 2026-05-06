"""build_crm.py — generate Sunwave CRM Dashboard at crm/index.html.

v3 batch build (May 6, 2026) — combined pipeline:
  - Rahul's xlsx-direct loading (preserved unchanged)
  - v3 additions: EXCLUDED test-record filter, sentinel admit-date sanitization,
    TTD/TSA outcome-date resolution with proxy fallback, DAILY aggregation blob

Reads MASTER_Sunwave_New_PowerQuerry.xlsx directly via pandas.

Inputs:
  - MASTER_Sunwave_New_PowerQuerry.xlsx  (downloaded by fetch_excel.py first)
  - dashboard_template_crm.html

Output:
  - crm/index.html
"""
import json
import os
import math
import pandas as pd
from datetime import datetime, timezone

XLSX = 'MASTER_Sunwave_New_PowerQuerry.xlsx'

# ── v3 config ───────────────────────────────────────────────────────────────
EXCLUDED_NAME_PATTERNS = [
    'jane sop training', 'jane doe',
    'test mike test tyson', 'test test',
    'jacob test', 'jason test', 'nat test',
    'julian', 'training', 'george clooney',
]
EXCLUDED_EMAIL_PATTERNS = [
    '@sunwavetestingtester', 'sunwavetesting',
    'jacob.machinia@sunwavehealth',
]

# When Sunwave starts exporting these as columns, set the values to the
# real column names. Pipeline auto-promotes from proxy to real dates.
TTD_DATE_FIELDS = {
    'lost':      None,
    'abandoned': None,
}

SENTINEL_DATE_PREFIXES = ('1899', '1900', '01/01/1900', '12/31/1899')


if not os.path.exists(XLSX):
    raise SystemExit(f'Source workbook not found: {XLSX}\n'
                     'Run fetch_excel.py first (requires AZURE_* env vars).')

print(f'[CRM] Loading {XLSX}...')
xl = pd.ExcelFile(XLSX)
print(f'[CRM] Sheets in workbook: {xl.sheet_names}')


def safe_str(v):
    if v is None:
        return ''
    if isinstance(v, float) and math.isnan(v):
        return ''
    s = str(v).strip()
    return '' if s.lower() in ('nan', 'nat', 'none') else s


def fmt_dt(v):
    """Format a pandas datetime/Timestamp into ISO-like string. Returns ''."""
    if v is None or pd.isna(v):
        return ''
    if hasattr(v, 'isoformat'):
        try:
            return v.isoformat()
        except Exception:
            pass
    return safe_str(v)


def parse_date_loose(s):
    """Parse various date string formats. Returns datetime or None.
    Rejects sentinel dates (1899/1900-prefixed)."""
    s = safe_str(s)
    if not s:
        return None
    for sp in SENTINEL_DATE_PREFIXES:
        if s.startswith(sp):
            return None
    if 'T' in s:
        try:
            return datetime.fromisoformat(s.replace('Z', '').split('.')[0][:19])
        except (ValueError, TypeError):
            pass
    for fmt in ('%Y-%m-%d', '%m/%d/%Y', '%m/%d/%Y %I:%M:%S %p',
                '%Y-%m-%dT%H:%M:%S', '%m/%d/%y'):
        try:
            return datetime.strptime(s.split('.')[0][:19] if 'T' in s else s, fmt)
        except (ValueError, TypeError):
            continue
    return None


def clean_date_str(v):
    """Normalize a date value to ISO string. Returns '' if unparseable or sentinel."""
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return ''
    s = fmt_dt(v) if not isinstance(v, str) else safe_str(v)
    if not s:
        return ''
    return s if parse_date_loose(s) else ''


def is_test_record(name, email):
    n = (name or '').lower().strip()
    e = (email or '').lower().strip()
    for p in EXCLUDED_NAME_PATTERNS:
        if p in n:
            return True
    for p in EXCLUDED_EMAIL_PATTERNS:
        if p in e:
            return True
    return False


def pick_sheet(*names):
    """Return DataFrame for the first sheet that exists in the workbook."""
    for n in names:
        if n in xl.sheet_names:
            print(f'  Using sheet: {n!r}')
            return pd.read_excel(xl, sheet_name=n)
    print(f'  Warning: none of {names} in workbook')
    return pd.DataFrame()


def coerce_date_col(series):
    """Robust date coercion that handles three storage formats:
      1. Already-datetime cells (pd.Timestamp)         → keep
      2. Excel serial numbers stored as int/float/str  → convert via Excel epoch
      3. Date strings ('05/02/2026', '2026-05-02', etc.) → parse normally

    Different sheets / cells in the same Sunwave xlsx use different formats —
    notably admission_date in 'Opportunities' arrives as Excel serial numbers,
    while created_on arrives as date strings. Without this helper, naive
    pd.to_datetime() interprets serial numbers as nanoseconds-since-epoch
    and produces 1970-01-01 results.
    """
    EXCEL_EPOCH = pd.Timestamp('1899-12-30')

    def conv(v):
        if v is None:
            return pd.NaT
        if isinstance(v, float) and math.isnan(v):
            return pd.NaT
        if isinstance(v, pd.Timestamp):
            return v
        if hasattr(v, 'isoformat'):  # datetime.datetime
            return pd.Timestamp(v)
        if isinstance(v, (int, float)):
            f = float(v)
            if 1 <= f <= 100000:  # plausible Excel serial range (1900..2173)
                return EXCEL_EPOCH + pd.Timedelta(days=f)
            return pd.NaT
        s = str(v).strip()
        if not s or s.lower() in ('nan', 'nat', 'none'):
            return pd.NaT
        # Try as Excel serial first (handles '46144' / '46144.0' string forms)
        try:
            f = float(s)
            if 1 <= f <= 100000:
                return EXCEL_EPOCH + pd.Timedelta(days=f)
        except (ValueError, TypeError):
            pass
        # Fall back to standard date-string parsing
        return pd.to_datetime(s, errors='coerce')

    return series.apply(conv)


# ── Process Opportunities ───────────────────────────────────────────────────
print('[CRM] Loading Opportunity sheet...')
odf = pick_sheet('Opportunity', 'Opportunities', 'Opportunities by Created Date',
                 'Opportunities Active')
for c in ('created_on', 'admission_date', 'lost_date', 'abandoned_date'):
    if c in odf.columns:
        odf[c] = coerce_date_col(odf[c])

opps = []
excluded_count = 0
excluded_oids = set()
for _, r in odf.iterrows():
    oid = safe_str(r.get('opportunity_id') or r.get('opportunity_legacy_id') or r.get('id'))
    if not oid:
        continue

    name = safe_str(r.get('patient name') or r.get('Patient Name') or '')
    email = safe_str(r.get('caller_email') or r.get('email') or '')
    if is_test_record(name, email):
        excluded_count += 1
        excluded_oids.add(oid)
        continue

    co = r.get('created_on')
    ad = r.get('admission_date')
    outcome = safe_str(r.get('outcome'))

    co_str = fmt_dt(co)
    ad_raw = fmt_dt(ad)

    # Sentinel sanitization: only emit admit when outcome is admitted AND date
    # parses (non-sentinel) AND admit >= created.
    co_d = parse_date_loose(co_str)
    ad_d = parse_date_loose(ad_raw)
    ad_clean = ''
    if outcome in ('Admitted', 'AdmittedLegacy') and ad_d:
        if not co_d or ad_d >= co_d:
            ad_clean = ad_raw

    lost_date_col = TTD_DATE_FIELDS.get('lost')
    aband_date_col = TTD_DATE_FIELDS.get('abandoned')
    lost_date_val = clean_date_str(r.get(lost_date_col)) if lost_date_col else ''
    aband_date_val = clean_date_str(r.get(aband_date_col)) if aband_date_col else ''

    opps.append({
        'id':      oid,
        'name':    name,
        'co':      co_str,
        'admit':   ad_clean,
        'outcome': outcome,
        'stage':   safe_str(r.get('stage')),
        'loc':     safe_str(r.get('level_of_care')),
        'ins':     safe_str(r.get('insurance provider')),
        'ref':     safe_str(r.get('referral name')),
        'lost_r':  safe_str(r.get('lost reason')),
        'aband_r': safe_str(r.get('abandoned reason')),
        'rep':     safe_str(r.get('adm. representative')),
        'lost_date':      lost_date_val,
        'abandoned_date': aband_date_val,
        # Filled in by resolve_outcome_dates() below:
        'outcome_date':    '',
        'outcome_proxy':   False,
        'scheduled_date':  '',
        'scheduled_proxy': False,
    })
print(f'  Opps: {len(opps)} ({excluded_count} test records excluded)')


# ── Process Timeline (activities) ──────────────────────────────────────────
print('[CRM] Loading Timeline sheet...')
tdf = pick_sheet('Timeline', 'Activities', 'Activity')
for c in ('activity_date', 'task_due_date', 'created_on', 'reminder_date_time'):
    if c in tdf.columns:
        tdf[c] = coerce_date_col(tdf[c])

acts = []
for _, r in tdf.iterrows():
    oid = safe_str(r.get('opportunity_id')) or safe_str(r.get('associated_with_id'))
    if oid and oid in excluded_oids:
        continue
    typ = safe_str(r.get('type'))
    if not (oid or typ):
        continue
    acts.append({
        'oid':   oid,
        'aid':   safe_str(r.get('id')),
        'type':  typ,
        'subj':  safe_str(r.get('task_subject'))[:140],
        'text':  safe_str(r.get('text'))[:400],
        'by':    safe_str(r.get('created_by_name')),
        'wf':    safe_str(r.get('workflow_status')),
        'date':  fmt_dt(r.get('activity_date')),
        'assoc': safe_str(r.get('associated_with')),
        'task_type':         safe_str(r.get('task_type')),
        'task_status':       safe_str(r.get('task_status')),
        'task_due_date':     fmt_dt(r.get('task_due_date')),
        'reminder_date_time':fmt_dt(r.get('reminder_date_time')),
        'assigned_to_name':  safe_str(r.get('assigned_to_name')),
    })
print(f'  Acts: {len(acts)} rows')


# ── Derive Users from Timeline ─────────────────────────────────────────────
print('[CRM] Deriving Users from Timeline...')
user_set = {}
for a in acts:
    for nm in (a.get('by'), a.get('assigned_to_name')):
        nm = safe_str(nm)
        if not nm:
            continue
        key = nm.lower()
        if key not in user_set:
            user_set[key] = {'id': '', 'name': nm, 'email': '', 'role': '', 'count': 0}
        user_set[key]['count'] += 1
users = sorted(user_set.values(), key=lambda u: -u['count'])
print(f'  Users: {len(users)}')


# ── Derive open CRM Tasks from Timeline ────────────────────────────────────
print('[CRM] Deriving Tasks from Timeline rows where type=Task...')
tasks = []
for _, r in tdf.iterrows():
    if safe_str(r.get('type')) != 'Task':
        continue
    oid = safe_str(r.get('opportunity_id')) or safe_str(r.get('associated_with_id'))
    if oid and oid in excluded_oids:
        continue
    status = safe_str(r.get('task_status')).lower()
    is_open = status in ('open', 'pending', '') and status not in ('completed', 'closed', 'cancelled')
    tasks.append({
        'id':         safe_str(r.get('id')),
        'aid':        safe_str(r.get('associated_with_id') or r.get('opportunity_id')),
        'assoc':      safe_str(r.get('associated_with')),
        'subject':    safe_str(r.get('task_subject')),
        'task_type':  safe_str(r.get('task_type')),
        'status':     safe_str(r.get('task_status')),
        'created_by': safe_str(r.get('created_by_name')),
        'assigned':   safe_str(r.get('assigned_to_name')),
        'text':       safe_str(r.get('text'))[:400],
        'due':        fmt_dt(r.get('task_due_date')),
        'created':    fmt_dt(r.get('created_on')),
        'reminder':   fmt_dt(r.get('reminder_date_time')),
        'is_open':    is_open,
    })
print(f'  Tasks: {len(tasks)} (open={sum(1 for t in tasks if t["is_open"])})')


# ── v3: Resolve TTD/TSA outcome dates (real or proxy) ──────────────────────
def resolve_outcome_dates(opps, acts):
    """Mutates opps: adds outcome_date / outcome_proxy / scheduled_date / scheduled_proxy."""
    by_oid = {}
    for a in acts:
        if not a['oid']:
            continue
        if a['type'] in ('Workflow', 'Wave'):
            continue
        d = parse_date_loose(a['date'])
        if not d:
            continue
        by_oid.setdefault(a['oid'], []).append(d)
    for oid in by_oid:
        by_oid[oid].sort()

    sched_by_oid = {}
    for a in acts:
        if not a['oid']:
            continue
        if a['type'] in ('Workflow', 'Wave'):
            continue
        if 'scheduled' not in (a.get('wf', '') or '').lower():
            continue
        d = parse_date_loose(a['date'])
        if not d:
            continue
        existing = sched_by_oid.get(a['oid'])
        if existing is None or d < existing:
            sched_by_oid[a['oid']] = d

    diag = {
        'tta_admits': 0,
        'ttd_lost_real': 0, 'ttd_lost_proxy': 0,
        'ttd_aband_real': 0, 'ttd_aband_proxy': 0,
        'tsa_real': 0, 'tsa_proxy': 0,
    }

    for o in opps:
        if o['outcome'] in ('Admitted', 'AdmittedLegacy'):
            if o['admit']:
                o['outcome_date'] = o['admit']
                diag['tta_admits'] += 1
        elif o['outcome'] == 'Lost':
            if o['lost_date']:
                o['outcome_date'] = o['lost_date']
                diag['ttd_lost_real'] += 1
            elif o['id'] in by_oid:
                o['outcome_date'] = by_oid[o['id']][-1].isoformat()
                o['outcome_proxy'] = True
                diag['ttd_lost_proxy'] += 1
        elif o['outcome'] == 'Abandoned':
            if o['abandoned_date']:
                o['outcome_date'] = o['abandoned_date']
                diag['ttd_aband_real'] += 1
            elif o['id'] in by_oid:
                o['outcome_date'] = by_oid[o['id']][-1].isoformat()
                o['outcome_proxy'] = True
                diag['ttd_aband_proxy'] += 1

        if o['outcome'] in ('Admitted', 'AdmittedLegacy') and o['admit']:
            sched_d = sched_by_oid.get(o['id'])
            if sched_d:
                o['scheduled_date'] = sched_d.isoformat()
                diag['tsa_real'] += 1
            elif o['id'] in by_oid and len(by_oid[o['id']]) > 0:
                o['scheduled_date'] = by_oid[o['id']][0].isoformat()
                o['scheduled_proxy'] = True
                diag['tsa_proxy'] += 1

    return diag


print('[CRM] Resolving outcome dates (TTA / TTD-Lost / TTD-Aband / TSA)...')
diag = resolve_outcome_dates(opps, acts)
print(f'  TTA admits:     {diag["tta_admits"]}')
print(f'  TTD-Lost:       {diag["ttd_lost_real"]} real / {diag["ttd_lost_proxy"]} proxy')
print(f'  TTD-Abandoned:  {diag["ttd_aband_real"]} real / {diag["ttd_aband_proxy"]} proxy')
print(f'  TSA:            {diag["tsa_real"]} real / {diag["tsa_proxy"]} proxy')


# ── v3: Build DAILY aggregation blob ───────────────────────────────────────
def build_daily(opps, acts, tasks):
    daily = {}

    def bucket(date_str):
        d = parse_date_loose(date_str)
        return d.strftime('%Y-%m-%d') if d else None

    def b_get(key):
        return daily.setdefault(key, {
            'opps_created': 0, 'opps_admitted': 0, 'opps_lost': 0, 'opps_abandoned': 0,
            'acts_total': 0, 'acts_human': 0, 'acts_by_type': {},
            'tasks_opened': 0, 'tasks_closed': 0,
        })

    for o in opps:
        kc = bucket(o['co'])
        if kc:
            b = b_get(kc)
            b['opps_created'] += 1
            if o['outcome'] == 'Lost':
                b['opps_lost'] += 1
            elif o['outcome'] == 'Abandoned':
                b['opps_abandoned'] += 1
        if o['outcome'] in ('Admitted', 'AdmittedLegacy'):
            ka = bucket(o['admit']) or bucket(o['co'])
            if ka:
                b_get(ka)['opps_admitted'] += 1

    for a in acts:
        k = bucket(a['date'])
        if not k:
            continue
        b = b_get(k)
        b['acts_total'] += 1
        if a['type'] not in ('Workflow', 'Wave'):
            b['acts_human'] += 1
        b['acts_by_type'][a['type']] = b['acts_by_type'].get(a['type'], 0) + 1

    for t in tasks:
        kc = bucket(t['created'])
        if kc:
            b_get(kc)['tasks_opened'] += 1
        if not t['is_open']:
            kd = bucket(t['due']) or kc
            if kd:
                b_get(kd)['tasks_closed'] += 1

    return daily


print('[CRM] Building DAILY aggregation blob...')
daily = build_daily(opps, acts, tasks)
print(f'  Daily rows: {len(daily)}')


# ── Meta ────────────────────────────────────────────────────────────────────
meta = {
    'refreshed_at': datetime.now(timezone.utc).isoformat(),
    'opps_count':   len(opps),
    'acts_count':   len(acts),
    'users_count':  len(users),
    'tasks_count':  len(tasks),
    'open_tasks':   sum(1 for t in tasks if t['is_open']),
}


# ── Build HTML ──────────────────────────────────────────────────────────────
template_path = 'dashboard_template_crm.html'
if not os.path.exists(template_path):
    raise SystemExit(f'Template not found: {template_path}')

with open(template_path, 'r', encoding='utf-8') as f:
    html = f.read()

sep = (',', ':')
replacements = {
    '/*INJECT_OPPS*/null':  json.dumps(opps,  separators=sep, ensure_ascii=True),
    '/*INJECT_ACTS*/null':  json.dumps(acts,  separators=sep, ensure_ascii=True),
    '/*INJECT_USERS*/null': json.dumps(users, separators=sep, ensure_ascii=True),
    '/*INJECT_TASKS*/null': json.dumps(tasks, separators=sep, ensure_ascii=True),
    '/*INJECT_DAILY*/null': json.dumps(daily, separators=sep, ensure_ascii=True),
    '/*INJECT_META*/null':  json.dumps(meta,  separators=sep, ensure_ascii=True),
}
for placeholder, value in replacements.items():
    if placeholder not in html:
        print(f'Warning: placeholder {placeholder} not found in template')
    html = html.replace(placeholder, value)

errors = []
if '/*INJECT_' in html:
    errors.append('Some /*INJECT_*/ placeholders remained unfilled')

os.makedirs('crm', exist_ok=True)
out_path = os.path.join('crm', 'index.html')
with open(out_path, 'w', encoding='utf-8') as f:
    f.write(html)

print(f'\nWrote {out_path}: {os.path.getsize(out_path)//1024} KB')
print(f'  opps={len(opps)} acts={len(acts)} users={len(users)} '
      f'tasks={len(tasks)} (open={meta["open_tasks"]})')
if errors:
    print('Pre-push checks FAILED:')
    for e in errors:
        print('  -', e)
    raise SystemExit(1)
print('Pre-push checks: OK')
