"""build_crm.py — generate Sunwave CRM Dashboard at crm/index.html.

Inputs:
  - report_data.json   (from build pipeline; has every sheet keyed by name)
  - dashboard_template_crm.html

Output:
  - crm/index.html

Usage (CI or local):
  python fetch_excel.py           # refresh report_data.json from SharePoint
  python build_crm.py             # build crm/index.html from template + data
"""
import json
import os
import re
from datetime import datetime, timezone

# ── Load source data ────────────────────────────────────────────────────────
with open('report_data.json', 'r', encoding='utf-8') as f:
    raw = json.load(f)

# Diagnostic: list all sheet keys and row counts
print('[CRM] report_data.json sheet inventory:')
for k in sorted(raw.keys()):
    rows = raw[k].get('rows') or []
    cols = raw[k].get('columns') or []
    print(f'  - {k!r:48s} rows={len(rows):4d}  cols={len(cols)}')
print()


def sheet_to_dicts(name):
    """Convert a sheet from report_data.json into a list of dicts."""
    sheet = raw.get(name)
    if not sheet:
        return []
    cols = sheet.get('columns') or []
    rows = sheet.get('rows') or []
    return [dict(zip(cols, r)) for r in rows]


# ── Date parsing helpers ────────────────────────────────────────────────────
DATE_PATTERNS = [
    # m/d/yyyy [hh:mm[:ss] [AM|PM]]
    re.compile(r'^(\d{1,2})[\-/](\d{1,2})[\-/](\d{4})\s+(\d{1,2}):(\d{2})(?::(\d{2}))?\s*(AM|PM|am|pm)?'),
    # m/d/yyyy
    re.compile(r'^(\d{1,2})[\-/](\d{1,2})[\-/](\d{4})$'),
    # yyyy-mm-dd[Thh:mm:ss]
    re.compile(r'^(\d{4})-(\d{2})-(\d{2})(?:[T\s](\d{2}):(\d{2})(?::(\d{2}))?)?'),
]

def parse_date(v):
    """Parse a date string into ISO 8601. Return '' if cannot parse."""
    if v is None or v == '' or (isinstance(v, float) and v != v):  # NaN check
        return ''
    s = str(v).strip()
    if not s or s.lower() in ('nan', 'nat', 'none'):
        return ''

    # m/d/yyyy hh:mm[:ss] [am/pm]
    m = DATE_PATTERNS[0].match(s)
    if m:
        mo, dy, yr = int(m.group(1)), int(m.group(2)), int(m.group(3))
        hr = int(m.group(4)); mn = int(m.group(5))
        sc = int(m.group(6)) if m.group(6) else 0
        ampm = (m.group(7) or '').upper()
        if ampm == 'PM' and hr < 12: hr += 12
        if ampm == 'AM' and hr == 12: hr = 0
        try:
            return datetime(yr, mo, dy, hr, mn, sc).isoformat()
        except Exception:
            return ''

    m = DATE_PATTERNS[1].match(s)
    if m:
        mo, dy, yr = int(m.group(1)), int(m.group(2)), int(m.group(3))
        try:
            return datetime(yr, mo, dy).isoformat()
        except Exception:
            return ''

    m = DATE_PATTERNS[2].match(s)
    if m:
        yr, mo, dy = int(m.group(1)), int(m.group(2)), int(m.group(3))
        hr = int(m.group(4)) if m.group(4) else 0
        mn = int(m.group(5)) if m.group(5) else 0
        sc = int(m.group(6)) if m.group(6) else 0
        try:
            return datetime(yr, mo, dy, hr, mn, sc).isoformat()
        except Exception:
            return ''

    # Last resort: try datetime.fromisoformat
    try:
        return datetime.fromisoformat(s.replace('Z', '+00:00')).isoformat()
    except Exception:
        return ''


def safe_str(v):
    if v is None or (isinstance(v, float) and v != v):
        return ''
    s = str(v).strip()
    return '' if s.lower() in ('nan', 'nat', 'none') else s


# ── Process Opportunities ───────────────────────────────────────────────────
# Primary source: "Opportunity" sheet. Fall back to other names if SharePoint
# sheet hasn't been renamed yet.
def pick_sheet(*names):
    for n in names:
        if n in raw and raw[n].get('rows'):
            print(f'  Using sheet: {n!r}')
            return sheet_to_dicts(n)
    print(f'  Warning: none of {names} found in report_data.json')
    return []

print('[CRM] Loading Opportunity sheet...')
opps_src = pick_sheet('Opportunity', 'Opportunities', 'Opportunities by Created Date', 'Opportunities Active')
opps = []
for r in opps_src:
    oid = safe_str(r.get('opportunity_id') or r.get('opportunity_legacy_id') or r.get('id'))
    if not oid:
        continue
    opps.append({
        'id':      oid,
        'name':    safe_str(r.get('patient name') or r.get('Patient Name') or ''),
        'co':      parse_date(r.get('created_on')),
        'admit':   parse_date(r.get('admission_date')),
        'outcome': safe_str(r.get('outcome')),
        'stage':   safe_str(r.get('stage')),
        'loc':     safe_str(r.get('level_of_care')),
        'ins':     safe_str(r.get('insurance provider')),
        'ref':     safe_str(r.get('referral name')),
        'lost_r':  safe_str(r.get('lost reason')),
        'aband_r': safe_str(r.get('abandoned reason')),
        'rep':     safe_str(r.get('adm. representative')),
        'created_by': safe_str(r.get('created_by')),
    })

# ── Process Timeline (activities) ──────────────────────────────────────────
print('[CRM] Loading Timeline sheet...')
acts_src = pick_sheet('Timeline')
acts = []
for r in acts_src:
    oid = safe_str(r.get('opportunity_id') or r.get('associated_with_id'))
    typ = safe_str(r.get('type'))
    by  = safe_str(r.get('created_by_name'))
    dt  = parse_date(r.get('activity_date'))
    if not (oid or typ):
        continue
    acts.append({
        'oid':   oid,
        'aid':   safe_str(r.get('id')),
        'type':  typ,
        'subj':  safe_str(r.get('task_subject'))[:140],
        'text':  safe_str(r.get('text'))[:400],
        'by':    by,
        'wf':    safe_str(r.get('workflow_status')),
        'date':  dt,
        'assoc': safe_str(r.get('associated_with')),
        'task_type': safe_str(r.get('task_type')),
        'task_status': safe_str(r.get('task_status')),
        'task_due_date': parse_date(r.get('task_due_date')),
        'reminder_date_time': parse_date(r.get('reminder_date_time')),
        'assigned_to_name': safe_str(r.get('assigned_to_name')),
    })

# ── Derive Users from Timeline (created_by_name + assigned_to_name) ────────
print('[CRM] Deriving Users from Timeline...')
user_set = {}  # lowercase_name -> {name, count}
for a in acts:
    for nm in (a.get('by'), a.get('assigned_to_name')):
        nm = safe_str(nm)
        if not nm: continue
        key = nm.lower()
        if key not in user_set:
            user_set[key] = {'id': '', 'name': nm, 'email': '', 'role': '', 'count': 0}
        user_set[key]['count'] += 1
users = sorted(user_set.values(), key=lambda u: -u['count'])

# ── Derive open CRM Tasks from Timeline rows where type='Task' ─────────────
print('[CRM] Deriving Tasks from Timeline rows where type=Task...')
tasks = []
for r in acts_src:
    typ = safe_str(r.get('type'))
    if typ != 'Task':
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
        'due':        parse_date(r.get('task_due_date')),
        'created':    parse_date(r.get('created_on')),
        'reminder':   parse_date(r.get('reminder_date_time')),
        'is_open':    is_open,
    })

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
    '/*INJECT_META*/null':  json.dumps(meta,  separators=sep, ensure_ascii=True),
}
for placeholder, value in replacements.items():
    if placeholder not in html:
        print(f'Warning: placeholder {placeholder} not found in template')
    html = html.replace(placeholder, value)

# Escape closing script tags inside JSON for safety
# (Already-escaped strings re-encode fine; this is a final defensive pass.)
# Note: separators above already produce safe JSON; the embedded text fields
# may contain "</script>" which would break the page — replace just the slash.
html = re.sub(r'(<script type="application/json"[^>]*>)([^<]*?)(</script>)',
              lambda m: m.group(1) + m.group(2).replace('</', '<\\/') + m.group(3),
              html)

# Pre-push checks
errors = []
if '/*INJECT_' in html:
    errors.append('Some /*INJECT_*/ placeholders remained unfilled')
# Brace balance (rough check)
open_b = html.count('{'); close_b = html.count('}')
if abs(open_b - close_b) > 50:  # tolerate minor diffs (CSS, template literals)
    errors.append(f'Brace imbalance: {{={open_b} }}={close_b}')

# Output
os.makedirs('crm', exist_ok=True)
out_path = os.path.join('crm', 'index.html')
with open(out_path, 'w', encoding='utf-8') as f:
    f.write(html)

print(f'Wrote {out_path}: {os.path.getsize(out_path)//1024} KB')
print(f'  opps={len(opps)} acts={len(acts)} users={len(users)} '
      f'tasks={len(tasks)} (open={meta["open_tasks"]})')
if errors:
    print('Pre-push checks FAILED:')
    for e in errors: print('  -', e)
    raise SystemExit(1)
print('Pre-push checks: OK')
