"""Build the live SharePoint-backed Sunwave dashboard.

Reuses CSS / dashboard JS / section HTML from build_combined.py and wraps
them with MSAL.js sign-in + Microsoft Graph API workbook fetch.
Output: index.html (also overwrites for GitHub Pages deploy)
"""
import re, os

# ── Constants from your Azure AD app + SharePoint location ───────────────────
CLIENT_ID       = 'debd63be-2397-4d6e-adb1-381574e7352b'
TENANT_ID       = '063ab74f-56d1-429a-b96d-a24a572025de'
SHAREPOINT_HOST = 'gshealthcarellc.sharepoint.com'
SITE_PATH       = 'sites/Provident'      # site-relative path, no leading slash
FILE_NAME       = 'MASTER_Sunwave_New_PowerQuerry.xlsx'

# ── Pull the existing CSS, JS, and section HTML from build_combined.py ───────
with open('build_combined.py', 'r', encoding='utf-8') as f:
    src = f.read()

def block(varname, raw=False):
    pat = rf'{varname} = (r?)"""(.*?)"""'
    m = re.search(pat, src, re.DOTALL)
    if not m: raise RuntimeError(f'Could not find {varname}')
    return m.group(2)

CSS = block('CSS')
JS  = block('JS')
SECTIONS = {n: block(n) for n in [
    'BILLING_SECTION','CENSUS_SECTION','MARKETING_SECTION','OPPORTUNITIES_SECTION',
    'REFERRAL_SECTION','UR_SECTION','CLINICAL_SECTION','OPERATIONS_SECTION','FIELD_EXPLORER_SECTION',
]}

# ── Add CSS for login + loading splash ──────────────────────────────────────
EXTRA_CSS = """
/* Live: login + loading overlay */
#liveOverlay { position: fixed; inset: 0; background: linear-gradient(135deg, #0f2540 0%, #1a6ec0 100%);
  display: flex; align-items: center; justify-content: center; z-index: 9999; color: #fff; font-family: Arial, sans-serif; }
.live-card { background: rgba(255,255,255,.08); padding: 32px 40px; border-radius: 12px; max-width: 460px;
  text-align: center; box-shadow: 0 10px 40px rgba(0,0,0,.5); backdrop-filter: blur(8px); }
.live-card h1 { font-size: 22px; margin-bottom: 6px; }
.live-card p  { font-size: 13px; opacity: .85; margin-bottom: 18px; }
.live-card small { font-size: 11px; opacity: .6; display: block; margin-top: 16px; }
.live-btn { padding: 11px 22px; background: linear-gradient(to bottom, #2e9c5a, #217346);
  border: 1px solid #145214; border-bottom: 3px solid #0d3b0d; border-radius: 6px; color: #fff;
  font-size: 13px; font-weight: 700; cursor: pointer; box-shadow: 0 4px 8px rgba(0,0,0,.4), inset 0 1px 0 rgba(255,255,255,.2);
  transition: all .15s; }
.live-btn:hover  { transform: translateY(-1px); box-shadow: 0 6px 10px rgba(0,0,0,.5); }
.live-btn:active { transform: translateY(2px); border-bottom-width: 1px; }
.spinner { width: 38px; height: 38px; border: 4px solid rgba(255,255,255,.2); border-top-color: #fff;
  border-radius: 50%; animation: spin 1s linear infinite; margin: 0 auto 14px; }
@keyframes spin { to { transform: rotate(360deg); } }
.live-progress { font-size: 12px; opacity: .8; min-height: 18px; }
.live-error { color: #ffd1d1; font-size: 12px; text-align: left; max-width: 420px;
  background: rgba(0,0,0,.25); padding: 10px 14px; border-radius: 6px; margin-top: 12px; word-break: break-word; }
"""

# ── JS data-loading wrapper around the dashboard ────────────────────────────
LOADER_JS = """
(function(){
  const msalConfig = {
    auth: {
      clientId: '__CLIENT_ID__',
      authority: 'https://login.microsoftonline.com/__TENANT_ID__',
      redirectUri: window.location.origin + window.location.pathname
    },
    cache: { cacheLocation: 'sessionStorage', storeAuthStateInCookie: false }
  };
  const SCOPES = ['Files.Read', 'Sites.Read.All'];
  const SHAREPOINT_HOST = '__SHAREPOINT_HOST__';
  const SITE_PATH       = '__SITE_PATH__';
  const FILE_NAME       = '__FILE_NAME__';

  let msalInstance;
  const CACHE_KEY = 'sunwave_data_v3';
  const CACHE_VERSION = 3;
  function readCache(){
    try {
      const raw = localStorage.getItem(CACHE_KEY);
      if (!raw) return null;
      const c = JSON.parse(raw);
      if (!c || c.v !== CACHE_VERSION || !c.data) return null;
      return c;
    } catch(e) { return null; }
  }
  function writeCache(data){
    try {
      localStorage.setItem(CACHE_KEY, JSON.stringify({ v: CACHE_VERSION, ts: Date.now(), data: data }));
    } catch(e) { console.warn('Cache write failed (storage full?):', e); }
  }
  function setProgress(s){ const e=document.getElementById('liveMsg'); if(e) e.textContent=s; }
  function showError(e){
    const card = document.getElementById('liveCard');
    card.innerHTML = '<h1>Error loading data</h1>'
      + '<p>Could not load the SharePoint workbook.</p>'
      + '<div class="live-error">' + (e && e.message ? e.message : String(e)) + '</div>'
      + '<button class="live-btn" style="margin-top:14px" onclick="location.reload()">Retry</button>';
  }

  async function getToken(){
    const accounts = msalInstance.getAllAccounts();
    let account = accounts[0];
    if (!account) {
      const r = await msalInstance.loginPopup({ scopes: SCOPES, prompt: 'select_account' });
      account = r.account;
    }
    msalInstance.setActiveAccount(account);
    try {
      const r = await msalInstance.acquireTokenSilent({ scopes: SCOPES, account });
      return r.accessToken;
    } catch (_) {
      const r = await msalInstance.acquireTokenPopup({ scopes: SCOPES });
      return r.accessToken;
    }
  }

  async function graph(path, token){
    const r = await fetch('https://graph.microsoft.com/v1.0' + path, {
      headers: { Authorization: 'Bearer ' + token, 'Content-Type': 'application/json' }
    });
    if (!r.ok) {
      const t = await r.text();
      throw new Error('Graph ' + r.status + ': ' + t.slice(0, 280));
    }
    return r.json();
  }

  async function fetchWorkbook(token){
    setProgress('Locating SharePoint site...');
    const site = await graph('/sites/' + SHAREPOINT_HOST + ':/' + SITE_PATH, token);

    setProgress('Finding workbook...');
    const search = await graph('/sites/' + site.id + "/drive/root/search(q='" + encodeURIComponent(FILE_NAME) + "')", token);
    const file = (search.value || []).find(f => f.name === FILE_NAME) || (search.value || [])[0];
    if (!file) throw new Error('File not found in SharePoint: ' + FILE_NAME);

    setProgress('Listing worksheets...');
    const ws = await graph('/sites/' + site.id + '/drive/items/' + file.id + '/workbook/worksheets', token);
    const sheetNames = ws.value.map(w => w.name);

    setProgress('Fetching ' + sheetNames.length + ' worksheets (0/' + sheetNames.length + ')...');
    const data = {};
    let done = 0;
    await Promise.all(sheetNames.map(async (name) => {
      try {
        const r = await graph('/sites/' + site.id + '/drive/items/' + file.id
          + '/workbook/worksheets/' + encodeURIComponent(name)
          + "/usedRange?$select=values", token);
        data[name] = r.values || [];
      } catch (e) {
        data[name] = [];
      }
      done++;
      setProgress('Fetching worksheets (' + done + '/' + sheetNames.length + ')...');
    }));
    return data;
  }

  // ── Excel value helpers ──
  function xDate(v){
    if (v === null || v === undefined || v === '') return '';
    if (typeof v === 'number') {
      const d = new Date(Math.round((v - 25569) * 86400000));
      return String(d.getUTCMonth()+1).padStart(2,'0') + '/' + String(d.getUTCDate()).padStart(2,'0') + '/' + d.getUTCFullYear();
    }
    const s = String(v).trim();
    if (!s) return '';
    let m = s.match(/^(\\d{1,2})[\\/\\-](\\d{1,2})[\\/\\-](\\d{2,4})/);
    if (m) { let yr=+m[3]; if (yr<100) yr+=2000; return String(+m[1]).padStart(2,'0')+'/'+String(+m[2]).padStart(2,'0')+'/'+yr; }
    m = s.match(/^(\\d{4})-(\\d{2})-(\\d{2})/);
    if (m) return String(+m[2]).padStart(2,'0')+'/'+String(+m[3]).padStart(2,'0')+'/'+m[1];
    return s;
  }
  function xNum(v){ if (v===null||v===undefined||v==='') return 0; const n=parseFloat(v); return isNaN(n)?0:Math.round(n*100)/100; }
  function xInt(v){ if (v===null||v===undefined||v==='') return null; const n=parseFloat(v); return isNaN(n)?null:Math.round(n); }
  function xStr(v){ if (v===null||v===undefined) return ''; return String(v).trim(); }
  function dowFromExcel(v){
    if (typeof v === 'number') {
      const d = new Date(Math.round((v - 25569) * 86400000));
      return d.getUTCDay();
    }
    const s = xDate(v);
    if (!s) return -1;
    const m = s.match(/^(\\d{2})\\/(\\d{2})\\/(\\d{4})/);
    if (!m) return -1;
    return new Date(Date.UTC(+m[3], +m[1]-1, +m[2])).getUTCDay();
  }
  function hourFromExcel(v){
    if (v===null||v===undefined||v==='') return -1;
    if (typeof v === 'number') return Math.floor(v * 24) % 24;
    const m = String(v).match(/(\\d{1,2}):/);
    return m ? +m[1] : -1;
  }

  // ── Transformation: Excel sheets → JSON shapes the dashboard expects ──
  function transform(sheets){
    const DATE_FIELDS = {
      'Census':'Admission Date','Census Active':'Admission Date',
      'Census_Admitted':'Admission Date','Census_Discharge':'Discharge Date',
      'GroupNotes':'session_date','Incident Report':'incident_reports.date_of_incident',
      'Opportunities Active':'created_on','Opportunities by Created Date':'created_on',
      'Opportunities':'created_on','Patients':'created_on',
      'Payment Report Payment Date':'payment_date','Payment Report Deposit Date':'deposit_date',
      'Referral Active':'created_on','Report Auth':'admission_date',
      'Report Deleted Form':'deleted_on','Report Diagnois Changes':'date_from',
      'Report Form Modified':'modified_on','Report Program Change':'start_on',
      'Report UR Changes':'admission_date','Users':'created_on',
    };
    const SKIP = ['Table of Contents','Realms','Bedboard','Payment Summary'];
    const DATE_COL_HINTS = ['date','_on','admission','discharge','deposit','review','modified','deleted'];

    // Build raw_data + tab_config for the 20 generic report tabs
    const raw_data = {};
    const tab_config = {};
    for (const [name, vals] of Object.entries(sheets)) {
      if (SKIP.includes(name)) continue;
      if (!vals || vals.length === 0) continue;
      const cols = vals[0].map(c => xStr(c));
      const dateColIdxs = cols.map((c, i) => {
        const lc = c.toLowerCase();
        return DATE_COL_HINTS.some(h => lc.includes(h)) ? i : -1;
      }).filter(i => i >= 0);
      const rows = vals.slice(1).map(r => r.map((c, i) => {
        if (c === null || c === undefined) return '';
        if (typeof c === 'number' && dateColIdxs.includes(i) && c > 25000 && c < 80000) return xDate(c);
        return String(c);
      }));
      raw_data[name] = { columns: cols, rows: rows };
      const dc = DATE_FIELDS[name];
      tab_config[name] = (dc && cols.indexOf(dc) >= 0) ? cols.indexOf(dc) : -1;
    }

    function map(name, fn){
      const v = sheets[name]; if (!v || v.length < 2) return [];
      const cols = v[0].map(c => xStr(c));
      const idx = (c) => cols.indexOf(c);
      const out = [];
      for (let i = 1; i < v.length; i++) out.push(fn(v[i], idx));
      return out;
    }

    const billing_rows = map('Payment Report Deposit Date', (r, idx) => ({
      deposit_date:         xDate(r[idx('deposit_date')]),
      payer_name:           xStr (r[idx('payer_name')]),
      level_of_care:        xStr (r[idx('level_of_care')]),
      adjustment_type:      xStr (r[idx('adjustment_type')]),
      service_facility:     xStr (r[idx('service_facility')]),
      service_name:         xStr (r[idx('service_name')]),
      payment_type:         xStr (r[idx('payment_type')]),
      line_charge_amount:   xNum (r[idx('line_charge_amount')]),
      line_paid_amount:     xNum (r[idx('line_paid_amount')]),
      line_adjusted:        xNum (r[idx('line_adjusted')]),
      line_allocated_amount:xNum (r[idx('line_allocated_amount')]),
      line_patient_name:    xStr (r[idx('line_patient_name')]),
      procedure_code:       xStr (r[idx('procedure_code')]),
    }));

    const census_rows = map('Census', (r, idx) => ({
      adm:   xDate(r[idx('Admission Date')]),
      dis:   xDate(r[idx('Discharge Date')]),
      loc:   xStr (r[idx('Admission Level Of Care')]),
      cloc:  xStr (r[idx('Current Level Of Care')]),
      gen:   xStr (r[idx('Patient Gender Code')]),
      age:   xInt (r[idx('Age')]),
      drug:  xStr (r[idx('Primary Drug Of Choice ')]),
      ref:   xStr (r[idx('Referral Source')]),
      dtype: xStr (r[idx('Discharge Type')]),
      los:   xInt (r[idx('Length Of Stay')]),
      name:  xStr (r[idx('Patient Name')]),
    }));

    const opp_rows = map('Opportunities by Created Date', (r, idx) => {
      const adVal = r[idx('admission_date')];
      const isValidYear = (typeof adVal === 'number') ? (adVal > 36500) : true;
      return {
        id:      xStr(r[idx('opportunity_id')]),
        co:      xDate(r[idx('created_on')]),
        adm:     isValidYear ? xDate(adVal) : '',
        outcome: xStr(r[idx('outcome')]),
        stage:   xStr(r[idx('stage')]),
        loc:     xStr(r[idx('level_of_care')]),
        ins:     xStr(r[idx('insurance provider')]),
        ref:     xStr(r[idx('referral name')]),
        lost_r:  xStr(r[idx('lost reason')]),
        aband_r: xStr(r[idx('abandoned reason')]),
        name:    xStr(r[idx('patient name')]),
      };
    });

    const auth_rows = map('Report Auth', (r, idx) => ({
      adm:      xDate(r[idx('admission_date')]),
      nrd:      xDate(r[idx('next_review_date')]),
      code:     xStr (r[idx('authorization_code')]),
      au:       xNum (r[idx('authorized_units')]),
      bu:       xNum (r[idx('billed_units_total')]),
      ins:      xStr (r[idx('insurance_provider')]),
      reviewer: xStr (r[idx('ur_reviewer')]),
      patient:  xStr (r[idx('patient_name')]),
      facility: xStr (r[idx('service_facility')]),
    }));

    const ops_rows = map('Census_Admitted', (r, idx) => ({
      date:      xDate(r[idx('Admission Date')]),
      hour:      hourFromExcel(r[idx('Admission Time')]),
      dow:       dowFromExcel (r[idx('Admission Date')]),
      rep:       xStr (r[idx('Admissions Rep')]),
      therapist: xStr (r[idx('Assigned Therapist')]),
      ins:       xStr (r[idx('Insurance Name')]),
      loc:       xStr (r[idx('Admission Level Of Care')]),
      name:      xStr (r[idx('Patient Name')]),
    }));

    const gn_rows = map('GroupNotes', (r, idx) => ({
      date:   xDate(r[idx('session_date')]),
      title:  xStr (r[idx('group_title')]),
      status: xStr (r[idx('status')]),
      mins:   Math.round(parseFloat(r[idx('length_time')])||0),
    }));

    // Timeline rows for Opportunity expand/collapse
    function xDateTime(v){
      if (v===null||v===undefined||v==='') return '';
      if (typeof v === 'number') {
        const d = new Date(Math.round((v - 25569) * 86400000));
        const hr = d.getUTCHours(), mn = d.getUTCMinutes();
        const ampm = hr>=12?'PM':'AM';
        const h12 = hr%12 === 0 ? 12 : hr%12;
        return String(d.getUTCMonth()+1).padStart(2,'0')+'/'+String(d.getUTCDate()).padStart(2,'0')+'/'+d.getUTCFullYear()
          + ' ' + String(h12).padStart(2,'0')+':'+String(mn).padStart(2,'0')+' '+ampm;
      }
      return String(v).trim();
    }
    function xSortKey(v){
      if (typeof v === 'number') return v;
      if (!v) return 0;
      const d = new Date(String(v));
      return isNaN(d) ? 0 : d.getTime()/1000;
    }
    const timeline_rows = map('Timeline', (r, idx) => ({
      oid:     xStr(r[idx('opportunity_id')]) || xStr(r[idx('associated_with_id')]),
      date:    xDateTime(r[idx('activity_date')]),
      subject: xStr(r[idx('task_subject')]),
      type:    xStr(r[idx('type')]),
      by:      xStr(r[idx('created_by_name')]),
      wf:      xStr(r[idx('workflow_status')]),
      text:    xStr(r[idx('text')]),
      assoc:   xStr(r[idx('associated_with')]),
      sortKey: xSortKey(r[idx('activity_date')]),
    }));

    // Referral Active rows
    const referral_rows = map('Referral Active', (r, idx) => ({
      id:    xStr(r[idx('referral_id')]) || xStr(r[idx('id')]),
      co:    xDate(r[idx('created_on')]),
      name:  xStr(r[idx('referral name')] || r[idx('name')]),
      type:  xStr(r[idx('referral type')]),
      stage: xStr(r[idx('referral source stage')]),
      owner: xStr(r[idx('referral_source_owner')]),
      city:  xStr(r[idx('referral source city')]),
      state: xStr(r[idx('referral source state')]),
    }));

    return { raw_data, tab_config, billing_rows, census_rows, opp_rows, auth_rows, ops_rows, gn_rows, timeline_rows, referral_rows };
  }

  function inject(id, obj){ document.getElementById(id).textContent = JSON.stringify(obj); }

  function applyData(t){
    inject('generalData', t.raw_data);
    inject('dateIdx',     t.tab_config);
    inject('billingData', t.billing_rows);
    inject('censusData',  t.census_rows);
    inject('oppData',     t.opp_rows);
    inject('authData',    t.auth_rows);
    inject('opsData',     t.ops_rows);
    inject('gnData',      t.gn_rows);
    inject('tlData',      t.timeline_rows);
    inject('refData',     t.referral_rows);
  }

  function bootDashboard(){
    document.getElementById('liveOverlay').remove();
    document.getElementById('app').style.display = 'flex';
    const tpl = document.getElementById('dashboardJS');
    const s = document.createElement('script');
    s.textContent = tpl.textContent;
    document.body.appendChild(s);
  }

  function showFreshIndicator(){
    const el = document.createElement('div');
    el.style.cssText = 'position:fixed;top:8px;right:14px;background:#217346;color:#fff;padding:5px 10px;border-radius:4px;font-size:11px;font-weight:600;z-index:9999;box-shadow:0 2px 6px rgba(0,0,0,.3);cursor:pointer;font-family:Arial,sans-serif';
    el.textContent = '↻ Fresh data available — click to refresh';
    el.onclick = () => location.reload();
    document.body.appendChild(el);
    setTimeout(() => { el.style.opacity = '0'; el.style.transition = 'opacity .5s'; setTimeout(() => el.remove(), 500); }, 8000);
  }

  async function fetchFresh(){
    const token = await getToken();
    const sheets = await fetchWorkbook(token);
    const t = transform(sheets);
    writeCache(t);
    return t;
  }

  async function start(){
    // ── Cached path: instant load, then refresh in background ──
    const cached = readCache();
    if (cached && cached.data) {
      applyData(cached.data);
      bootDashboard();
      // Refresh in background, no UI blocking
      setTimeout(async () => {
        try {
          const fresh = await fetchFresh();
          // Compare hashes/sizes to decide whether to notify
          const changed = JSON.stringify(fresh).length !== JSON.stringify(cached.data).length;
          if (changed) showFreshIndicator();
        } catch (e) { console.warn('Background refresh failed:', e); }
      }, 200);
      return;
    }

    // ── Cold path: show loader, fetch, render ──
    document.getElementById('liveCard').innerHTML =
      '<div class="spinner"></div><h1>Loading dashboard</h1>'
    + '<p>Connecting to SharePoint... (first-time setup, ~30s)</p>'
    + '<div class="live-progress" id="liveMsg">Authenticating...</div>';
    try {
      const token = await getToken();
      const sheets = await fetchWorkbook(token);
      setProgress('Processing data...');
      const t = transform(sheets);
      writeCache(t);
      applyData(t);
      bootDashboard();
    } catch (e) {
      console.error(e);
      showError(e);
    }
  }

  function showLogin(){
    document.getElementById('liveCard').innerHTML =
      '<h1>Sunwave Dashboard</h1>'
    + '<p>Sign in with your Microsoft 365 account to view live SharePoint data.</p>'
    + '<button class="live-btn" id="liveLoginBtn">Sign in with Microsoft 365</button>'
    + '<small>Provident Healthcare Management</small>';
    document.getElementById('liveLoginBtn').onclick = start;
  }

  // Init MSAL once page + library are ready
  let bootAttempts = 0;
  async function boot(){
    bootAttempts++;
    if (!window.msal || !window.msal.PublicClientApplication) {
      if (bootAttempts > 100) { showError(new Error('MSAL.js failed to load from CDN. Check internet connection or content blockers.')); return; }
      setTimeout(boot, 50); return;
    }
    try {
      msalInstance = new msal.PublicClientApplication(msalConfig);
      if (typeof msalInstance.initialize === 'function') {
        await msalInstance.initialize();
      }
      await msalInstance.handleRedirectPromise();
      const accounts = msalInstance.getAllAccounts();
      if (accounts.length > 0) start();
      else showLogin();
    } catch (e) {
      console.error('Boot error:', e);
      showError(e);
    }
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', boot);
  else boot();
})();
"""

LOADER_JS = (LOADER_JS
             .replace('__CLIENT_ID__',       CLIENT_ID)
             .replace('__TENANT_ID__',       TENANT_ID)
             .replace('__SHAREPOINT_HOST__', SHAREPOINT_HOST)
             .replace('__SITE_PATH__',       SITE_PATH)
             .replace('__FILE_NAME__',       FILE_NAME))

# Keep the dashboard JS exactly as-is (including the runDashboard() invoke at the
# bottom). It will be injected as a fresh <script> by the loader AFTER the data
# placeholders have been populated, so all `const X = JSON.parse(...)` calls
# read real data, not the empty placeholders.
DASHBOARD_JS_WRAPPED = JS

# ── Compose final HTML ──────────────────────────────────────────────────────
html = (
'<!DOCTYPE html>\n<html lang="en">\n<head>\n'
'<meta charset="UTF-8">\n'
'<meta name="viewport" content="width=device-width,initial-scale=1">\n'
'<title>Sunwave Dashboard — Live</title>\n'
'<style>' + CSS + EXTRA_CSS + '</style>\n'
'</head>\n<body>\n'

# Login / loading overlay (shown first)
'<div id="liveOverlay">\n'
'  <div class="live-card" id="liveCard">\n'
'    <div class="spinner"></div>\n'
'    <h1>Sunwave Dashboard</h1>\n'
'    <p>Initializing...</p>\n'
'  </div>\n'
'</div>\n'

# Main app (hidden until data loads) — new top-tab + filter rail layout
'<div id="app" style="display:none">\n'
'  <div id="topbar">\n'
'    <div class="brand"><span class="name">Sunwave Dashboard</span><span class="sub">Provident Healthcare Management</span></div>\n'
'    <nav id="tabBar"></nav>\n'
'    <button class="topbar-action" onclick="toggleTabsMenu(event)" title="Show / hide tabs">&#9881;&nbsp; Tabs</button>\n'
'    <button class="topbar-action" onclick="location.reload()" title="Refresh data">&#8635;&nbsp; Refresh</button>\n'
'  </div>\n'
'  <div id="tabsMenu" class="tabs-menu"></div>\n'
'  <div id="main">\n'
'    <aside id="filterRail"><h3>Filters</h3><div id="filterContent"></div></aside>\n'
'    <div id="content">\n'
'      <div class="page-header">\n'
'        <div><h2 id="pageTitle">AR / Billing Dashboard</h2>\n'
'             <small id="pageSub">Live from SharePoint</small></div>\n'
'        <div class="page-actions">\n'
'          <button class="page-action-btn green" onclick="exportPageToExcel()">&#8595;&nbsp; Excel</button>\n'
'          <button class="page-action-btn blue"  onclick="exportPageToPNG()">&#8595;&nbsp; PNG</button>\n'
'        </div>\n'
'      </div>\n'
'      <div id="sectionsWrap">\n'
+ SECTIONS['BILLING_SECTION']
+ SECTIONS['CENSUS_SECTION']
+ SECTIONS['MARKETING_SECTION']
+ SECTIONS['OPPORTUNITIES_SECTION']
+ SECTIONS.get('REFERRAL_SECTION', '')
+ SECTIONS['UR_SECTION']
+ SECTIONS['CLINICAL_SECTION']
+ SECTIONS['OPERATIONS_SECTION']
+ SECTIONS['FIELD_EXPLORER_SECTION'] +
'      </div>\n'
'    </div>\n'
'  </div>\n'
'</div>\n'

# External libs (Excel + PNG export)
'<script src="https://cdn.jsdelivr.net/npm/xlsx@0.18.5/dist/xlsx.full.min.js"></script>\n'
'<script src="https://cdn.jsdelivr.net/npm/html2canvas@1.4.1/dist/html2canvas.min.js"></script>\n'

# Empty data placeholders (populated after Graph fetch)
'<script type="application/json" id="generalData">{}</script>\n'
'<script type="application/json" id="dateIdx">{}</script>\n'
'<script type="application/json" id="billingData">[]</script>\n'
'<script type="application/json" id="censusData">[]</script>\n'
'<script type="application/json" id="oppData">[]</script>\n'
'<script type="application/json" id="authData">[]</script>\n'
'<script type="application/json" id="opsData">[]</script>\n'
'<script type="application/json" id="gnData">[]</script>\n'
'<script type="application/json" id="tlData">[]</script>\n'
'<script type="application/json" id="refData">[]</script>\n'

# MSAL.js library (v3 - requires initialize()). Multiple CDNs as fallback.
'<script src="https://cdn.jsdelivr.net/npm/@azure/msal-browser@3.10.0/lib/msal-browser.min.js"></script>\n'
'<script>if(!window.msal){document.write(\'<script src="https://unpkg.com/@azure/msal-browser@3.10.0/lib/msal-browser.min.js"><\\/script>\');}</script>\n'
'<script>if(!window.msal){document.write(\'<script src="https://alcdn.msauth.net/browser/3.10.0/js/msal-browser.min.js"><\\/script>\');}</script>\n'

# Dashboard JS held as inert template text. Loader copies its textContent into
# a real <script> element after data placeholders are populated.
'<script id="dashboardJS" type="text/x-dashboard-js">' + DASHBOARD_JS_WRAPPED + '</script>\n'

# Live data loader
'<script>' + LOADER_JS + '</script>\n'

'</body>\n</html>'
)

with open('index.html', 'w', encoding='utf-8') as f:
    f.write(html)
print(f"Wrote index.html: {os.path.getsize('index.html')/1024:.1f} KB")
