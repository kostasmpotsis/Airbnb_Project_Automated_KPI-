"""
Airbnb AutoSync → Snowflake
Τρέχει αυτόματα από GitHub Actions κάθε 3 μέρες.
Τα credentials διαβάζονται από αρχεία που γράφει το workflow από τα GitHub Secrets.
"""

import os
import re
from base64 import urlsafe_b64decode
from datetime import datetime, timedelta

import pandas as pd
import snowflake.connector
from bs4 import BeautifulSoup
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from snowflake.connector.pandas_tools import write_pandas

# ── Snowflake config ──────────────────────────────────────────────────────────
SNOWFLAKE_ACCOUNT  = 'VFHKJRE-GP30607'
SNOWFLAKE_USER     = 'KOSTAS'
SNOWFLAKE_PASSWORD = os.environ.get('SNOWFLAKE_PASSWORD', 'Mpotsi!1kostas')
SNOWFLAKE_DB       = 'AIRBNB_DB'
SNOWFLAKE_SCHEMA   = 'RAW'
SNOWFLAKE_WH       = 'AIRBNB_WH'
SNOWFLAKE_TABLE    = 'RESERVATIONS_RAW'

# ── Gmail config ──────────────────────────────────────────────────────────────
SCOPES      = ["https://www.googleapis.com/auth/gmail.readonly"]
CREDS_FILE  = "credentials.json"   # Γράφεται από το workflow από το secret GMAIL_CREDENTIALS_JSON
TOKEN_FILE  = "token.json"         # Γράφεται από το workflow από το secret GMAIL_TOKEN_JSON

# ═════════════════════════════════════════════════════════════════════════════
# 1) Gmail Authentication
# ═════════════════════════════════════════════════════════════════════════════

print("🔐 Φόρτωση Gmail credentials...")
creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)

if creds.expired and creds.refresh_token:
    print("🔄 Ανανέωση token...")
    creds.refresh(Request())
    # Αποθήκευση updated token (χρήσιμο για local εκτέλεση, αλλά στο CI δεν persist)
    with open(TOKEN_FILE, 'w') as f:
        f.write(creds.to_json())

service = build("gmail", "v1", credentials=creds)
print("✅ Gmail service έτοιμο")

# ═════════════════════════════════════════════════════════════════════════════
# 2) Βρες την τελευταία ημερομηνία στο Snowflake
# ═════════════════════════════════════════════════════════════════════════════

print("\n📊 Σύνδεση στο Snowflake...")
conn = snowflake.connector.connect(
    account=SNOWFLAKE_ACCOUNT, user=SNOWFLAKE_USER,
    password=SNOWFLAKE_PASSWORD, database=SNOWFLAKE_DB,
    schema=SNOWFLAKE_SCHEMA, warehouse=SNOWFLAKE_WH
)
cur = conn.cursor()
cur.execute(f"SELECT MAX(_LOADED_AT) FROM {SNOWFLAKE_TABLE}")
row = cur.fetchone()
conn.close()

FALLBACK_DATE = datetime(2026, 3, 25)

if row and row[0]:
    last_loaded = row[0]
    fetch_from  = last_loaded - timedelta(days=1)
    print(f"📅 Τελευταία εγγραφή: {last_loaded.strftime('%d/%m/%Y %H:%M')}")
else:
    fetch_from = FALLBACK_DATE
    print(f"⚠️  Fallback date: {FALLBACK_DATE.strftime('%d/%m/%Y')}")

start_date = fetch_from.strftime("%Y/%m/%d")
end_date   = datetime.now().strftime("%Y/%m/%d")
print(f"🔍 Αναζήτηση emails: {start_date} → {end_date}")

# ═════════════════════════════════════════════════════════════════════════════
# 3) Φέρε emails από Gmail με pagination
# ═════════════════════════════════════════════════════════════════════════════

query = f'from:airbnb.com after:{start_date} before:{end_date}'
msgs, page_token = [], None

while True:
    kwargs = {"userId": "me", "q": query, "maxResults": 500}
    if page_token:
        kwargs["pageToken"] = page_token
    resp = service.users().messages().list(**kwargs).execute()
    batch = resp.get("messages", [])
    msgs.extend(batch)
    page_token = resp.get("nextPageToken")
    print(f"   📄 Σελίδα: {len(batch)} emails (σύνολο: {len(msgs)})")
    if not page_token:
        break

print(f"\n📨 Σύνολο: {len(msgs)} Airbnb emails")

# ═════════════════════════════════════════════════════════════════════════════
# 4) Parsing helpers
# ═════════════════════════════════════════════════════════════════════════════

MONTH_MAP = {
    "Ιαν": 1, "Φεβ": 2, "Μαρ": 3, "Απρ": 4,
    "Μαΐ": 5, "Μαι": 5, "Μα": 5,
    "Ιουν": 6, "Ιουλ": 7,
    "Αυγ": 8, "Σεπ": 9, "Οκτ": 10, "Νοε": 11, "Δεκ": 12
}

def get_airbnb_email_body(service, msg_id):
    msg = service.users().messages().get(userId="me", id=msg_id, format="full").execute()
    payload = msg.get("payload", {})
    if "parts" in payload:
        for part in payload["parts"]:
            mime = part.get("mimeType", "")
            body = part.get("body", {})
            if mime == "text/plain" and "data" in body:
                return urlsafe_b64decode(body["data"]).decode("utf-8", errors="ignore")
            if mime == "text/html" and "data" in body:
                raw_html = urlsafe_b64decode(body["data"]).decode("utf-8", errors="ignore")
                return BeautifulSoup(raw_html, "lxml").get_text("\n", strip=True)
    body = payload.get("body", {})
    if "data" in body:
        return urlsafe_b64decode(body["data"]).decode("utf-8", errors="ignore")
    return "(EMPTY EMAIL BODY)"

def _parse_price(s):
    if not s:
        return None
    s = str(s).replace("€", "").replace(" ", "").replace(".", "").replace(",", ".")
    s = s.replace("—", "-").replace("–", "-").strip()
    try:
        return float(s)
    except:
        return None

def _convert_date(date_str, year_hint=None):
    if pd.isna(date_str):
        return None
    s = str(date_str).strip()
    if not s:
        return None
    m = re.search(r'(\d{1,2})\s+([Α-Ωα-ωΆ-Ώ]+)', s)
    if not m:
        return None
    day = int(m.group(1))
    month_str = m.group(2)
    month = None
    for k, v in MONTH_MAP.items():
        if month_str.startswith(k[:3]):
            month = v
            break
    if not month:
        return None
    year = year_hint or datetime.now().year
    try:
        return datetime(year, month, day)
    except:
        return None

def _convert_time(time_str):
    if not time_str:
        return None
    t = time_str.strip()
    pm = bool(re.search(r'μ\.?μ', t, re.IGNORECASE))
    am = bool(re.search(r'π\.?μ', t, re.IGNORECASE))
    digits = re.sub(r'[^\d:]', '', t)
    try:
        parts = digits.split(":")
        h = int(parts[0])
        mn = int(parts[1]) if len(parts) > 1 else 0
        if pm and h != 12:
            h += 12
        if am and h == 12:
            h = 0
        return f"{h:02d}:{mn:02d}:00"
    except:
        return None

def _detect_status(subject, body):
    combined = (subject + " " + body).lower()
    if any(x in combined for x in ["ακυρώθηκε", "ακύρωση", "cancel"]):
        return "Canceled"
    return "Confirmed"

def parse_airbnb_email(text):
    rec = {}
    m = re.search(r'DATE:\s*(.+?)(?:\n|$)', text)
    rec["Email Sent Date"] = m.group(1).strip() if m else None
    m = re.search(r'SUBJECT:\s*(.+?)(?:\n|$)', text)
    rec["Subject"] = m.group(1).strip() if m else None
    rec["Status"]        = _detect_status(rec.get("Subject", ""), text)
    rec["CategoryValue"] = 0
    m = re.search(r'[Οο]/[Ηη]\s+(.+?)\s+(?:καταφθάνει|φτάνει|φθάνει)', rec.get("Subject", ""), re.IGNORECASE)
    if not m:
        m = re.search(r'επισκέπτης σας,\s+([Α-Ωα-ωA-Za-z\s]+?)(?:,\s|\s+έπρεπε)', text)
    rec["Guest Name"] = m.group(1).strip() if m else None
    m = re.search(r'(KOSTAS\s+ACROPOLIS\s+STUDIO\s+\d+)', text, re.IGNORECASE)
    rec["Listing"] = m.group(1).strip().upper() if m else None

    DAY_PREFIX = r'(?:(?:Δευ|Τρι|Τετ|Πεμ|Παρ|Σαβ|Κυρ)[^\d]*)?'
    DATE_PAT   = r'(\d{1,2}\s+[Α-Ωα-ω]{2,})'
    m = re.search(r'Άφιξη.*?Αναχώρηση.*?' + DAY_PREFIX + DATE_PAT + r'\s+' + DAY_PREFIX + DATE_PAT, text, re.DOTALL)
    if not m:
        m = re.search(r'Άφιξη\s*\n' + DAY_PREFIX + r'\s*' + DATE_PAT + r'.*?Αναχώρηση\s*\n' + DAY_PREFIX + r'\s*' + DATE_PAT, text, re.DOTALL)
    if not m:
        m = re.search(r'Άφιξη[:\s]+' + DATE_PAT + r'.*?Αναχώρηση[:\s]+' + DATE_PAT, text, re.DOTALL)
    if not m:
        m = re.search(r'(?:Ημερομηνίες|Άφιξη)[^\d]*' + DATE_PAT + r'[^\d]+' + DATE_PAT, text, re.DOTALL)

    if m:
        rec["Check-in Date"]  = m.group(1).strip()
        rec["Check-out Date"] = m.group(2).strip()
    else:
        mc = re.search(r'(\d{1,2})\s*[–\-]\s*(\d{1,2})\s+([Α-Ωα-ω]{2,})', text)
        if mc:
            month_part = mc.group(3).strip()
            rec["Check-in Date"]  = f"{mc.group(1)} {month_part}"
            rec["Check-out Date"] = f"{mc.group(2)} {month_part}"
        else:
            rec["Check-in Date"]  = None
            rec["Check-out Date"] = None

    times = re.findall(r'(\d{1,2}:\d{2}\s*[μπ]\.\s*[μπ]\.)', text)
    rec["Check-in Time"]  = times[0].strip() if len(times) > 0 else None
    rec["Check-out Time"] = times[1].strip() if len(times) > 1 else None
    m = re.search(r'(\d+)\s+ενήλικ', text)
    if not m:
        m = re.search(r'(\d+)\s+επισκέπτ', text)
    rec["Guests Count"] = int(m.group(1)) if m else None
    m = re.search(r'ΚΩΔΙΚ[ΌO][ΣS]\s+ΕΠΙΒΕΒΑ[ΊΙ]ΩΣΗΣ\s*\n\s*([A-Z0-9]{8,12})', text)
    if not m:
        m = re.search(r'κράτηση\s+([A-Z0-9]{8,12})', text)
    if not m:
        m = re.search(r'\b(HM[A-Z0-9]{8,10})\b', text)
    rec["Confirmation Code"] = m.group(1).strip() if m else None
    m = re.search(r'(\d+)\s+κριτικ', text)
    rec["Guest Reviews"] = float(m.group(1)) if m else None
    m = re.search(r'€\s*([\d\.,]+)\s*x\s*(\d+)\s*διανυκτ', text)
    if m:
        rec["Nightly Rate"]  = _parse_price(m.group(1))
        rec["Nights Count"]  = float(m.group(2))
        rec["Nightly Total"] = round(rec["Nightly Rate"] * rec["Nights Count"], 2) if rec["Nightly Rate"] else None
    else:
        rec["Nightly Rate"] = rec["Nights Count"] = rec["Nightly Total"] = None
    m = re.search(r'Κόστος καθαρισμού\s*€\s*([\d\.,]+)', text)
    rec["Cleaning Fee"] = _parse_price(m.group(1)) if m else None
    m = re.search(r'Φόροι χρήσης ακινήτου\s*€\s*([\d\.,]+)', text)
    rec["Property Tax"] = _parse_price(m.group(1)) if m else None
    m = re.search(r'ΣΎΝΟΛΟ\s*\(EUR\)\s*€\s*([\d\.,]+)', text)
    rec["Guest Paid Total"] = _parse_price(m.group(1)) if m else None
    m = re.search(r'ΚΕΡΔΊΖΕΤΕ\s*€\s*([\d\.,]+)', text)
    rec["Host Payout"] = _parse_price(m.group(1)) if m else None
    m = re.search(r'Προμήθεια υπηρεσιών οικοδεσπότη.*?(-?€\s*[\d\.,]+)', text)
    if m:
        val = m.group(1).replace("€", " ").replace(" ", "").replace(",", ".").replace("—", "-").replace("–", "-")
        try:
            rec["Host Fee"] = abs(float(val))
        except:
            rec["Host Fee"] = None
    else:
        rec["Host Fee"] = None

    year_hint = None
    if rec.get("Email Sent Date"):
        ym = re.search(r'\b(\d{4})\b', rec["Email Sent Date"])
        year_hint = int(ym.group(1)) if ym else None

    rec["Check-in Date Converted"]  = _convert_date(rec.get("Check-in Date"),  year_hint)
    rec["Check-out Date Converted"] = _convert_date(rec.get("Check-out Date"), year_hint)
    rec["Check-in Time Converted"]  = _convert_time(rec.get("Check-in Time"))
    rec["Check-out Time Converted"] = _convert_time(rec.get("Check-out Time"))

    if rec["Status"] == "Canceled" and rec.get("Confirmation Code"):
        rec["Canceled Reservation Code"] = rec["Confirmation Code"]
        mc = re.search(r'(\d{1,2})\s*[–\-]\s*(\d{1,2})\s+([Α-Ωα-ω]{2,})', text)
        if mc:
            rec["Canceled Start Day"] = int(mc.group(1))
            rec["Canceled End Day"]   = int(mc.group(2))
            rec["Canceled Month"]     = mc.group(3).strip()
            rec["Canceled Year"]      = year_hint
        else:
            rec["Canceled Start Day"] = rec["Canceled End Day"] = rec["Canceled Month"] = None
            rec["Canceled Year"] = year_hint
    else:
        rec["Canceled Reservation Code"] = rec["Canceled Start Day"] = None
        rec["Canceled End Day"] = rec["Canceled Month"] = rec["Canceled Year"] = None

    return rec

# ═════════════════════════════════════════════════════════════════════════════
# 5) Loop emails → parse → DataFrame
# ═════════════════════════════════════════════════════════════════════════════

BOOKING_KEYWORDS = [
    "κράτησ", "Κράτησ", "reservation", "Reservation",
    "άφιξη", "Άφιξη", "επιβεβαίωση", "Επιβεβαίωση",
    "υπενθύμιση", "Υπενθύμιση", "φθάνει", "φτάνει", "καταφθάνει",
    "ακυρώθηκε", "Ακυρώθηκε", "ακύρωση", "Ακύρωση", "cancel"
]

all_records = []
skipped = errors = 0
print(f"\n⚙️  Επεξεργασία {len(msgs)} emails...\n{'─'*90}")

for i, m in enumerate(msgs):
    msg_id = m["id"]
    try:
        meta = service.users().messages().get(
            userId="me", id=msg_id,
            format="metadata", metadataHeaders=["Subject", "Date"]
        ).execute()
        subject = next((h["value"] for h in meta["payload"]["headers"] if h["name"]=="Subject"), "")
        date    = next((h["value"] for h in meta["payload"]["headers"] if h["name"]=="Date"), "")
        if not any(kw in subject for kw in BOOKING_KEYWORDS):
            skipped += 1
            continue
        body = get_airbnb_email_body(service, msg_id)
        full_text = f"DATE: {date}\nSUBJECT: {subject}\n\n{body}"
        record = parse_airbnb_email(full_text)
        record["_gmail_id"] = msg_id
        if record.get("Confirmation Code"):
            all_records.append(record)
            status_icon = "❌" if record["Status"] == "Canceled" else "✅"
            print(f"  {status_icon} #{i:>3}  [{record['Status']:9}]  {record['Confirmation Code']:12}  "
                  f"{str(record.get('Check-in Date','?')):10} → {str(record.get('Check-out Date','?')):10}  "
                  f"{str(record.get('Listing','?'))[:25]}")
        else:
            skipped += 1
    except Exception as e:
        errors += 1
        print(f"  ⚠️  #{i} (id={msg_id}): {e}")

print(f"\n{'─'*90}")
print(f"✅ Κρατήσεις : {len(all_records)}  |  ⏭️  Παραλείφθηκαν : {skipped}  |  ❌ Σφάλματα : {errors}\n")

# ═════════════════════════════════════════════════════════════════════════════
# 6) Transformations
# ═════════════════════════════════════════════════════════════════════════════

df = pd.DataFrame(all_records)

if df.empty:
    print("⚠️  Δεν βρέθηκαν κρατήσεις. Τέλος.")
    exit(0)

def extract_year_from_email_date(email_date_str):
    if email_date_str:
        match = re.search(r'\b(\d{4})\b', email_date_str)
        if match:
            return int(match.group(1))
    return datetime.now().year

df['email_year_hint'] = df['Email Sent Date'].apply(extract_year_from_email_date)
df['Check-in Date Datetime']  = df.apply(lambda row: _convert_date(row['Check-in Date'],  row['email_year_hint']), axis=1)
df['Check-out Date Datetime'] = df.apply(lambda row: _convert_date(row['Check-out Date'], row['email_year_hint']), axis=1)

listing_order = ['KOSTAS ACROPOLIS STUDIO 1', 'KOSTAS ACROPOLIS STUDIO 2']
df['Listing'] = pd.Categorical(df['Listing'], categories=listing_order, ordered=True)

df_final_sorted = df.sort_values(
    by=['Listing', 'Check-in Date Datetime', 'Check-out Date Datetime'],
    ascending=[True, True, True]
).reset_index(drop=True)

cols_to_drop = ['Status', 'CategoryValue', 'Host Fee', 'Canceled Reservation Code', 'email_year_hint', 'Canceled Year']
df_final_sorted = df_final_sorted.drop(columns=[c for c in cols_to_drop if c in df_final_sorted.columns], errors='ignore')

def get_new_status(subject):
    if subject and ('Υπενθύμιση κράτησης' in subject or 'Η κράτηση έχει επιβεβαιωθεί' in subject):
        return 'Confirmed'
    return 'Canceled'

df_final_sorted['New_Status'] = df_final_sorted['Subject'].apply(get_new_status)
df_final_sorted['Real_Host_Payout'] = df_final_sorted['Host Payout'] - df_final_sorted['Property Tax']

# ═════════════════════════════════════════════════════════════════════════════
# 7) Upload στο Snowflake
# ═════════════════════════════════════════════════════════════════════════════

df_up = df_final_sorted.copy()
df_up = df_up.sort_values(
    by=['Check-in Date Datetime', 'Check-out Date Datetime'], ascending=True
).reset_index(drop=True)

df_up.columns = (
    df_up.columns
    .str.upper()
    .str.replace(' ', '_', regex=False)
    .str.replace('-', '_', regex=False)
)

for col in df_up.select_dtypes(include=['datetime64']).columns:
    df_up[col] = df_up[col].astype(str).replace('NaT', None)

print("\n📤 Upload στο Snowflake...")
conn = snowflake.connector.connect(
    account=SNOWFLAKE_ACCOUNT, user=SNOWFLAKE_USER,
    password=SNOWFLAKE_PASSWORD, database=SNOWFLAKE_DB,
    schema=SNOWFLAKE_SCHEMA, warehouse=SNOWFLAKE_WH
)
cur = conn.cursor()

try:
    cur.execute(f'CREATE TEMPORARY TABLE TEMP_RESERVATIONS LIKE {SNOWFLAKE_TABLE}')
    write_pandas(conn, df_up, 'TEMP_RESERVATIONS',
                 database=SNOWFLAKE_DB, schema=SNOWFLAKE_SCHEMA,
                 auto_create_table=False, overwrite=True)

    cols     = ', '.join(df_up.columns)
    src_cols = ', '.join([f'src.{c}' for c in df_up.columns])
    cur.execute(f'''
        MERGE INTO {SNOWFLAKE_TABLE} tgt
        USING TEMP_RESERVATIONS src
          ON tgt.CONFIRMATION_CODE = src.CONFIRMATION_CODE
        WHEN MATCHED THEN UPDATE SET
            tgt.NEW_STATUS       = src.NEW_STATUS,
            tgt.REAL_HOST_PAYOUT = src.REAL_HOST_PAYOUT,
            tgt.HOST_PAYOUT      = src.HOST_PAYOUT,
            tgt._LOADED_AT       = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT ({cols}, _LOADED_AT)
            VALUES ({src_cols}, CURRENT_TIMESTAMP())
    ''')
    r = cur.fetchone()
    print(f'✅ MERGE ολοκληρώθηκε!')
    print(f'   📥 Νέες κρατήσεις : {r[0]}')
    print(f'   🔄 Updated        : {r[1]}')
finally:
    cur.close()
    conn.close()

print("\n🎉 Sync ολοκληρώθηκε επιτυχώς!")
