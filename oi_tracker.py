import requests
import gspread
from google.oauth2.service_account import Credentials
from apscheduler.schedulers.blocking import BlockingScheduler

# ── 1) Google Sheets setup ────────────────────────────────────────
SCOPES   = ["https://www.googleapis.com/auth/spreadsheets"]
creds    = Credentials.from_service_account_file("service-account.json", scopes=SCOPES)
gc       = gspread.authorize(creds)
SHEET_ID = "1B6q7ssbPzkXNCm73edXR8lpHm9aTco6URwMGhERZe-E"
sheet    = gc.open_by_key(SHEET_ID).sheet1

# ── 2) HTTP session & headers ─────────────────────────────────────
BASE_URL = "https://www.nseindia.com"
HEADERS  = {
    "User-Agent":      "Mozilla/5.0",
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "en-IN,en;q=0.9",
    "Referer":         BASE_URL,
    "X-Requested-With":"XMLHttpRequest"
}

session = requests.Session()
# Prime cookies
session.get(BASE_URL, headers=HEADERS, timeout=5)

# ── 3) Constants ───────────────────────────────────────────────────
STEP = 50  # strike intervals

# ── 4) Helpers ────────────────────────────────────────────────────
def get_symbols():
    """Fetch Nifty 50 symbols."""
    url = f"{BASE_URL}/api/equity-stockIndices"
    resp = session.get(url, headers=HEADERS, params={"index": "NIFTY 50"}, timeout=5)
    resp.raise_for_status()
    data = resp.json()
    return [item["symbol"] for item in data.get("data", [])]

def fetch_price(symbol):
    """Fetch LTP for a given stock symbol."""
    url = f"{BASE_URL}/api/quote-equity"
    resp = session.get(url, headers=HEADERS, params={"symbol": symbol}, timeout=5)
    resp.raise_for_status()
    data = resp.json()
    return data.get("priceInfo", {}).get("lastPrice", 0)

def fetch_option_chain(symbol):
    """Fetch full option chain JSON for a symbol."""
    url = f"{BASE_URL}/api/option-chain-equities"
    resp = session.get(url, headers=HEADERS, params={"symbol": symbol}, timeout=5)
    resp.raise_for_status()
    data = resp.json()
    return data["records"]["data"]

def compute_oi_changes(chain, atm):
    """Extract ΔOI(Call) and ΔOI(Put) at the ATM strike."""
    rec = next((r for r in chain if r["strikePrice"] == atm), {})
    return (
        rec.get("CE", {}).get("changeinOpenInterest", 0),
        rec.get("PE", {}).get("changeinOpenInterest", 0),
    )

def write_col(a1, vals):
    """
    Write a flat list `vals` into the sheet starting at cell `a1` downward.
    E.g. write_col("A2", symbols_list)
    """
    start_col = a1[0]
    start_row = int(a1[1:])
    end_row   = start_row + len(vals) - 1
    cell_range = f"{start_col}{start_row}:{start_col}{end_row}"
    cells = sheet.range(cell_range)
    for cell, v in zip(cells, vals):
        cell.value = v
    sheet.update_cells(cells)

# ── 5) Jobs ────────────────────────────────────────────────────────
def reset_all():
    symbols = get_symbols()
    write_col("A2", symbols)

    atms = [ int(round(fetch_price(s) / STEP) * STEP) for s in symbols ]
    write_col("B2", atms)

def update_oi():
    symbols = [c.value for c in sheet.range("A2:A51")]
    atms     = [int(c.value or 0) for c in sheet.range("B2:B51")]

    calls, puts = [], []
    for sym, atm in zip(symbols, atms):
        chain = fetch_option_chain(sym)
        co, po = compute_oi_changes(chain, atm)
        calls.append(co)
        puts.append(po)

    write_col("C2", calls)
    write_col("D2", puts)

def init():
    reset_all()
    update_oi()

# ── 6) Scheduler ───────────────────────────────────────────────────
if __name__ == "__main__":
    init()

    sched = BlockingScheduler(timezone="Asia/Kolkata")
    # Daily reset at 09:15 IST
    sched.add_job(reset_all, "cron", hour=9, minute=15)
    # Every 5 minutes after 09:15
    sched.add_job(update_oi, "interval", minutes=5, start_date="2025-04-24 09:15:00")
    sched.start()
