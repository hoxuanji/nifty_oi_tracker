import requests
import gspread
from google.oauth2.service_account import Credentials
from apscheduler.schedulers.blocking import BlockingScheduler

# 1) Google Sheets setup
SCOPES   = ["https://www.googleapis.com/auth/spreadsheets"]
creds    = Credentials.from_service_account_file("service-account.json", scopes=SCOPES)
gc       = gspread.authorize(creds)
SHEET_ID = "1B6q7ssbPzkXNCm73edXR8lpHm9aTco6URwMGhERZe-E"
sheet    = gc.open_by_key(SHEET_ID).sheet1

# 2) HTTP session & headers
BASE_URL = "https://www.nseindia.com"
HEADERS  = {
    "Host":             "www.nseindia.com",
    "Connection":       "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent":       "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/114.0.0.0 Safari/537.36",
    "Accept":           "text/html,application/xhtml+xml,application/xml;"
                        "q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language":  "en-IN,en;q=0.9",
    "Accept-Encoding":  "gzip, deflate, br",
    "Referer":          BASE_URL,
    "Origin":           BASE_URL,
    "TE":               "trailers",
    "X-Requested-With": "XMLHttpRequest"
}

session = requests.Session()
# 2a) Prime cookies: home page
session.get(BASE_URL, headers=HEADERS, timeout=5)
# 2b) Prime cookies: live equity market page
session.get(f"{BASE_URL}/market-data/live-equity-market", headers=HEADERS, timeout=5)

# 3) Constants
STEP = 50  # strike interval

# 4) Helpers
def get_symbols():
    """Fetch the current Nifty 50 constituent symbols."""
    url  = f"{BASE_URL}/api/equity-stockIndices"
    resp = session.get(url, headers=HEADERS, params={"index": "NIFTY 50"}, timeout=5)
    resp.raise_for_status()
    data = resp.json()
    return [item["symbol"] for item in data.get("data", [])]

def fetch_price(symbol):
    """Fetch LTP for a given equity symbol."""
    url  = f"{BASE_URL}/api/quote-equity"
    resp = session.get(url, headers=HEADERS, params={"symbol": symbol}, timeout=5)
    resp.raise_for_status()
    data = resp.json()
    return data.get("priceInfo", {}).get("lastPrice", 0)

def fetch_option_chain(symbol):
    """Fetch full option-chain JSON for a given symbol."""
    url  = f"{BASE_URL}/api/option-chain-equities"
    resp = session.get(url, headers=HEADERS, params={"symbol": symbol}, timeout=5)
    resp.raise_for_status()
    data = resp.json()
    return data["records"]["data"]

def compute_oi_changes(chain, atm):
    """Extract ΔOI(Call) & ΔOI(Put) at the ATM strike."""
    rec = next((r for r in chain if r["strikePrice"] == atm), {})
    return (
        rec.get("CE", {}).get("changeinOpenInterest", 0),
        rec.get("PE", {}).get("changeinOpenInterest", 0)
    )

def write_col(a1, vals):
    """
    Write a flat list `vals` into the sheet starting at cell `a1` downward.
    e.g. write_col("A2", symbols_list)
    """
    col      = a1[0]
    start_row= int(a1[1:])
    end_row  = start_row + len(vals) - 1
    cell_rng = f"{col}{start_row}:{col}{end_row}"
    cells    = sheet.range(cell_rng)
    for cell, v in zip(cells, vals):
        cell.value = v
    sheet.update_cells(cells)

# 5) Jobs
def reset_all():
    symbols = get_symbols()
    write_col("A2", symbols)

    atms = [int(round(fetch_price(sym) / STEP) * STEP) for sym in symbols]
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

# 6) Scheduler
if __name__ == "__main__":
    init()

    sched = BlockingScheduler(timezone="Asia/Kolkata")
    # daily reset at 09:15 IST
    sched.add_job(reset_all, "cron", hour=9, minute=15)
    # update OI every 5 min thereafter
    sched.add_job(update_oi, "interval", minutes=5,
                  start_date="2025-04-24 09:15:00")
    sched.start()
