import requests
import gspread
from google.oauth2.service_account import Credentials

# ── 1) Google Sheets setup ────────────────────────────────────────
SCOPES   = ["https://www.googleapis.com/auth/spreadsheets"]
creds    = Credentials.from_service_account_file("service-account.json", scopes=SCOPES)
gc       = gspread.authorize(creds)
SHEET_ID = "1B6q7ssbPzkXNCm73edXR8lpHm9aTco6URwMGhERZe-E"
sheet    = gc.open_by_key(SHEET_ID).sheet1

# ── 2) Constants ───────────────────────────────────────────────────
STEP = 50  # most stock strikes are in ₹50 steps

# ── 3) Helpers ────────────────────────────────────────────────────
def get_symbols():
    """
    Fetch the current Nifty 50 constituent symbols from NSE.
    """
    headers = {
        "User-Agent":      "Mozilla/5.0",
        "Accept-Language": "en-IN,en;q=0.9",
        "Referer":         "https://www.nseindia.com/"
    }

    session = requests.Session()
    session.get("https://www.nseindia.com", headers=headers, timeout=5)

    url  = "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050"
    resp = session.get(url, headers=headers, timeout=5)
    resp.raise_for_status()

    data = resp.json()
    return [item["symbol"] for item in data.get("data", [])]

def fetch_price(symbol):
    """
    Fetches the last traded price for a given NSE equity symbol.
    """
    headers = {
        "User-Agent":      "Mozilla/5.0",
        "Accept-Language": "en-IN,en;q=0.9",
        "Referer":         "https://www.nseindia.com/"
    }

    session = requests.Session()
    session.get("https://www.nseindia.com", headers=headers, timeout=5)

    url  = f"https://www.nseindia.com/api/quote-equity?symbol={symbol}"
    resp = session.get(url, headers=headers, timeout=5)
    resp.raise_for_status()

    data = resp.json()
    return data.get("priceInfo", {}).get("lastPrice", 0)

def fetch_option_chain(symbol):
    """
    Fetches the option chain for a given NSE equity symbol.
    """
    headers = {
        "User-Agent":      "Mozilla/5.0",
        "Accept-Language": "en-IN,en;q=0.9",
        "Referer":         "https://www.nseindia.com/"
    }

    session = requests.Session()
    session.get("https://www.nseindia.com", headers=headers, timeout=5)

    url  = f"https://www.nseindia.com/api/option-chain-equities?symbol={symbol}"
    resp = session.get(url, headers=headers, timeout=5)
    resp.raise_for_status()

    data = resp.json()
    return data["records"]["data"]

def compute_oi_changes(chain, atm):
    """
    From the full option chain JSON, pick out the change in OI
    for CE and PE at the exact ATM strike.
    """
    rec = next((r for r in chain if r["strikePrice"] == atm), {})
    return (
        rec.get("CE", {}).get("changeinOpenInterest", 0),
        rec.get("PE", {}).get("changeinOpenInterest", 0),
    )

def write_col(a1, vals):
    """
    Write a flat list of vals into the sheet starting at cell a1 downwards.
    e.g. write_col("A2", ["REL", "TCS", ...])
    """
    end_row = int(a1[1:]) + len(vals) - 1
    rng      = f"{a1}:{a1[0]}{end_row}"
    cells    = sheet.range(rng)
    for cell, v in zip(cells, vals):
        cell.value = v
    sheet.update_cells(cells)

# ── 4) Job definitions ─────────────────────────────────────────────
def reset_all():
    symbols = get_symbols()
    write_col("A2", symbols)

    atms = [int(round(fetch_price(s) / STEP) * STEP) for s in symbols]
    write_col("B2", atms)

def update_oi():
    syms = [c.value            for c in sheet.range("A2:A51")]
    atms = [int(c.value or 0)  for c in sheet.range("B2:B51")]

    calls, puts = [], []
    for sym, atm in zip(syms, atms):
        chain = fetch_option_chain(sym)
        co, po = compute_oi_changes(chain, atm)
        calls.append(co)
        puts.append(po)

    write_col("C2", calls)
    write_col("D2", puts)

def init():
    reset_all()
    update_oi()

# ── 5) Scheduler ───────────────────────────────────────────────────
if __name__ == "__main__":
    init()

    from apscheduler.schedulers.blocking import BlockingScheduler
    sched = BlockingScheduler(timezone="Asia/Kolkata")

    # 1) every day at 09:15 IST → reset symbols & ATM strikes
    sched.add_job(reset_all, "cron", hour=9, minute=15)

    # 2) every 5 minutes thereafter → update ΔOI
    sched.add_job(
        update_oi,
        "interval",
        minutes=5,
        start_date="2025-04-24 09:15:00",
    )

    sched.start()
