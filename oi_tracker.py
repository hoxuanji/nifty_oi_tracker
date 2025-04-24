import time, requests
from nsetools import Nse
import gspread
from google.oauth2.service_account import Credentials

# ── 1) Google Sheets setup ────────────────────────────────────────
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
creds  = Credentials.from_service_account_file('service-account.json', scopes=SCOPES)
gc     = gspread.authorize(creds)
SHEET_ID = "B6q7ssbPzkXNCm73edXR8lpHm9aTco6URwMGhERZe-E"
sheet    = gc.open_by_key(SHEET_ID).sheet1

# ── 2) NSE clients & constants ────────────────────────────────────
nse    = Nse()
STEP   = 50  # most stock strikes are in ₹50 steps

# ── 3) Helpers ────────────────────────────────────────────────────
def get_symbols():
    return nse.get_stocks_in_index("NIFTY 50")

def fetch_price(symbol):
    # get last traded price
    quote = nse.get_quote(symbol)               # :contentReference[oaicite:1]{index=1}
    return quote['lastPrice']

def fetch_option_chain(symbol):
    # needs the NSE website cookies/headers
    headers = {
        'User-Agent': 'Mozilla/5.0',
        'Accept-Language': 'en_IN,en;q=0.9',
    }
    s = requests.Session()
    s.get("https://www.nseindia.com", headers=headers)  # prime cookies
    url = f"https://www.nseindia.com/api/option-chain-equities?symbol={symbol}"
    data = s.get(url, headers=headers).json()
    return data['records']['data']

def compute_oi_changes(chain, atm):
    # find the record matching strikePrice==atm
    rec = next((r for r in chain if r['strikePrice']==atm), {})
    return (
      rec.get('CE',{}).get('changeinOpenInterest', 0),
      rec.get('PE',{}).get('changeinOpenInterest', 0),
    )

def write_col(a1, vals):
    """Write a flat list of vals into the sheet starting at a1 downwards."""
    cells = sheet.range(f"{a1}:{a1[0]}{int(a1[1:]) + len(vals) - 1}")
    for cell, v in zip(cells, vals):
        cell.value = v
    sheet.update_cells(cells)

# ── 4) Jobs ────────────────────────────────────────────────────────
def reset_all():
    symbols = get_symbols()
    # 1) write symbols
    write_col('A2', symbols)

    # 2) compute & write ATM strikes
    atms = [ int(round(fetch_price(s)/STEP)*STEP) for s in symbols ]
    write_col('B2', atms)

def update_oi():
    # read symbols & atms
    syms = [c.value for c in sheet.range('A2:A51')]
    atms = [int(c.value) for c in sheet.range('B2:B51')]

    calls, puts = [], []
    for sym, atm in zip(syms, atms):
        chain = fetch_option_chain(sym)
        co, po = compute_oi_changes(chain, atm)
        calls.append(co)
        puts .append(po)

    write_col('C2', calls)
    write_col('D2', puts)

def init():
    reset_all()
    update_oi()

# ── 5) Scheduler ───────────────────────────────────────────────────
if __name__ == "__main__":
    init()

    from apscheduler.schedulers.blocking import BlockingScheduler
    sched = BlockingScheduler(timezone="Asia/Kolkata")

    # reset every morning at 9:15 AM IST
    sched.add_job(reset_all, 'cron', hour=9, minute=15)

    # update OI changes every 5 minutes thereafter
    sched.add_job(update_oi, 'interval', minutes=5, start_date='2025-04-24 09:15:00')

    sched.start()
