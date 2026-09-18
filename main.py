import os
import json
import time
import threading
import requests

try:
    import websocket
except ImportError:
    import subprocess
    subprocess.check_call( )
    import websocket

ALPACA_KEY = os.environ.get("ALPACA_KEY", "")
ALPACA_SECRET = os.environ.get("ALPACA_SECRET", "")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "")

WS_URL = "wss://stream.data.alpaca.markets/v1beta1/news"

CATALYST_KEYWORDS = [
    "upgrade", "downgrade", "upgraded", "downgraded",
    "price target", "initiated", "reiterated",
    "contract", "agreement", "partnership", "acquisition",
    "merger", "lawsuit", "settlement", "sec charges",
    "earnings", "guidance", "beat", "miss",
    "fda", "approval", "trial", "recall",
    "ceo", "resign", "appointed", "fired",
    "dividend", "buyback", "offering", "dilution",
]

def is_catalyst(text):
    t = (text or "").lower()
    return any(k in t for k in CATALYST_KEYWORDS)

def send_telegram(headline, summary, symbol, source, url, ts):
    if not WEBHOOK_URL:
        print("No webhook url, skip:", headline)
        return
    payload = {
        "headline": headline,
        "summary": summary,
        "symbol": symbol,
        "source": source,
        "url": url,
        "timestamp": ts,
    }
    try:
        r = requests.post(WEBHOOK_URL, json=payload, timeout=10)
        print("Webhook status:", r.status_code)
    except Exception as e:
        print("Webhook error:", e)

def on_message(ws, message):
    try:
        data = json.loads(message)
    except Exception:
        return
    if isinstance(data, list):
        for item in data:
            handle_item(item)
    else:
        handle_item(data)

def handle_item(item):
    if not isinstance(item, dict):
        return
    msg_type = item.get("T") or item.get("type")
    if msg_type and msg_type != "n":
        return
    headline = item.get("headline") or item.get("title") or ""
    summary = item.get("summary") or item.get("content") or ""
    symbol = item.get("symbol") or item.get("symbols") or ""
    source = item.get("source") or "alpaca"
    url = item.get("url") or ""
    ts = item.get("created_at") or item.get("timestamp") or ""
    text = headline + " " + summary
    if is_catalyst(text):
        print("CATALYST:", symbol, headline)
        send_telegram(headline, summary, symbol, source, url, ts)
    else:
        print("Skip (not catalyst):", headline[:80])

def on_open(ws):
    print("Connected, authenticating...")
    ws.send(json.dumps({
        "action": "auth",
        "key": ALPACA_KEY,
        "secret": ALPACA_SECRET,
    }))

def on_error(ws, error):
    print("WS error:", error)

def on_close(ws, code, msg):
    print("WS closed:", code, msg)

def run():
    while True:
        ws = websocket.WebSocketApp(
            WS_URL,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )
        ws.run_forever()
        print("Reconnecting in 5s...")
        time.sleep(5)

if __name__ == "__main__":
    if not ALPACA_KEY or not ALPACA_SECRET:
        print("Set ALPACA_KEY and ALPACA_SECRET env vars")
    else:
        t = threading.Thread(target=run, daemon=True)
        t.start()
        while True:
            time.sleep(60)
