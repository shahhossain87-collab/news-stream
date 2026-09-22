import os
import re
import json
import time
from datetime import datetime, timezone
from urllib.parse import urlparse

import requests
import websocket


ALPACA_KEY = os.environ.get("ALPACA_KEY", "").strip()
ALPACA_SECRET = os.environ.get("ALPACA_SECRET", "").strip()
WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "").strip()

MAX_AGE_SEC = int(os.environ.get("MAX_AGE_SEC", "3600"))
ALLOW_MISSING_TIMESTAMP = (
    os.environ.get("ALLOW_MISSING_TIMESTAMP", "false").strip().lower() == "true"
)
TEST_SEND_ALL = (
    os.environ.get("TEST_SEND_ALL", "false").strip().lower() == "true"
)

# V3 decision-compression mode. When true, low-priority/indirect/follow-up items
# are still logged but are not forwarded to the downstream webhook.
ACTIONABLE_ONLY = (
    os.environ.get("ACTIONABLE_ONLY", "false").strip().lower() == "true"
)

WS_URL = "wss://stream.data.alpaca.markets/v1beta1/news"


HIGH = [
    "fda approves",
    "fda approved",
    "fda approval",
    "fda rejects",
    "fda rejected",
    "complete response letter",
    "phase 3 results",
    "phase iii results",
    "phase 3 met",
    "phase iii met",
    "phase 3 failed",
    "phase iii failed",
    "primary endpoint met",
    "primary endpoint failed",
    "sec charges",
    "sec charged",
    "doj charges",
    "doj charged",
    "criminal charges",
    "to acquire",
    "agrees to acquire",
    "acquisition of",
    "merger agreement",
    "definitive merger agreement",
    "takeover offer",
    "wins contract",
    "won contract",
    "awarded contract",
    "contract award",
    "selected for contract",
    "raises guidance",
    "raised guidance",
    "cuts guidance",
    "cut guidance",
    "lowers guidance",
    "lowered guidance",
    "withdraws guidance",
    "withdrawn guidance",
    "beats estimates",
    "beat estimates",
    "misses estimates",
    "missed estimates",
    "earnings beat",
    "earnings miss",
    "ceo resigns",
    "ceo resigned",
    "ceo steps down",
    "appointed ceo",
    "names new ceo",
    "files for bankruptcy",
    "filed for bankruptcy",
    "bankruptcy filing",
    "going concern warning",
    "trading halted",
    "trading halt",
    "tokenization launch",
    "tokenized shares",
    "tokenized securities",
    "launches 24/7 trading",
]

MEDIUM = [
    "upgraded to",
    "downgraded to",
    "raises price target",
    "raised price target",
    "cuts price target",
    "cut price target",
    "initiates coverage",
    "initiated coverage",
    "price target",
    "partnership",
    "strategic partnership",
    "strategic agreement",
    "collaboration agreement",
    "lawsuit",
    "settlement",
    "class action",
    "buyback",
    "share repurchase",
    "secondary offering",
    "dilutive offering",
    "dilution",
    "dividend increase",
    "raises dividend",
    "dividend cut",
    "cuts dividend",
    "product recall",
    "recall",
    "warning letter",
]

LOW = [
    "upgrade",
    "downgrade",
    "upgraded",
    "downgraded",
    "reiterated",
    "maintains",
    "dividend",
    "offering",
]

RECAP = [
    # Clear recap / recycled-content patterns. These are dropped only when
    # no stronger HIGH catalyst phrase is present in the same item.
    "what you need to know",
    "stocks to watch",
    "market wrap",
    "closing bell",
    "premarket wrap",
    "top stories",
    "best stocks",
    "as previously reported",
    "as we reported",
    "recap:",
    "week in review",
    "daily wrap",
    "morning roundup",
    "afternoon roundup",
    "bulls and bears",
    "couldn't stop buzzing",
    "couldnt stop buzzing",
    "complete transcript",
    "earnings call transcript",
    "conference call transcript",
    "full transcript",
    "market-moving news",
    "why is ",
    "why are ",
]

# Conservative noise filter. These patterns are commentary/calendar items, not
# fresh company catalysts. Strong HIGH catalyst phrases still override them.
NOISE = [
    "earnings scheduled for",
    "jim cramer",
    "kevin o'leary",
    "kevin o’leary",
    "mad money",
    "shark tank",
]

# Repeated macro soundbites can arrive as many separate headlines within minutes.
# Keep the first one, then suppress near-term repeats for the same symbol/topic.
MACRO_REPEAT_SYMBOLS = {"SPY", "QQQ", "DIA", "IWM"}
MACRO_REPEAT_PHRASES = [
    "fed",
    "fomc",
    "goolsbee",
    "powell",
    "inflation",
    "interest rate",
    "rate cuts",
    "rate cut",
]

# V3 decision-compression layers. These do not replace the original catalyst
# classifier; they qualify whether an item deserves trader attention.
ANALYST_ROUTINE = [
    "maintains ", "reiterates ", "reiterated ", "price target",
]
ANALYST_STRONG = [
    "upgrades ", "upgraded to", "downgrades ", "downgraded to",
    "initiates coverage", "initiated coverage",
]
INDIRECT_OR_SECTOR = [
    "shares of software companies", "shares of semiconductor companies",
    "shares of crypto-linked companies", "shares of electronic equipment",
    "amid overall market strength", "broader rally", "sector strength",
    "indirectly support", "indirectly benefit", "peer in",
]
RISK_ALERT_PHRASES = [
    "accounting investigation", "administrative leave", "delisting notice",
    "non-compliance notice", "going concern", "files for bankruptcy",
    "filed for bankruptcy", "bankruptcy filing", "restatement",
    "sec charges", "doj charges", "criminal charges",
]

STOPWORDS = {
    "the", "a", "an", "and", "or", "of", "to", "for", "in", "on",
    "with", "after", "as", "at", "by", "from", "inc", "corp",
    "corporation", "ltd", "co", "plc", "update", "says", "said",
}


class TtlSet:
    def __init__(self, ttl=3600, maxlen=4000):
        self.ttl = ttl
        self.maxlen = maxlen
        self.data = {}

    def cleanup(self):
        now = time.time()
        cutoff = now - self.ttl
        expired = [key for key, timestamp in self.data.items() if timestamp <= cutoff]
        for key in expired:
            self.data.pop(key, None)
        if len(self.data) > self.maxlen:
            oldest = sorted(self.data.items(), key=lambda x: x[1])
            remove_count = len(self.data) - self.maxlen
            for key, _ in oldest[:remove_count]:
                self.data.pop(key, None)

    def add_if_new(self, key):
        if not key:
            return True
        now = time.time()
        old = self.data.get(key)
        if old is not None and (now - old) < self.ttl:
            return False
        self.data[key] = now
        if len(self.data) > self.maxlen:
            self.cleanup()
        return True


seen_ids = TtlSet(ttl=6 * 3600, maxlen=6000)
seen_events = TtlSet(ttl=3 * 3600, maxlen=5000)
seen_macro = TtlSet(ttl=15 * 60, maxlen=500)

# Lightweight event-family memory. Same-symbol/same-family headlines are marked
# as FOLLOW_UP instead of being mistaken for independent trade ideas.
event_clusters = {}
EVENT_CLUSTER_TTL = 2 * 3600


def as_symbol(value):
    if isinstance(value, list):
        parts = [str(x).upper().strip() for x in value if x]
        return ",".join(parts)
    if value:
        return str(value).upper().strip()
    return ""


def primary_symbol(value):
    text = as_symbol(value)
    return text.split(",")[0] if text else ""


def parse_ts(value):
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def age_seconds(created_at):
    dt = parse_ts(created_at)
    if not dt:
        return None
    return (datetime.now(timezone.utc) - dt).total_seconds()


def norm_headline(text):
    text = (text or "").lower()
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    words = [word for word in text.split() if word and word not in STOPWORDS]
    return " ".join(words[:12])


def fingerprint(symbol, headline, event_type):
    return "|".join([
        primary_symbol(symbol),
        event_type or "unknown",
        norm_headline(headline),
    ])


def match_any(text, phrases):
    for phrase in phrases:
        if phrase in text:
            return phrase
    return None


def classify(headline, summary):
    text = f"{headline or ''} {summary or ''}".lower()
    recap_hit = match_any(text, RECAP)
    noise_hit = match_any(text, NOISE)
    high_hit = match_any(text, HIGH)
    medium_hit = match_any(text, MEDIUM)
    low_hit = match_any(text, LOW)

    # A known strong catalyst always wins, even if commentary/recap wording is
    # present in the same item. This protects recall for genuinely material news.
    if high_hit:
        return "RESEARCH", "HIGH", high_hit

    # Conservative suppression: only obvious recap, calendar and personality
    # commentary noise is dropped. Unknown company-specific wording still passes.
    if recap_hit:
        return "DROP", "LOW", recap_hit
    if noise_hit:
        return "DROP", "LOW", noise_hit

    if medium_hit:
        return "WATCH", "MEDIUM", medium_hit
    if low_hit:
        return "WATCH", "LOW", low_hit

    return "WATCH", "LOW", "unclassified_candidate"


def is_macro_repeat(symbols, headline, summary):
    symbol = primary_symbol(symbols)
    if symbol not in MACRO_REPEAT_SYMBOLS:
        return False
    text = f"{headline or ''} {summary or ''}".lower()
    topic = match_any(text, MACRO_REPEAT_PHRASES)
    if not topic:
        return False
    # Group all Fed/inflation soundbites for the same broad-market symbol into
    # a 15-minute bucket. The first gets through; follow-ups are logged only.
    key = f"macro:{symbol}:fed_inflation"
    return not seen_macro.add_if_new(key)


def event_family(headline, summary, matched=""):
    text = f"{headline or ''} {summary or ''} {matched or ''}".lower()
    families = [
        ("fda_clinical", ["fda", "phase 3", "phase iii", "primary endpoint", "clinical trial"]),
        ("guidance", ["guidance", "outlook", "forecast"]),
        ("earnings", ["eps", "revenue", "sales", "earnings", "quarter"]),
        ("mna", ["acquire", "acquisition", "merger", "take-private", "takeover", "hsr"]),
        ("analyst", ["price target", "upgraded", "downgraded", "initiates coverage", "maintains", "reiterates"]),
        ("contract", ["contract", "purchase agreement", "power purchase agreement", "awarded"]),
        ("legal_regulatory", ["lawsuit", "settlement", "attorney general", "sec ", "doj ", "nasdaq", "delisting"]),
        ("financing", ["offering", "convertible", "equity facility", "dilution", "buyback", "repurchase", "dividend"]),
        ("management", ["ceo", "cfo", "coo", "president", "resigns", "appointed"]),
        ("insider", ["insider", "form 4", "chairman bought", "ceo purchased", "cfo purchased"]),
        ("macro", ["fed", "fomc", "inflation", "interest rate", "white house", "president trump", "bank of canada"]),
        ("product", ["launch", "product", "demonstrate", "presentation", "abstract"]),
    ]
    for family, phrases in families:
        if any(p in text for p in phrases):
            return family
    return "other"


def event_cluster_status(symbols, family):
    symbol = primary_symbol(symbols)
    if not symbol:
        return "NEW_EVENT"
    now = time.time()
    # purge old cluster timestamps opportunistically
    expired = [k for k, ts in event_clusters.items() if (now - ts) >= EVENT_CLUSTER_TTL]
    for key in expired:
        event_clusters.pop(key, None)
    key = f"{symbol}|{family}"
    old = event_clusters.get(key)
    event_clusters[key] = now
    return "FOLLOW_UP" if old is not None and (now - old) < EVENT_CLUSTER_TTL else "NEW_EVENT"


def freshness_bucket(age):
    if age is None:
        return "UNKNOWN"
    if age <= 5 * 60:
        return "FRESH"
    if age <= 15 * 60:
        return "EARLY"
    if age <= 60 * 60:
        return "AGING"
    return "STALE"


def directness_bucket(symbols, headline, summary):
    symbol = primary_symbol(symbols)
    text = f"{headline or ''} {summary or ''}".lower()
    if symbol in MACRO_REPEAT_SYMBOLS:
        return "MARKET_MACRO"
    if any(p in text for p in INDIRECT_OR_SECTOR):
        return "INDIRECT_OR_SECTOR"
    # The news feed does not provide a reliable company-name-to-ticker mapping, so
    # avoid claiming certainty. Downstream research can upgrade this to DIRECT.
    return "LIKELY_DIRECT"


def analyst_signal_quality(headline, summary):
    text = f"{headline or ''} {summary or ''}".lower()
    if any(p in text for p in ANALYST_STRONG):
        return "STRONG_CHANGE"
    if any(p in text for p in ANALYST_ROUTINE):
        return "ROUTINE_OR_PT_ONLY"
    return "NOT_ANALYST"


def actionability(route, impact, family, event_status, freshness, directness, analyst_quality, headline, summary):
    text = f"{headline or ''} {summary or ''}".lower()

    if route == "DROP":
        return "SUPPRESS", "explicit_noise_or_recap"

    if any(p in text for p in RISK_ALERT_PHRASES):
        return "RISK_ALERT", "material_negative_or_governance_risk"

    if directness == "INDIRECT_OR_SECTOR":
        return "NOT_DIRECT", "ticker_linkage_is_indirect_or_sector_level"

    if family == "analyst" and analyst_quality == "ROUTINE_OR_PT_ONLY":
        return "LOW_PRIORITY", "routine_analyst_reiteration_or_pt_change"

    if event_status == "FOLLOW_UP" and impact != "HIGH":
        return "UPDATE_ONLY", "same_symbol_same_event_family_recently_seen"

    if directness == "MARKET_MACRO":
        return "MACRO_CONTEXT", "broad_market_context_not_single_stock_entry"

    if impact == "HIGH" and freshness in ("FRESH", "EARLY"):
        return "VALIDATE_NOW", "fresh_high_impact_catalyst"

    if impact == "HIGH":
        return "RESEARCH_DEEP", "high_impact_but_not_fresh"

    if impact == "MEDIUM" and freshness in ("FRESH", "EARLY"):
        return "WATCH_SETUP", "fresh_medium_impact_candidate"

    if route == "RESEARCH":
        return "RESEARCH_DEEP", "research_candidate"

    return "LOW_PRIORITY", "insufficient_edge_for_immediate_swing_action"


def should_forward_action(action):
    if not ACTIONABLE_ONLY:
        return action != "SUPPRESS"
    return action in {"VALIDATE_NOW", "WATCH_SETUP", "RESEARCH_DEEP", "RISK_ALERT", "MACRO_CONTEXT"}


def send_webhook(payload):
    if not WEBHOOK_URL:
        print("NO_WEBHOOK:", payload.get("headline", "")[:90])
        return False
    try:
        response = requests.post(WEBHOOK_URL, json=payload, timeout=8)
        response.raise_for_status()
        print(
            "WEBHOOK_OK",
            response.status_code,
            payload.get("route"),
            payload.get("primary_symbol"),
            payload.get("headline", "")[:70],
        )
        return True
    except requests.RequestException as exc:
        print("WEBHOOK_FAIL:", exc)
        return False


def handle_item(ws, item):
    if not isinstance(item, dict):
        return

    kind = item.get("T") or item.get("type") or ""
    msg = item.get("msg") or ""

    if kind == "success":
        print("SERVER_SUCCESS:", msg)
        if msg == "authenticated":
            ws.send(json.dumps({
                "action": "subscribe",
                "news": ["*"],
            }))
            print("SUBSCRIBE_SENT")
        return

    if kind == "subscription":
        print("SUBSCRIBED:", item)
        return

    if kind == "error":
        print("SERVER_ERROR:", item)
        return

    if kind and kind != "n":
        return

    headline = item.get("headline") or item.get("title") or ""
    if not headline:
        return

    news_id = str(item.get("id") or "")
    created_at = item.get("created_at") or ""
    symbols = item.get("symbols") or item.get("symbol")
    summary = item.get("summary") or item.get("content") or ""
    source = item.get("source") or "alpaca"
    url = item.get("url") or ""

    age = age_seconds(created_at)
    if age is None:
        if not ALLOW_MISSING_TIMESTAMP:
            print("DROP_NO_TIMESTAMP:", headline[:90])
            return
    else:
        if age < -60:
            print("CLOCK_WARNING:", int(age), headline[:90])
        if age > MAX_AGE_SEC and not TEST_SEND_ALL:
            print("STALE:", int(age), "sec:", headline[:90])
            return

    if news_id and not seen_ids.add_if_new("id:" + news_id):
        print("DUP_ID:", headline[:90])
        return

    route, impact, matched = classify(headline, summary)
    family = event_family(headline, summary, matched)
    event_status = event_cluster_status(symbols, family)
    freshness = freshness_bucket(age)
    directness = directness_bucket(symbols, headline, summary)
    analyst_quality = analyst_signal_quality(headline, summary)
    action, action_reason = actionability(
        route, impact, family, event_status, freshness, directness,
        analyst_quality, headline, summary
    )

    if route != "RESEARCH" and is_macro_repeat(symbols, headline, summary):
        print("DROP_MACRO_REPEAT:", primary_symbol(symbols), headline[:90])
        return

    event_key = fingerprint(symbols, headline, matched or "none")
    if not seen_events.add_if_new(event_key):
        print("DUP_EVENT:", headline[:90])
        return

    if route == "DROP":
        print("DROP:", headline[:90])

    payload = {
        "schema_version": "3.0",
        "route": route,
        "impact": impact,
        "matched": matched,
        "headline": headline,
        "summary": (summary or "")[:500],
        "symbol": as_symbol(symbols),
        "primary_symbol": primary_symbol(symbols),
        "source": source,
        "url": url,
        "timestamp": created_at,
        "received_at": datetime.now(timezone.utc).isoformat(),
        "age_sec": int(age) if age is not None else None,
        "news_id": news_id,
        "event_fingerprint": event_key,
        "event_family": family,
        "event_status": event_status,
        "freshness": freshness,
        "directness": directness,
        "analyst_signal_quality": analyst_quality,
        "actionability": action,
        "action_reason": action_reason,
        # These fields are deliberately not fabricated here. A downstream market-data
        # step should calculate actual price movement from catalyst time before any entry.
        "price_move_since_catalyst_pct": None,
        "price_check_required": action in {"VALIDATE_NOW", "WATCH_SETUP", "RESEARCH_DEEP"},
        "swing_rule": "GOOD_NEWS_IS_NOT_AN_ENTRY; validate price reaction and setup before trade",
        "host": urlparse(url).netloc if url else "",
        "test_send_all": TEST_SEND_ALL,
        "actionable_only": ACTIONABLE_ONLY,
    }

    print(route, impact, payload["primary_symbol"], "|", action, "|", family, "|", headline)

    # V3 gate: by default preserve backward-compatible recall. Set ACTIONABLE_ONLY=true
    # to forward only compressed, trader-relevant candidates.
    forward_candidate = route in ("RESEARCH", "WATCH") and should_forward_action(action)

    if TEST_SEND_ALL or forward_candidate:
        send_webhook(payload)
    else:
        print("QUALIFIED_OUT:", action, payload["primary_symbol"], headline[:90])


def on_message(ws, message):
    try:
        data = json.loads(message)
    except Exception as exc:
        print("BAD_JSON:", exc)
        return
    if isinstance(data, list):
        for item in data:
            handle_item(ws, item)
    else:
        handle_item(ws, data)


def on_open(ws):
    print("CONNECTED: credentials sent in WebSocket headers")


def on_error(ws, error):
    print("WS_ERROR:", error)


def on_close(ws, close_code, close_msg):
    print("WS_CLOSED:", close_code, close_msg)


def run():
    reconnect_delay = 2
    while True:
        try:
            headers = [
                f"APCA-API-KEY-ID: {ALPACA_KEY}",
                f"APCA-API-SECRET-KEY: {ALPACA_SECRET}",
            ]
            ws = websocket.WebSocketApp(
                WS_URL,
                header=headers,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )
            ws.run_forever(ping_interval=30, ping_timeout=10)
        except Exception as exc:
            print("WS_FATAL:", exc)
        print(f"Reconnecting in {reconnect_delay}s...")
        time.sleep(reconnect_delay)
        reconnect_delay = min(reconnect_delay * 2, 30)


if __name__ == "__main__":
    print("Starting Fast Catalyst Alpaca Stream V3.0 Decision-Compression")
    print("MAX_AGE_SEC =", MAX_AGE_SEC)
    print("TEST_SEND_ALL =", TEST_SEND_ALL)
    print("ACTIONABLE_ONLY =", ACTIONABLE_ONLY)
    print("ALPACA_KEY loaded:", bool(ALPACA_KEY), "length:", len(ALPACA_KEY))
    print("ALPACA_SECRET loaded:", bool(ALPACA_SECRET), "length:", len(ALPACA_SECRET))
    if not ALPACA_KEY or not ALPACA_SECRET:
        raise SystemExit("Missing ALPACA_KEY or ALPACA_SECRET")
    if not WEBHOOK_URL:
        print("WARNING: WEBHOOK_URL is not configured")
    run()
