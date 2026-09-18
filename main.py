import os
import re
import json
import time
import sys
import subprocess
from datetime import datetime, timezone
from urllib.parse import urlparse

import requests

try:
    import websocket
except ImportError:
    subprocess.check_call([
        sys.executable,
        "-m",
        "pip",
        "install",
        "websocket-client"
    ])
    import websocket


ALPACA_KEY = os.environ.get("ALPACA_KEY", "")
ALPACA_SECRET = os.environ.get("ALPACA_SECRET", "")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "")

MAX_AGE_SEC = int(os.environ.get("MAX_AGE_SEC", "900"))

ALLOW_MISSING_TIMESTAMP = (
    os.environ.get(
        "ALLOW_MISSING_TIMESTAMP",
        "false"
    ).lower() == "true"
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
]


STOPWORDS = {
    "the",
    "a",
    "an",
    "and",
    "or",
    "of",
    "to",
    "for",
    "in",
    "on",
    "with",
    "after",
    "as",
    "at",
    "by",
    "from",
    "inc",
    "corp",
    "corporation",
    "ltd",
    "co",
    "plc",
    "update",
    "says",
    "said",
}


class TtlSet:
    def __init__(self, ttl=3600, maxlen=4000):
        self.ttl = ttl
        self.maxlen = maxlen
        self.data = {}

    def cleanup(self):
        now = time.time()
        cutoff = now - self.ttl

        expired = [
            key
            for key, timestamp in self.data.items()
            if timestamp <= cutoff
        ]

        for key in expired:
            self.data.pop(key, None)

        if len(self.data) > self.maxlen:
            oldest = sorted(
                self.data.items(),
                key=lambda x: x[1]
            )

            remove_count = (
                len(self.data) - self.maxlen
            )

            for key, _ in oldest[:remove_count]:
                self.data.pop(key, None)

    def add_if_new(self, key):
        if not key:
            return True

        now = time.time()
        old = self.data.get(key)

        if (
            old is not None
            and (now - old) < self.ttl
        ):
            return False

        self.data[key] = now

        if len(self.data) > self.maxlen:
            self.cleanup()

        return True


seen_ids = TtlSet(
    ttl=6 * 3600,
    maxlen=6000
)

seen_events = TtlSet(
    ttl=3 * 3600,
    maxlen=5000
)


def as_symbol(value):
    if isinstance(value, list):
        parts = [
            str(x).upper().strip()
            for x in value
            if x
        ]

        return ",".join(parts)

    if value:
        return str(value).upper().strip()

    return ""


def primary_symbol(value):
    text = as_symbol(value)

    return (
        text.split(",")[0]
        if text
        else ""
    )


def parse_ts(value):
    if not value:
        return None

    try:
        dt = datetime.fromisoformat(
            str(value).replace(
                "Z",
                "+00:00"
            )
        )

        if dt.tzinfo is None:
            dt = dt.replace(
                tzinfo=timezone.utc
            )

        return dt

    except Exception:
        return None


def age_seconds(created_at):
    dt = parse_ts(created_at)

    if not dt:
        return None

    now = datetime.now(
        timezone.utc
    )

    return (
        now - dt
    ).total_seconds()


def norm_headline(text):
    text = (
        text or ""
    ).lower()

    text = re.sub(
        r"[^a-z0-9\s]",
        " ",
        text
    )

    words = [
        word
        for word in text.split()
        if (
            word
            and word not in STOPWORDS
        )
    ]

    return " ".join(
        words[:12]
    )


def fingerprint(
    symbol,
    headline,
    event_type
):
    return "|".join([
        primary_symbol(symbol),
        event_type or "unknown",
        norm_headline(headline),
    ])


def match_any(
    text,
    phrases
):
    for phrase in phrases:
        if phrase in text:
            return phrase

    return None


def classify(
    headline,
    summary
):
    text = (
        f"{headline or ''} "
        f"{summary or ''}"
    ).lower()

    recap_hit = match_any(
        text,
        RECAP
    )

    high_hit = match_any(
        text,
        HIGH
    )

    medium_hit = match_any(
        text,
        MEDIUM
    )

    low_hit = match_any(
        text,
        LOW
    )

    if high_hit:
        return (
            "RESEARCH",
            "HIGH",
            high_hit
        )

    if recap_hit:
        return (
            "DROP",
            "LOW",
            recap_hit
        )

    if medium_hit:
        return (
            "WATCH",
            "MEDIUM",
            medium_hit
        )

    if low_hit:
        return (
            "WATCH",
            "LOW",
            low_hit
        )

    return (
        "DROP",
        "LOW",
        None
    )


def send_webhook(payload):
    if not WEBHOOK_URL:
        print(
            "NO_WEBHOOK:",
            payload.get(
                "headline",
                ""
            )[:90]
        )

        return False

    delays = [
        0,
        2,
        5
    ]

    for attempt, delay in enumerate(
        delays,
        start=1
    ):
        if delay:
            time.sleep(delay)

        try:
            response = requests.post(
                WEBHOOK_URL,
                json=payload,
                timeout=10
            )

            response.raise_for_status()

            print(
                "WEBHOOK_OK",
                response.status_code,
                payload.get("route"),
                payload.get(
                    "primary_symbol"
                ),
                payload.get(
                    "headline",
                    ""
                )[:70],
            )

            return True

        except requests.RequestException as exc:
            print(
                f"WEBHOOK_FAIL "
                f"attempt={attempt}/"
                f"{len(delays)}:",
                exc
            )

    print(
        "WEBHOOK_GAVE_UP:",
        payload.get(
            "primary_symbol"
        ),
        payload.get(
            "headline",
            ""
        )[:90]
    )

    return False


def handle_item(
    ws,
    item
):
    if not isinstance(
        item,
        dict
    ):
        return

    kind = (
        item.get("T")
        or item.get("type")
        or ""
    )

    msg = (
        item.get("msg")
        or ""
    )

    if kind == "success":
        print(
            "SERVER_SUCCESS:",
            msg
        )

        if msg == "connected":
            ws.send(
                json.dumps({
                    "action": "auth",
                    "key": ALPACA_KEY,
                    "secret": ALPACA_SECRET,
                })
            )

            return

        if msg == "authenticated":
            ws.send(
                json.dumps({
                    "action": "subscribe",
                    "news": ["*"],
                })
            )

            print(
                "SUBSCRIBE_SENT"
            )

            return

        return

    if kind == "subscription":
        print(
            "SUBSCRIBED:",
            item
        )

        return

    if kind == "error":
        print(
            "SERVER_ERROR:",
            item
        )

        return

    if (
        kind
        and kind != "n"
    ):
        return

    headline = (
        item.get("headline")
        or item.get("title")
        or ""
    )

    if not headline:
        return

    news_id = str(
        item.get("id")
        or ""
    )

    created_at = (
        item.get("created_at")
        or ""
    )

    symbols = (
        item.get("symbols")
        or item.get("symbol")
    )

    summary = (
        item.get("summary")
        or item.get("content")
        or ""
    )

    source = (
        item.get("source")
        or "alpaca"
    )

    url = (
        item.get("url")
        or ""
    )

    age = age_seconds(
        created_at
    )

    if age is None:

        if not ALLOW_MISSING_TIMESTAMP:
            print(
                "DROP_NO_TIMESTAMP:",
                headline[:90]
            )

            return

    else:

        if age < -60:
            print(
                "CLOCK_WARNING:",
                int(age),
                headline[:90]
            )

        if age > MAX_AGE_SEC:
            print(
                "STALE:",
                int(age),
                "sec:",
                headline[:90]
            )

            return

    if news_id:

        is_new_id = (
            seen_ids.add_if_new(
                "id:" + news_id
            )
        )

        if not is_new_id:
            print(
                "DUP_ID:",
                headline[:90]
            )

            return

    route, impact, matched = classify(
        headline,
        summary
    )

    event_key = fingerprint(
        symbols,
        headline,
        matched or "none"
    )

    is_new_event = (
        seen_events.add_if_new(
            event_key
        )
    )

    if not is_new_event:
        print(
            "DUP_EVENT:",
            headline[:90]
        )

        return

    if route == "DROP":
        print(
            "DROP:",
            headline[:90]
        )

        return

    payload = {
        "schema_version": "2.1",

        "route": route,
        "impact": impact,
        "matched": matched,

        "headline": headline,

        "summary": (
            summary[:500]
        ),

        "symbol": as_symbol(
            symbols
        ),

        "primary_symbol": primary_symbol(
            symbols
        ),

        "source": source,
        "url": url,

        "timestamp": created_at,

        "received_at": datetime.now(
            timezone.utc
        ).isoformat(),

        "age_sec": (
            int(age)
            if age is not None
            else None
        ),

        "news_id": news_id,

        "event_fingerprint": event_key,

        "host": (
            urlparse(url).netloc
            if url
            else ""
        ),
    }

    print(
        route,
        impact,
        payload[
            "primary_symbol"
        ],
        "|",
        matched,
        "|",
        headline
    )

    send_webhook(
        payload
    )


def on_message(
    ws,
    message
):
    try:
        data = json.loads(
            message
        )

    except Exception as exc:
        print(
            "BAD_JSON:",
            exc
        )

        return

    if isinstance(
        data,
        list
    ):
        for item in data:
            handle_item(
                ws,
                item
            )

    else:
        handle_item(
            ws,
            data
        )


def on_open(ws):
    print(
        "CONNECTED: waiting for Alpaca connected message"
    )


def on_error(
    ws,
    error
):
    print(
        "WS_ERROR:",
        error
    )


def on_close(
    ws,
    close_code,
    close_msg
):
    print(
        "WS_CLOSED:",
        close_code,
        close_msg
    )


def run():
    reconnect_delay = 2

    while True:

        try:
            ws = websocket.WebSocketApp(
                WS_URL,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )

            ws.run_forever(
                ping_interval=30,
                ping_timeout=10,
            )

        except Exception as exc:
            print(
                "WS_FATAL:",
                exc
            )

        print(
            f"Reconnecting in "
            f"{reconnect_delay}s..."
        )

        time.sleep(
            reconnect_delay
        )

        reconnect_delay = min(
            reconnect_delay * 2,
            30
        )


if __name__ == "__main__":

    print(
        "Starting Fast Catalyst "
        "Alpaca Stream V2.1"
    )

    print(
        "MAX_AGE_SEC =",
        MAX_AGE_SEC
    )

    if (
        not ALPACA_KEY
        or not ALPACA_SECRET
    ):
        raise SystemExit(
            "Missing ALPACA_KEY "
            "or ALPACA_SECRET"
        )

    if not WEBHOOK_URL:
        print(
            "WARNING: WEBHOOK_URL "
            "is not configured"
        )

    run()
