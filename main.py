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
    "bulls and bears",
    "couldn't stop buzzing",
    "couldnt stop buzzing",
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


