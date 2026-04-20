import random
import uuid
from datetime import datetime, timezone


_METHODS = random.choices(
    ["GET", "POST", "PUT", "DELETE", "PATCH"],
    weights=[60, 20, 10, 5, 5],
    k=1,
)[0:0]  # just declare weights below in generate()

_METHOD_WEIGHTS = [60, 20, 10, 5, 5]
_METHODS_LIST = ["GET", "POST", "PUT", "DELETE", "PATCH"]

_PATH_TEMPLATES = [
    "/api/products/{id}",
    "/api/users/{id}",
    "/api/orders/{id}",
    "/api/cart/{id}",
    "/api/search",
    "/api/recommendations",
    "/api/reviews/{id}",
    "/",
    "/health",
    "/api/auth/login",
]

_STATUS_BY_METHOD = {
    "GET":    (([200] * 75 + [304] * 5 + [404] * 12 + [401] * 3 + [500] * 5)),
    "POST":   (([201] * 60 + [400] * 20 + [422] * 10 + [401] * 5 + [500] * 5)),
    "PUT":    (([200] * 65 + [400] * 15 + [404] * 10 + [401] * 5 + [500] * 5)),
    "DELETE": (([204] * 65 + [404] * 20 + [401] * 10 + [500] * 5)),
    "PATCH":  (([200] * 65 + [400] * 15 + [404] * 10 + [401] * 5 + [500] * 5)),
}

_LATENCY_BY_STATUS_CLASS = {
    2: (10, 200),
    3: (1, 10),
    4: (5, 50),
    5: (100, 2000),
}

_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.1",
    "Googlebot/2.1 (+http://www.google.com/bot.html)",
]


def generate() -> dict:
    method = random.choices(_METHODS_LIST, weights=_METHOD_WEIGHTS, k=1)[0]
    template = random.choice(_PATH_TEMPLATES)
    path = template.replace("{id}", str(random.randint(1, 9999)))
    status_code = random.choice(_STATUS_BY_METHOD[method])
    status_class = status_code // 100
    latency_lo, latency_hi = _LATENCY_BY_STATUS_CLASS[status_class]
    latency_ms = random.randint(latency_lo, latency_hi)
    ip = f"203.0.113.{random.randint(1, 254)}"
    user_agent = random.choice(_USER_AGENTS)
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

    return {
        "request_id": str(uuid.uuid4()),
        "method": method,
        "path": path,
        "status_code": status_code,
        "latency_ms": latency_ms,
        "ip": ip,
        "user_agent": user_agent,
        "ts": ts,
    }
