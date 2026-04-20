import re
from datetime import datetime, timezone

import pytest

import generator  # resolved via pythonpath = ["producer"] in pyproject.toml


VALID_METHODS = {"GET", "POST", "PUT", "DELETE", "PATCH"}
VALID_STATUS_CODES = {200, 201, 204, 304, 400, 401, 404, 422, 500}
VALID_USER_AGENTS = {
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.1",
    "Googlebot/2.1 (+http://www.google.com/bot.html)",
}

_TS_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$")
_IP_RE = re.compile(r"^203\.0\.113\.\d{1,3}$")
_UUID4_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
)


@pytest.mark.unit
class TestSchema:
    def test_returns_dict(self):
        assert isinstance(generator.generate(), dict)

    def test_exactly_eight_fields(self):
        assert set(generator.generate().keys()) == {
            "request_id", "method", "path", "status_code",
            "latency_ms", "ip", "user_agent", "ts",
        }


@pytest.mark.unit
class TestRequestId:
    def test_is_uuid4_string(self):
        rid = generator.generate()["request_id"]
        assert isinstance(rid, str)
        assert _UUID4_RE.match(rid), f"Not a UUIDv4: {rid}"

    def test_unique_across_100_calls(self):
        ids = {generator.generate()["request_id"] for _ in range(100)}
        assert len(ids) == 100


@pytest.mark.unit
class TestMethod:
    def test_valid_method(self):
        for _ in range(50):
            assert generator.generate()["method"] in VALID_METHODS


@pytest.mark.unit
class TestPath:
    def test_starts_with_slash(self):
        for _ in range(20):
            assert generator.generate()["path"].startswith("/")

    def test_no_unreplaced_template_placeholders(self):
        for _ in range(50):
            assert "{id}" not in generator.generate()["path"]


@pytest.mark.unit
class TestStatusCode:
    def test_is_integer(self):
        assert isinstance(generator.generate()["status_code"], int)

    def test_in_known_set(self):
        for _ in range(100):
            sc = generator.generate()["status_code"]
            assert sc in VALID_STATUS_CODES, f"Unexpected status code: {sc}"


@pytest.mark.unit
class TestLatencyMs:
    def test_is_positive_integer(self):
        for _ in range(50):
            lm = generator.generate()["latency_ms"]
            assert isinstance(lm, int)
            assert lm > 0

    def test_5xx_floor_at_100ms(self):
        """5xx latency range starts at 100ms per generator design."""
        samples = []
        for _ in range(500):
            event = generator.generate()
            if event["status_code"] // 100 == 5:
                samples.append(event["latency_ms"])
        if samples:
            assert min(samples) >= 100, f"5xx latency below floor: {min(samples)}ms"

    def test_2xx_ceiling_at_200ms(self):
        """2xx latency range tops out at 200ms per generator design."""
        samples = []
        for _ in range(500):
            event = generator.generate()
            if event["status_code"] // 100 == 2:
                samples.append(event["latency_ms"])
        if samples:
            assert max(samples) <= 200, f"2xx latency above ceiling: {max(samples)}ms"


@pytest.mark.unit
class TestIp:
    def test_is_test_net_range(self):
        """Must be in 203.0.113.0/24 (TEST-NET-3, RFC 5737)."""
        for _ in range(20):
            ip = generator.generate()["ip"]
            assert _IP_RE.match(ip), f"IP outside TEST-NET-3: {ip}"

    def test_last_octet_valid(self):
        for _ in range(50):
            last = int(generator.generate()["ip"].split(".")[-1])
            assert 1 <= last <= 254


@pytest.mark.unit
class TestUserAgent:
    def test_is_known_user_agent(self):
        for _ in range(20):
            assert generator.generate()["user_agent"] in VALID_USER_AGENTS


@pytest.mark.unit
class TestTimestamp:
    def test_format_iso8601_ms_utc(self):
        ts = generator.generate()["ts"]
        assert _TS_RE.match(ts), f"Bad timestamp format: {ts}"

    def test_parses_as_utc(self):
        ts = generator.generate()["ts"]
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        assert dt.tzinfo is not None

    def test_is_recent(self):
        ts = generator.generate()["ts"]
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        delta = abs((datetime.now(timezone.utc) - dt).total_seconds())
        assert delta < 5, f"Timestamp {delta:.1f}s from now — too stale"
