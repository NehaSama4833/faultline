"""Client-side retry policies. One `call` = one logical request (possibly several HTTP attempts)."""
import asyncio, json, random, time
from dataclasses import dataclass, field
import httpx

URL = "/v1/chat/completions"


@dataclass
class Outcome:
    ok: bool
    attempts: int
    latency: float
    reason: str = ""


class Budget:
    """Retries allowed in total <= ratio * logical requests + floor (Finagle-style ratio budget)."""
    def __init__(self, ratio=0.1, floor=10):
        self.ratio, self.floor, self.requests, self.retries = ratio, floor, 0, 0

    def request(self):
        self.requests += 1

    def try_retry(self):
        if self.retries < self.ratio * self.requests + self.floor:
            self.retries += 1
            return True
        return False


@dataclass
class Policy:
    name: str
    timeout: float = 1.0
    max_attempts: int = 1
    backoff: str = "none"          # none | expo_jitter
    honor_retry_after: bool = True
    validate: bool = False         # treat unparseable model output as retryable
    budget_ratio: float | None = None
    base: float = 0.1
    cap: float = 2.0
    rng: random.Random = field(default_factory=lambda: random.Random(0))
    _budget: Budget | None = None

    def __post_init__(self):
        if self.budget_ratio is not None:
            self._budget = Budget(self.budget_ratio)

    def delay(self, k, retry_after):
        if retry_after is not None and self.honor_retry_after:
            return retry_after
        if self.backoff == "expo_jitter":      # "full jitter": uniform(0, min(cap, base * 2^k))
            return self.rng.uniform(0, min(self.cap, self.base * 2 ** k))
        return 0.0

    async def attempt(self, client):
        """Returns (kind, retry_after). kind in ok | bad_output | http429 | http5xx | http4xx | timeout | conn."""
        try:
            r = await client.post(URL, json={"messages": []}, timeout=self.timeout)
        except httpx.TimeoutException:
            return "timeout", None
        except httpx.TransportError:
            return "conn", None
        if r.status_code == 429:
            return "http429", float(r.headers.get("retry-after", 0) or 0) or None
        if r.status_code >= 500:
            return "http5xx", None
        if r.status_code >= 400:
            return "http4xx", None
        try:
            json.loads(r.json()["choices"][0]["message"]["content"])
            return "ok", None
        except Exception:
            return ("bad_output" if self.validate else "ok"), None   # without validation the caller never notices

    async def call(self, client) -> Outcome:
        t0 = time.monotonic()
        if self._budget: self._budget.request()
        kind = "?"
        for k in range(self.max_attempts):
            kind, ra = await self.attempt(client)
            if kind == "ok":
                return Outcome(True, k + 1, time.monotonic() - t0)
            retryable = kind in ("timeout", "conn", "http429", "http5xx", "bad_output")
            if not retryable or k + 1 >= self.max_attempts:
                break
            if self._budget and not self._budget.try_retry():
                kind += "(budget)"
                break
            await asyncio.sleep(self.delay(k, ra))
        return Outcome(False, k + 1, time.monotonic() - t0, kind)


def standard(rng_seed=0):
    r = lambda: random.Random(rng_seed)
    return [
        Policy("no_retry", timeout=1.0, max_attempts=1, rng=r()),
        Policy("immediate_x4", timeout=1.0, max_attempts=4, backoff="none", honor_retry_after=False, validate=True, rng=r()),
        Policy("backoff_x4", timeout=1.0, max_attempts=4, backoff="expo_jitter", validate=True, rng=r()),
        Policy("backoff_x4+budget", timeout=1.0, max_attempts=4, backoff="expo_jitter", validate=True, budget_ratio=0.1, rng=r()),
    ]
