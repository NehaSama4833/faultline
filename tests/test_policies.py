import asyncio, json, random
import httpx, pytest
from faultline.policies import Policy, Budget

OK = {"choices": [{"message": {"content": json.dumps({"a": 1})}}]}
BAD = {"choices": [{"message": {"content": "nope {"}}]}

def run(policy, responses):
    """responses: list consumed in order; an Exception instance is raised, a (status, json, headers) tuple returned."""
    calls = []
    def handler(req):
        calls.append(req)
        r = responses[min(len(calls) - 1, len(responses) - 1)]
        if isinstance(r, Exception): raise r
        s, j, h = (r + ({},))[:3] if len(r) < 3 else r
        return httpx.Response(s, json=j, headers=h)
    async def go():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url="http://x") as c:
            return await policy.call(c)
    return asyncio.run(go()), len(calls)

@pytest.fixture(autouse=True)
def fast_sleep(monkeypatch):
    slept = []
    async def s(d): slept.append(d)
    monkeypatch.setattr("faultline.policies.asyncio.sleep", s)
    return slept

def test_success_first_try():
    o, n = run(Policy("p", max_attempts=3), [(200, OK)])
    assert o.ok and o.attempts == 1 and n == 1

def test_retries_5xx_then_succeeds():
    o, n = run(Policy("p", max_attempts=4), [(500, {}), (500, {}), (200, OK)])
    assert o.ok and o.attempts == 3 and n == 3

def test_gives_up_after_max_attempts():
    o, n = run(Policy("p", max_attempts=3), [(500, {})])
    assert not o.ok and n == 3 and o.reason == "http5xx"

def test_4xx_not_retried():
    o, n = run(Policy("p", max_attempts=5), [(400, {})])
    assert not o.ok and n == 1

def test_timeout_and_connection_errors_are_retried():
    o, n = run(Policy("p", max_attempts=3), [httpx.ReadTimeout("t"), httpx.RemoteProtocolError("r"), (200, OK)])
    assert o.ok and n == 3

def test_retry_after_honored_and_ignorable(fast_sleep):
    run(Policy("p", max_attempts=2), [(429, {}, {"retry-after": "0.7"}), (200, OK)])
    assert fast_sleep == [0.7]
    fast_sleep.clear()
    run(Policy("p", max_attempts=2, honor_retry_after=False), [(429, {}, {"retry-after": "0.7"}), (200, OK)])
    assert fast_sleep == [0.0]

def test_expo_jitter_bounds(fast_sleep):
    p = Policy("p", max_attempts=6, backoff="expo_jitter", base=0.1, cap=0.5, rng=random.Random(3))
    run(p, [(500, {})])
    caps = [min(0.5, 0.1 * 2 ** k) for k in range(5)]
    assert len(fast_sleep) == 5 and all(0 <= d <= c for d, c in zip(fast_sleep, caps))
    assert len(set(fast_sleep)) > 1   # jittered, not constant

def test_bad_output_retried_only_with_validation():
    o, n = run(Policy("p", max_attempts=3, validate=True), [(200, BAD), (200, OK)])
    assert o.ok and n == 2
    o, n = run(Policy("p", max_attempts=3, validate=False), [(200, BAD)])
    assert o.ok and n == 1   # "success" on garbage: the silent failure this benchmark reports separately

def test_budget_caps_total_retries():
    b = Budget(ratio=0.1, floor=0)
    for _ in range(20): b.request()
    assert sum(b.try_retry() for _ in range(10)) == 2   # 0.1 * 20

def test_budget_blocks_retry_in_policy():
    p = Policy("p", max_attempts=4, budget_ratio=0.0)
    p._budget.floor = 0
    o, n = run(p, [(500, {})])
    assert not o.ok and n == 1 and "budget" in o.reason
