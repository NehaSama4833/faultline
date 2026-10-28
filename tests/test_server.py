import asyncio, time
import httpx, pytest
from collections import Counter
from faultline.server import choose_fault
from faultline.harness import ServerProc

def test_fault_sequence_is_deterministic_and_matches_rates():
    rates = {"http429": 0.1, "http500": 0.2, "garbage": 0.05}
    a = [choose_fault(5, i, rates) for i in range(20000)]
    assert a == [choose_fault(5, i, rates) for i in range(20000)]
    assert a != [choose_fault(6, i, rates) for i in range(20000)]
    c = Counter(a)
    for k, r in rates.items(): assert abs(c[k] / 20000 - r) < 0.01
    assert abs(c["ok"] / 20000 - 0.65) < 0.01

@pytest.fixture(scope="module")
def srv():
    with ServerProc() as s: yield s

def post(srv, **kw): return httpx.post(srv.url + "/v1/chat/completions", json={}, **kw)

def test_healthy_server_returns_json_content(srv):
    srv.config(rates={}, service_s=0.01)
    r = post(srv); assert r.status_code == 200 and r.json()["choices"][0]["message"]["content"] == '{"answer": 42}'

def test_each_fault_behaves_as_documented(srv):
    srv.config(rates={"http429": 1.0}, retry_after_s=0.3); r = post(srv)
    assert r.status_code == 429 and r.headers["retry-after"] == "0.3"
    srv.config(rates={"http500": 1.0}, service_s=0.01); assert post(srv).status_code == 500
    srv.config(rates={"garbage": 1.0}, service_s=0.01); r = post(srv)
    assert r.status_code == 200 and "forty-two" in r.text
    srv.config(rates={"truncate": 1.0}, service_s=0.01)
    with pytest.raises(httpx.TransportError): post(srv)
    srv.config(rates={"hang": 1.0}, hang_s=2.0, service_s=0.01)
    with pytest.raises(httpx.ReadTimeout): post(srv, timeout=0.3)

def test_outage_window_returns_500_then_recovers(srv):
    srv.config(rates={}, service_s=0.01, outage_start=0.0, outage_end=1.0)
    assert post(srv).status_code == 500
    time.sleep(1.1); assert post(srv).status_code == 200

def test_abandoned_requests_still_consume_server_capacity():
  with ServerProc() as srv:   # own server: leftover handlers from other tests must not count
      srv.config(rates={}, service_s=0.2, slots=1)
      async def go():
          async with httpx.AsyncClient(base_url=srv.url) as c:
              await asyncio.gather(*[c.post("/v1/chat/completions", json={}, timeout=0.05) for _ in range(5)], return_exceptions=True)
      asyncio.run(go())
      time.sleep(1.4)    # 5 x 0.2 s of work was queued even though every client gave up after 0.05 s
      assert srv.stats()["served"] == 5
