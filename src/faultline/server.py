"""Fault-injecting mock of an OpenAI-style chat endpoint.

Every request is assigned a fault by a seeded RNG keyed on (seed, arrival number), so a run's fault
sequence is reproducible. Capacity is modelled explicitly: `slots` workers each need `service_s`
seconds per request, and requests queue without a limit. A client that gives up does NOT cancel the
server's work (the handler keeps going), which is how overloaded real servers waste capacity.
Configure with POST /_config, read counters with GET /_stats, restart the clock with POST /_reset.
"""
import asyncio, json, random, time
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse

FAULT_ORDER = ["http429", "http500", "hang", "truncate", "garbage"]
DEFAULT = dict(rates={}, service_s=0.04, slots=1, hang_s=10.0, retry_after_s=0.2,
               outage_start=None, outage_end=None, seed=0)

app = FastAPI()
S = {}


def choose_fault(seed, n, rates):
    """Deterministic fault for arrival number n. Rates are probabilities, applied in FAULT_ORDER."""
    r = random.Random(f"{seed}:{n}").random()
    acc = 0.0
    for f in FAULT_ORDER:
        acc += rates.get(f, 0.0)
        if r < acc:
            return f
    return "ok"


def reset(cfg=None):
    S.clear()
    S.update(DEFAULT)
    S.update(cfg or {})
    S.update(n=0, t0=time.monotonic(), sem=asyncio.Semaphore(S["slots"]), arrivals={}, served=0, faults={})


@app.on_event("startup")
async def _startup():
    reset()


@app.post("/_config")
async def config(req: Request):
    reset(await req.json())
    return {"ok": True}


@app.post("/_reset")
async def _reset():
    reset({k: S[k] for k in DEFAULT})
    return {"ok": True}


@app.get("/_stats")
async def stats():
    return {"arrivals_per_s": S["arrivals"], "received": S["n"], "served": S["served"], "faults": S["faults"]}


def body():
    return {"choices": [{"message": {"role": "assistant", "content": json.dumps({"answer": 42})}}]}


@app.post("/v1/chat/completions")
async def chat():
    now = time.monotonic() - S["t0"]
    n = S["n"]; S["n"] += 1
    sec = int(now)
    S["arrivals"][sec] = S["arrivals"].get(sec, 0) + 1
    fault = choose_fault(S["seed"], n, S["rates"])
    S["faults"][fault] = S["faults"].get(fault, 0) + 1
    if fault == "http429":     # front-door rate limiter: cheap, no capacity used
        return JSONResponse({"error": "rate limited"}, 429, headers={"Retry-After": str(S["retry_after_s"])})
    if fault == "hang":
        await asyncio.sleep(S["hang_s"])
    async with S["sem"]:
        await asyncio.sleep(S["service_s"])
    S["served"] += 1
    t = time.monotonic() - S["t0"]
    if S["outage_start"] is not None and S["outage_start"] <= now < S["outage_end"]:
        return JSONResponse({"error": "outage"}, 500)   # decided at arrival time, after consuming capacity
    if fault == "http500":
        return JSONResponse({"error": "internal"}, 500)
    if fault == "garbage":
        return JSONResponse({"choices": [{"message": {"role": "assistant", "content": "Sure! {answer: forty-two"}}]})
    if fault == "truncate":
        full = json.dumps(body())

        async def gen():
            yield full[: len(full) // 2]
            raise RuntimeError("connection dropped mid-body")
        return StreamingResponse(gen(), media_type="application/json")
    return JSONResponse(body())
