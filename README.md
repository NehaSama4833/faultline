# Faultline

A seeded, fault-injecting mock of an OpenAI-style chat endpoint, plus four client retry policies and two experiments: (1) each fault type in isolation, (2) a **retry storm**: a short total outage against a server running below capacity, and what each policy does afterwards.

**Status: Day 7 MVP.** Mock server only, one machine (2-vCPU sandbox), 3 seeds for the storm, 1 seed for the single-fault table. Nothing was run against a real LLM API.

## Problem and what exists
Retry guidance is well known: exponential backoff with jitter, honour `Retry-After`, cap retries with a budget (AWS Well-Architected REL05-BP03; the retry-budget idea from Finagle/gRPC). I did not invent any of it. What I wanted was a runnable, deterministic way to *see* the effects on an LLM-style client, including a failure mode (silent garbage output) that status codes do not reveal.

## Pieces
- `server.py`: faults chosen by a seeded RNG per arrival number (`http429` with `Retry-After`, `http500`, `hang`, `truncate` = body cut mid-transfer, `garbage` = HTTP 200 with unparseable model output). Capacity is explicit: `slots` workers x `service_s`. The queue is unbounded and **work for a request continues after its client gives up** (tested), which is what makes overload self-reinforcing. A configurable outage window returns 500 after consuming capacity.
- `policies.py`: `no_retry`; `immediate_x4` (4 attempts, no delay, ignores Retry-After); `backoff_x4` (full-jitter exponential backoff, honours Retry-After); `backoff_x4+budget` (same, plus retries capped at 10% of requests + 10). All use a 1 s client timeout; all except `no_retry` validate that the model output parses (`bad_output` is retried).
- `experiment.py`, `report.py`. A fresh server process per run so one run's backlog cannot leak into the next.

## Run
```bash
python3 -m venv .venv && .venv/bin/pip install -e '.[dev]'
.venv/bin/pytest -q                                              # 15 tests
.venv/bin/python -m faultline.experiment --seeds 3 --duration 30 # about 13 min
.venv/bin/python -m faultline.report results/experiment.json
```

## Results (`results/experiment_table.txt`)

**1. One fault type at a time, 30% of requests, 200 requests, 8 workers** (final success / mean attempts per request / p99 latency in s):

| fault | no_retry | immediate_x4 | backoff_x4 | backoff_x4+budget |
|---|---|---|---|---|
| 429 | 0.700 / 1.00 / 0.33 | 0.995 / 1.42 / 0.33 | 1.000 / 1.43 / 0.41 | 0.810 / 1.15 / 0.37 |
| 500 | 0.700 / 1.00 / 0.33 | 0.990 / 1.42 / 1.31 | 0.990 / 1.42 / 1.47 | 0.810 / 1.15 / 0.74 |
| hang (3 s) | 0.700 / 1.00 / 1.00 | 0.990 / 1.42 / 3.17 | 0.995 / 1.42 / 3.51 | 0.810 / 1.15 / 2.15 |
| truncated body | 0.700 / 1.00 / 0.34 | 0.990 / 1.42 / 1.31 | 0.995 / 1.42 / 1.48 | 0.810 / 1.15 / 0.75 |
| garbage output | **1.000** / 1.00 / 0.33 | 0.990 / 1.42 / 1.31 | 0.990 / 1.42 / 1.48 | 0.810 / 1.15 / 0.75 |

- **The `no_retry` garbage cell is misleading by design.** It reports 100% because it does not check the output; the true usable rate is about 70%. Status codes alone hid a 30% failure.
- Retrying recovers about 99% of requests at a cost of about 1.4 attempts per request, for every fault type.
- Hangs are expensive: a timeout of 1 s per failed attempt puts p99 at 3+ s even though the eventual success is high.
- **The budget policy trades success for protection:** at a sustained 30% fault rate it exhausts its retry allowance and ends at 0.81. That is the design (it assumes faults are rare); it is not free.

**2. Retry storm** (Poisson 16 req/s offered, server capacity 25 req/s, 1 s timeout, total outage from t=5 s to t=9 s, 30 s run, 3 seeds):

| policy | overall success | attempts reaching server per request | peak arrivals/s | recovered after outage? |
|---|---|---|---|---|
| no_retry | 0.84-0.89 | 1.00 | 22-27 | yes, immediately |
| immediate_x4 | 0.13-0.17 | 3.5-3.6 | 73-80 | **no** (0% success from t=5 s to end of run) |
| backoff_x4 | 0.13-0.17 | 3.5-3.6 | 73-80 | **no** |
| backoff_x4+budget | 0.84-0.89 | 1.05-1.06 | 29-34 | yes, immediately |

During the outage every request fails, so each retrying client sends all 4 attempts: offered attempts jump to about 64/s, far above the 25/s capacity. A queue forms, each queued request takes longer than the 1 s timeout, clients retry, and the server spends its capacity on work nobody is waiting for. When the outage ends the load stays above capacity, so the server never catches up within the 21 s that remain. `no_retry` is hurt only during the 4 outage seconds. The budget policy matches `no_retry` after the outage, with ~5% extra requests.

**Backoff alone did not help here.** `backoff_x4` and `immediate_x4` finished with identical counts in every seed, and the arithmetic explains why: every request after the outage fails all four attempts either way, so the number of attempts is the same; delays only shift when they arrive. Backoff spreads load; it does not reduce it. Only the budget removed attempts.

## Limitations (read these)
- **The storm is partly an artifact of my modelling choices:** one server worker, an unbounded queue, no load shedding, no client-side concurrency limit, work continuing after the client disconnects, a 1 s timeout against a 0.04 s service time. A server that sheds load, cancels abandoned work, or a client with a circuit breaker would change the result a lot. These are the standard remedies; I did not test them.
- **"Did not recover" means not within the remaining 21 s of the run.** I did not run longer, so I cannot say it would never recover. Not a measured metastable-failure threshold; I did not sweep offered load or capacity.
- **Single-fault table is one seed, 200 requests per cell** (about 3 points of standard error on success rates). Differences of about 0.01 between immediate and backoff are noise.
- Backoff parameters (base 0.1 s, cap 2 s, 4 attempts, 10% budget) were picked once, not tuned. The server's `Retry-After` is a flat 0.05-0.2 s.
- Non-streaming only; no real token streaming faults. Hang and truncation behaviour is what this server and httpx do, not necessarily real API gateways.
- A localhost server in a 2-vCPU sandbox shared with the load generator: timings are rough.

## Future work
Sweep offered load and timeout; add load shedding, cancellation of abandoned work and a circuit breaker; streaming faults; hedged requests; validate against a real endpoint with a dry-run key (not done, no key).
