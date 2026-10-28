import argparse, asyncio, json, time

from .harness import ServerProc, run_closed, run_open, pct
from .policies import standard, Policy

RATES_30 = ["http429", "http500", "hang", "truncate", "garbage"]


def matrix(args):
    rows = []
    for fault in RATES_30:
        for pol in standard(1):
            with ServerProc() as srv:      # fresh process per run: no leftover handlers or backlog from the previous run
                srv.config(rates={fault: 0.3}, seed=7, hang_s=3.0, retry_after_s=0.05)   # hang 3 s > 1 s client timeout
                res = asyncio.run(run_closed(srv, pol, args.n))
                lat = [o.latency for o in res]
                rows.append(dict(fault=fault, policy=pol.name, success=sum(o.ok for o in res) / len(res),
                                 attempts=sum(o.attempts for o in res) / len(res), p50=pct(lat, .5), p99=pct(lat, .99)))
                print(rows[-1], flush=True)
    return rows


def storm(args):
    """Open-loop load below capacity; a short total outage (500s, still costing capacity); watch recovery."""
    out = []
    for seed in range(args.seeds):
        for pol in standard(seed):
            with ServerProc() as srv:
                srv.config(rates={}, seed=seed, service_s=0.04, slots=1, outage_start=args.outage[0], outage_end=args.outage[1])
                res = asyncio.run(run_open(srv, pol, args.rate, args.duration, seed))
                st = srv.stats()
                sec = lambda ta: int(ta)
                buckets = {}
                for ta, o in res: buckets.setdefault(sec(ta), []).append(o.ok)
                frac = [(sum(buckets[s]) / len(buckets[s]) if buckets.get(s) else 0.0) for s in range(int(args.duration))]
                rec = None   # first second after outage start+... where success >= 0.9 for 3 consecutive seconds
                for s in range(int(args.outage[1]), int(args.duration) - 2):
                    if all(f >= 0.9 for f in frac[s:s + 3]): rec = s - args.outage[1]; break
                out.append(dict(policy=pol.name, seed=seed, success_frac_per_s=frac, recovery_s=rec,
                                overall_success=sum(o.ok for _, o in res) / len(res),
                                server_received=st["received"], logical=len(res),
                                amplification=st["received"] / len(res), peak_arrivals_per_s=max(st["arrivals_per_s"].values())))
                print({k: v for k, v in out[-1].items() if k != "success_frac_per_s"}, flush=True)
    return out


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=200)
    ap.add_argument("--seeds", type=int, default=3)
    ap.add_argument("--rate", type=float, default=16)
    ap.add_argument("--duration", type=float, default=40)
    ap.add_argument("--outage", type=float, nargs=2, default=[5, 9])
    ap.add_argument("--out", default="results/experiment.json")
    ap.add_argument("--skip-matrix", action="store_true")
    a = ap.parse_args(argv)
    r = {"config": vars(a)}
    if not a.skip_matrix: r["matrix"] = matrix(a)
    r["storm"] = storm(a)
    json.dump(r, open(a.out, "w"), indent=1)


if __name__ == "__main__":
    main()
