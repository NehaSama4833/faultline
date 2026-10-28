import asyncio, json, os, random, signal, socket, subprocess, sys, time
import httpx

PY = sys.executable


def free_port():
    s = socket.socket(); s.bind(("127.0.0.1", 0)); p = s.getsockname()[1]; s.close(); return p


class ServerProc:
    def __init__(self):
        self.port = free_port()
        self.p = subprocess.Popen([PY, "-m", "uvicorn", "faultline.server:app", "--port", str(self.port), "--log-level", "error"],
                                  stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
        self.url = f"http://127.0.0.1:{self.port}"
        for _ in range(100):
            try:
                httpx.post(self.url + "/_reset", timeout=1); return
            except Exception:
                time.sleep(0.1)
        raise RuntimeError("server did not start")

    def config(self, **cfg):
        httpx.post(self.url + "/_config", json=cfg, timeout=5).raise_for_status()

    def stats(self):
        return httpx.get(self.url + "/_stats", timeout=5).json()

    def close(self):
        os.killpg(self.p.pid, signal.SIGKILL); self.p.wait(5)   # graceful stop would wait for the queued backlog

    def __enter__(self): return self
    def __exit__(self, *a): self.close()


def pct(xs, q):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, max(0, int(-(-q * len(xs) // 1)) - 1))] if xs else float("nan")


async def run_closed(srv, policy, n, conc=8):
    """n logical requests with `conc` workers (used for single-fault matrix)."""
    lim = httpx.Limits(max_connections=200, max_keepalive_connections=50)
    async with httpx.AsyncClient(base_url=srv.url, limits=lim) as c:
        q = asyncio.Queue()
        for _ in range(n): q.put_nowait(1)
        res = []

        async def w():
            while not q.empty():
                q.get_nowait(); res.append(await policy.call(c))
        await asyncio.gather(*[w() for _ in range(conc)])
    return res


async def run_open(srv, policy, rate, duration, seed):
    """Poisson arrivals at `rate`/s for `duration` s, each an independent logical request.
    Returns [(arrival_time, Outcome)] for requests that finished before duration+drain."""
    rng = random.Random(seed)
    lim = httpx.Limits(max_connections=2000, max_keepalive_connections=100)
    out, tasks = [], []
    async with httpx.AsyncClient(base_url=srv.url, limits=lim) as c:
        t0 = time.monotonic(); t = 0.0

        async def one(ta):
            o = await policy.call(c); out.append((ta, o))
        while t < duration:
            t += rng.expovariate(rate)
            await asyncio.sleep(max(0, t0 + t - time.monotonic()))
            tasks.append(asyncio.create_task(one(t)))
        await asyncio.gather(*tasks)
    return out
