"""
Traffic generator / load tester for the load-balanced demo website.

    python traffic.py                                  # 200 req, conc 20 -> http://localhost:8080
    python traffic.py --url http://<VM-IP>:8080 -n 1000 -c 50
    python traffic.py --rps 30 -n 600                  # steady 30 req/s for 20 s
    python traffic.py --watch 5                        # repeat every 5 s (live load)

Prints how the load balancer distributed requests across replicas
(from the X-Instance response header), status codes per replica,
latency percentiles and throughput. Saves JSON to results/.
"""

import argparse
import asyncio
import json
import os
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "multi-agent"))

from antfarm.traffic import generate_traffic  # noqa: E402


def show(r: dict):
    print(f"\n{r['requests']} requests -> {r['target']}  (concurrency {r['concurrency']})")
    print(f"  wall {r['wall_time_s']}s | {r['throughput_rps']} req/s | success {r['success_rate_pct']}%")
    l = r["latency_ms"]
    print(f"  latency p50 {l['p50']}ms  p95 {l['p95']}ms  p99 {l['p99']}ms  max {l['max']}ms")
    print("  load-balancer distribution:")
    tot = r["requests"]
    for inst, n in r["by_instance"].items():
        bar = "#" * int(40 * n / tot)
        codes = {k.split(":")[1]: v for k, v in r["by_instance_status"].items() if k.startswith(inst + ":")}
        print(f"    {inst:<12} {n:>5} ({100 * n / tot:5.1f}%) {bar}  {codes}")


async def main(a):
    os.makedirs(os.path.join(HERE, "results"), exist_ok=True)
    while True:
        r = await generate_traffic(a.url.rstrip("/"), a.n, a.c, rps=a.rps)
        show(r)
        path = os.path.join(HERE, "results", f"traffic_{time.strftime('%Y%m%d_%H%M%S')}.json")
        with open(path, "w") as f:
            json.dump(r, f, indent=2)
        if not a.watch:
            print(f"\nsaved {path}")
            break
        await asyncio.sleep(a.watch)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default="http://localhost:8080")
    p.add_argument("-n", type=int, default=200, help="total requests")
    p.add_argument("-c", type=int, default=20, help="concurrency")
    p.add_argument("--rps", type=float, default=None, help="pace to a steady request rate")
    p.add_argument("--watch", type=float, default=0, help="repeat every N seconds")
    asyncio.run(main(p.parse_args()))
