"""
A/B benchmark of the Data Forge API across git refs (or running servers).

  # Launch each ref from a throwaway worktree and compare (default: main vs HEAD):
  python scripts/bench_ab.py --refs main HEAD --rows 100000 1000000

  # Or benchmark servers you started yourself, then compare the saved results:
  python scripts/bench_ab.py --url http://localhost:8080 --label before
  python scripts/bench_ab.py --compare bench_results/before.json bench_results/after.json

Timings are client wall-clock (upload + server work + download + Arrow decode),
median of --repeat runs. Endpoints missing on a ref are reported as n/a.
"""
import argparse
import io
import json
import os
import statistics
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import numpy as np
import orjson
import polars as pl
import pyarrow.ipc as ipc
import requests

ARROW = "application/vnd.apache.arrow.stream"
REPO = Path(__file__).resolve().parent.parent


def make_frame(n: int) -> pl.DataFrame:
    rng = np.random.default_rng(42)
    return pl.DataFrame({
        "id": np.arange(n, dtype=np.int64),
        "well_code": rng.integers(0, 5_000, n),
        "oil_kbd": rng.random(n) * 500,
        "gas_mmcfd": rng.random(n) * 100,
        "field": rng.choice([f"FIELD_{i}" for i in range(50)], n),
        "well_ref": [f"WELL_{i:09d}" for i in range(n)],
    })


def payloads(df: pl.DataFrame) -> tuple[bytes, bytes]:
    json_body = orjson.dumps({"data": df.to_dicts(), "compression": "zstd"})
    sink = io.BytesIO()
    df.write_ipc_stream(sink)
    return json_body, sink.getvalue()


def timed(fn, repeat: int):
    """Median wall time of fn(); returns None if the endpoint doesn't exist on this ref."""
    times = []
    for _ in range(repeat):
        start = time.perf_counter()
        if fn() is None:
            return None
        times.append(time.perf_counter() - start)
    return statistics.median(times)


def post(url, body, content_type):
    r = requests.post(url, data=body, headers={"content-type": content_type}, timeout=600)
    if r.status_code in (404, 405, 415, 422):
        return None
    r.raise_for_status()
    return r


def get_table(url, expected_rows=None):
    r = requests.get(url, timeout=600)
    if r.status_code in (404, 405, 422):
        return None
    r.raise_for_status()
    table = ipc.open_stream(r.content).read_all()
    if expected_rows is not None and table.num_rows != expected_rows:
        raise AssertionError(f"{url}: got {table.num_rows} rows, expected {expected_rows}")
    return table


def run_suite(base: str, rows: list[int], repeat: int) -> dict:
    results = {}
    tag = uuid.uuid4().hex[:6]
    for n in rows:
        df = make_frame(n)
        json_body, arrow_body = payloads(df)
        new = lambda kind: f"bench_{kind}_{n}_{tag}_{uuid.uuid4().hex[:6]}"

        for engine in ("polars", "duckdb"):
            results[f"write json  {engine:<7}{n:>9,}"] = (n, timed(
                lambda: post(f"{base}/write/{engine}/{new('w')}", json_body, "application/json"), repeat))
        results[f"write arrow polars {n:>9,}"] = (n, timed(
            lambda: post(f"{base}/write/polars/{new('w')}", arrow_body, ARROW), repeat))

        read_schema = new("r")
        post(f"{base}/write/polars/{read_schema}", json_body, "application/json")
        for engine in ("polars", "arrow", "duckdb"):
            results[f"read        {engine:<7}{n:>9,}"] = (n, timed(
                lambda: get_table(f"{base}/read/{engine}/{read_schema}", n), repeat))

        def parallel_reads():
            with ThreadPoolExecutor(4) as pool:
                tables = list(pool.map(lambda _: get_table(f"{base}/read/arrow/{read_schema}", n), range(4)))
            return None if None in tables else tables
        results[f"read x4 parallel  {n:>9,}"] = (4 * n, timed(parallel_reads, repeat))

        # Event-loop responsiveness: latency of a trivial endpoint while a large write runs.
        latencies, done = [], threading.Event()
        writer = threading.Thread(target=lambda: (
            post(f"{base}/write/polars/{new('w')}", json_body, "application/json"), done.set()))
        writer.start()
        while not done.is_set():
            start = time.perf_counter()
            requests.get(f"{base}/schemas/", timeout=600)
            latencies.append(time.perf_counter() - start)
            time.sleep(0.01)
        writer.join()
        results[f"max latency during write {n:>9,}"] = (None, max(latencies) if latencies else None)
        print(f"  done {n:,} rows", flush=True)
    return results


def wait_ready(base: str, proc, timeout=90):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"server exited with code {proc.returncode}")
        try:
            if requests.get(f"{base}/schemas/", timeout=2).ok:
                return
        except requests.ConnectionError:
            pass
        time.sleep(0.5)
    raise TimeoutError("server did not become ready")


def bench_ref(ref: str, args, out: Path) -> dict:
    with tempfile.TemporaryDirectory(prefix="dataforge_ab_") as tmp:
        tree = Path(tmp) / "tree"
        subprocess.run(["git", "-C", str(REPO), "worktree", "add", "--detach", str(tree), ref],
                       check=True, capture_output=True)
        log = open(out / f"server_{ref.replace('/', '_')}.log", "w")
        env = {**os.environ, "PYTHONPATH": str(tree), "DATAFORGE_PORT": str(args.port)}
        proc = subprocess.Popen([args.python, "-m", "app.main"], cwd=tree, env=env, stdout=log, stderr=log)
        base = f"http://127.0.0.1:{args.port}"
        try:
            wait_ready(base, proc)
            print(f"[{ref}] server up, benchmarking...", flush=True)
            return run_suite(base, args.rows, args.repeat)
        finally:
            proc.terminate()
            proc.wait(timeout=30)
            log.close()
            subprocess.run(["git", "-C", str(REPO), "worktree", "remove", "--force", str(tree)],
                           capture_output=True)


def fmt(v, rows):
    if v is None:
        return "n/a"
    return f"{v:.3f}s" + (f" ({rows / v / 1e6:.2f}M r/s)" if rows else "")


def compare(labels: list[str], runs: list[dict]) -> str:
    keys = list(dict.fromkeys(k for r in runs for k in r))
    header = ["benchmark"] + labels + (["speedup"] if len(runs) == 2 else [])
    lines = ["| " + " | ".join(header) + " |", "|" + "---|" * len(header)]
    for k in keys:
        cells = [fmt(r.get(k, (None, None))[1], r.get(k, (None, None))[0]) for r in runs]
        if len(runs) == 2:
            a, b = (r.get(k, (None, None))[1] for r in runs)
            cells.append(f"{a / b:.1f}x" if a and b else "")
        lines.append("| " + " | ".join([k.strip()] + cells) + " |")
    return "\n".join(lines)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--refs", nargs="+", default=["main", "HEAD"])
    p.add_argument("--url", help="benchmark an already-running server instead of launching refs")
    p.add_argument("--label", default="run", help="result name for --url mode")
    p.add_argument("--compare", nargs="+", metavar="JSON", help="only compare saved result files")
    p.add_argument("--rows", nargs="+", type=int, default=[100_000, 1_000_000])
    p.add_argument("--repeat", type=int, default=3)
    p.add_argument("--port", type=int, default=8080, help="must be 8080 for refs that hardcode it (main)")
    p.add_argument("--python", default=sys.executable, help="interpreter with the project deps installed")
    p.add_argument("--out", type=Path, default=REPO / "bench_results")
    args = p.parse_args()
    args.out.mkdir(exist_ok=True)

    if args.compare:
        runs = [{k: tuple(v) for k, v in json.loads(Path(f).read_text()).items()} for f in args.compare]
        labels = [Path(f).stem for f in args.compare]
    else:
        targets = [(args.label, None)] if args.url else [(ref, ref) for ref in args.refs]
        labels, runs = [], []
        for label, ref in targets:
            res = run_suite(args.url.rstrip("/"), args.rows, args.repeat) if args.url else bench_ref(ref, args, args.out)
            (args.out / f"{label.replace('/', '_')}.json").write_text(json.dumps(res, indent=1))
            labels.append(label)
            runs.append(res)

    table = compare(labels, runs)
    print("\n" + table)
    (args.out / f"compare_{time.strftime('%Y%m%d_%H%M%S')}.md").write_text(table + "\n")


if __name__ == "__main__":
    main()
