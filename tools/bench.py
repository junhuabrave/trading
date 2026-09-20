#!/usr/bin/env python3
"""
bench.py run [--gate] [--write] [--strace] [--only NAME...]

Runs every benchmark binary, collects their JSON lines into build/bench/run.json (with host
class, commit, compiler), and compares against bench/baselines/<host-class>.json:

  ns/op rows:  p50 may grow at most 5 %; p99.9 at most 15 % when the baseline was recorded --pinned
               (on an unpinned laptop p99.9 is scheduler noise and is reported, not gated)
  ops/s rows:  may fall at most 5 %
  allocs:      must not grow
  any row missing from the baseline: reported, and a failure under --gate

--write stores the run as the baseline for this host class (commit it with the reason).
--gate exits 1 on any regression or missing baseline; without it the comparison is informational,
which is how CI on shared runners uses it. --strace (Linux) also runs bench_risk under strace and
fails if the order path made a syscall after warm-up.
"""
import sys, os, json, subprocess, argparse, platform, re, time, shutil

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SNAP = "build/refdata-20260915-v1.bin"
BENCHES = [
    ("bench", ["build/bench", SNAP]),
    ("bench_seq", ["build/bench_seq", "build/seqbench"]),
    ("bench_risk", ["build/bench_risk", SNAP]),
    ("bench_oms", ["build/bench_oms", SNAP]),
    ("bench_md", ["build/bench_md", "build/mdbench"]),
    ("bench_decode", ["build/bench_decode", SNAP]),
    ("run_sim --bench", ["build/run_sim", SNAP, "build/sim/bench", "--bench", "--steps", "20000"]),
]
GATES = {"p50": 0.05, "p999": 0.15, "value": -0.05}

def host_class():
    for b in ("build/bench", "build/bench_seq"):
        pass
    if platform.system() == "Darwin":
        cpu = subprocess.run(["sysctl", "-n", "machdep.cpu.brand_string"], capture_output=True, text=True).stdout.strip()
    else:
        cpu = "unknown"
        try:
            for line in open("/proc/cpuinfo"):
                if line.startswith("model name"): cpu = line.split(":", 1)[1].strip(); break
        except OSError: pass
    return f"{platform.machine()}/{cpu}"

def baseline_path(hc):
    return os.path.join(ROOT, "bench", "baselines", re.sub(r"[^A-Za-z0-9._-]+", "_", hc) + ".json")

def run_bench(name, cmd):
    if not os.path.exists(cmd[0]):
        print(f"  skip {name}: {cmd[0]} not built"); return []
    t0 = time.time()
    p = subprocess.run(cmd, capture_output=True, text=True, cwd=ROOT)
    rows = []
    for line in p.stdout.splitlines():
        if line.startswith('{"bench"'):
            try: rows.append(json.loads(line))
            except json.JSONDecodeError: print("  bad json:", line)
    status = "ok" if p.returncode == 0 else f"exit {p.returncode}"
    print(f"  {name}: {len(rows)} rows, {time.time() - t0:.1f}s, {status}")
    if p.returncode != 0:
        print(p.stderr[-2000:])
        rows.append({"bench": f"{name}.exit", "unit": "exit", "value": p.returncode, "allocs": 0})
    return rows

def strace_check():
    if platform.system() != "Linux" or not shutil.which("strace"):
        print("  strace: not available on this host, skipped"); return True
    log = os.path.join(ROOT, "build", "benchruns", "strace.log")
    subprocess.run(["strace", "-f", "-o", log, "build/bench_risk", SNAP], cwd=ROOT, capture_output=True)
    # syscalls after the warm-up marker (the first write to stdout is the report, printed after the timed loop)
    lines = open(log).read().splitlines()
    calls = [l for l in lines if re.match(r"^\d+\s+\w+\(", l)]
    # everything between the last mmap/brk of set-up and the first write(1, is the timed region
    first_write = next((i for i, l in enumerate(calls) if re.search(r"\swrite\(1,", l)), len(calls))
    setup_end = max((i for i, l in enumerate(calls[:first_write]) if re.search(r"\s(mmap|brk|munmap|openat|read)\(", l)), default=0)
    timed = [l for l in calls[setup_end + 1:first_write] if not re.search(r"\s(exit_group|rt_sig|futex)\(", l)]
    if timed:
        print(f"  strace: {len(timed)} syscalls on the order path after set-up:"); [print("    " + l[:120]) for l in timed[:10]]
        return False
    print("  strace: no syscalls on the order path"); return True

def compare(run, base):
    fails, notes = [], []
    brow = {r["bench"]: r for r in base.get("rows", [])} if base else {}
    pinned = bool(base and base.get("pinned"))
    if base and not pinned: notes.append("baseline host not pinned: p99.9 is informational, p50 and throughput are gated")
    for r in run["rows"]:
        b = brow.get(r["bench"])
        if r["unit"] == "exit": fails.append(f"{r['bench']}: benchmark exited {r['value']}"); continue
        if not b: notes.append(f"{r['bench']}: no baseline row"); continue
        if r["unit"] == "ns/op":
            for k in (("p50", "p999") if pinned else ("p50",)):
                if b.get(k, 0) > 0:
                    d = r[k] / b[k] - 1
                    if d > GATES[k]: fails.append(f"{r['bench']}: {k} {b[k]:.1f} -> {r[k]:.1f} (+{d*100:.1f}% > {GATES[k]*100:.0f}%)")
        elif r["unit"] == "ops/s":
            d = r["value"] / b["value"] - 1
            if d < GATES["value"]: fails.append(f"{r['bench']}: {b['value']:.0f} -> {r['value']:.0f} ops/s ({d*100:.1f}%)")
        if r.get("allocs", 0) > b.get("allocs", 0): fails.append(f"{r['bench']}: allocations {b.get('allocs',0)} -> {r['allocs']}")
    return fails, notes

def fmt_row(r):
    if r["unit"] == "ns/op": return f"{r['bench']:36s} p50 {r['p50']:9.1f}  p99 {r['p99']:9.1f}  p99.9 {r['p999']:9.1f}  max {r['max']:10.1f} ns  allocs {r.get('allocs',0)}"
    return f"{r['bench']:36s} {r['value']:14.1f} {r['unit']}  allocs {r.get('allocs',0)}"

def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run"); r.add_argument("--gate", action="store_true"); r.add_argument("--write", action="store_true")
    r.add_argument("--strace", action="store_true"); r.add_argument("--only", nargs="*")
    r.add_argument("--pinned", action="store_true", help="with --write: this host is pinned and quiet, so p99.9 is gated too")
    a = ap.parse_args()
    os.makedirs(os.path.join(ROOT, "build", "benchruns"), exist_ok=True)
    hc = host_class()
    commit = subprocess.run(["git", "rev-parse", "--short", "HEAD"], capture_output=True, text=True, cwd=ROOT).stdout.strip()
    print(f"host class: {hc}\ncommit: {commit}")
    rows = []
    for name, cmd in BENCHES:
        if a.only and name not in a.only: continue
        rows += run_bench(name, cmd)
    run = {"hostClass": hc, "commit": commit, "pinned": bool(a.pinned), "when": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "rows": rows}
    json.dump(run, open(os.path.join(ROOT, "build", "benchruns", "run.json"), "w"), indent=1)
    print("\n" + "\n".join(fmt_row(x) for x in rows))
    ok = True
    if a.strace: ok = strace_check() and ok
    bp = baseline_path(hc)
    if a.write:
        json.dump(run, open(bp, "w"), indent=1); print(f"\nwrote baseline {os.path.relpath(bp, ROOT)}"); return 0 if ok else 1
    base = json.load(open(bp)) if os.path.exists(bp) else None
    fails, notes = compare(run, base)
    print()
    if base is None: print(f"no baseline for this host class ({os.path.relpath(bp, ROOT)}); run with --write to create one"); notes.append("no baseline")
    for n in notes: print("note:", n)
    for f in fails: print("REGRESSION:", f)
    if not fails and base is not None: print(f"within gates of baseline {os.path.relpath(bp, ROOT)} ({base.get('commit')})")
    if a.gate and (fails or base is None or not ok): print("bench gate: FAIL"); return 1
    print("bench gate: pass" if a.gate else "informational run")
    return 0 if ok else 1

if __name__ == "__main__":
    sys.exit(main())
