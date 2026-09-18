#!/usr/bin/env python3
"""
loginspect.py : look inside journals and frame logs.

  loginspect.py dump  PATH [--from SEQ] [--to SEQ] [--template NAME|ID]... [--limit N] [--json]
  loginspect.py chain PATH SEQ            follow causeSeq backwards from SEQ (the latency waterfall)
  loginspect.py diff  PATH_A PATH_B       first differing frame, by sequence
  loginspect.py stats PATH                counts per template, per source, batch and flag totals

PATH is a journal directory (segments seg-NNNNNN.jnl, each with a 64-byte header), a single
segment file, or a plain frame log with no header (what snapshot.py diff and test_codec write).
"""
import sys, os, json, struct, argparse, glob
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "gen", "py"))
import trading as t

JHDR = 64

def segments(path):
    if os.path.isdir(path):
        return sorted(glob.glob(os.path.join(path, "seg-*.jnl")))
    return [path]

def frames(path):
    """Yield (header, body) across every segment in order."""
    for seg in segments(path):
        buf = open(seg, "rb").read()
        off = JHDR if buf[:4] == b"SEQJ" else 0
        for h, body in t.iter_frames(buf[off:]):
            yield h, body

def name_of(h):
    cls = t.MESSAGES.get(h.templateId)
    return cls.__name__ if cls else f"template{h.templateId}"

def flag_str(flags):
    out = [f.name for f in t.FrameFlags if f.value & flags]
    return "|".join(out) if out else "-"

def fmt(h, body, as_json=False):
    if as_json:
        d = {"seq": h.seq, "stream": h.streamId, "template": name_of(h), "seqTs": h.seqTs, "originTs": h.originTs,
             "causeSeq": h.causeSeq, "sourceId": h.sourceId, "flags": flag_str(h.flags)}
        if hasattr(body, "_FIELDS"):
            d["body"] = {f: getattr(body, f) for f in body._FIELDS if not f.startswith(("pad", "reserved"))}
        return json.dumps(d, default=str)
    b = repr(body) if hasattr(body, "_FIELDS") else f"<{len(body)} bytes>"
    if len(b) > 160: b = b[:157] + "..."
    lat = f" +{h.seqTs - h.originTs}ns" if h.originTs and h.seqTs >= h.originTs else ""
    return f"{h.streamId}:{h.seq:<9d} {name_of(h):20s} src={h.sourceId:<3d} cause={h.causeSeq:<9d} {flag_str(h.flags):12s}{lat} {b}"

def resolve_templates(names):
    ids = set()
    by_name = {cls.__name__: tid for tid, cls in t.MESSAGES.items()}
    for n in names or []:
        ids.add(int(n) if n.isdigit() else by_name[n])
    return ids

def cmd_dump(a):
    want = resolve_templates(a.template); n = 0
    for h, body in frames(a.path):
        if a.frm and h.seq < a.frm: continue
        if a.to and h.seq > a.to: break
        if want and h.templateId not in want: continue
        print(fmt(h, body, a.json)); n += 1
        if a.limit and n >= a.limit: break
    print(f"-- {n} frames", file=sys.stderr)

def cmd_chain(a):
    by_seq = {}
    for h, body in frames(a.path):
        if h.streamId == a.stream: by_seq[h.seq] = (h, body)
    seq = a.seq; chain = []
    while seq and seq in by_seq:
        chain.append(by_seq[seq]); seq = by_seq[seq][0].causeSeq
    if not chain: sys.exit(f"seq {a.seq} not found")
    chain.reverse()
    t0 = chain[0][0].originTs or chain[0][0].seqTs
    prev = t0
    for h, body in chain:
        print(f"+{(h.seqTs - t0):>9d}ns  (+{h.seqTs - prev:>7d})  {fmt(h, body)}")
        prev = h.seqTs
    print(f"-- {len(chain)} hops, {chain[-1][0].seqTs - t0} ns origin to last")

def cmd_diff(a):
    fa, fb = frames(a.a), frames(a.b); n = 0
    for (ha, ba), (hb, bb) in zip(fa, fb):
        n += 1
        if ha.pack() != hb.pack() or getattr(ba, "pack", lambda: ba)() != getattr(bb, "pack", lambda: bb)():
            print(f"first difference at frame {n}:\n  A: {fmt(ha, ba)}\n  B: {fmt(hb, bb)}"); sys.exit(1)
    ra, rb = sum(1 for _ in fa), sum(1 for _ in fb)
    if ra or rb: print(f"identical for {n} frames, then A has {ra} more and B has {rb} more"); sys.exit(1)
    print(f"identical: {n} frames")

def cmd_stats(a):
    per_t, per_src, flags = {}, {}, {}
    n = 0; first = last = None; batches = 0
    for h, body in frames(a.path):
        n += 1; first = first or h.seq; last = h.seq
        per_t[name_of(h)] = per_t.get(name_of(h), 0) + 1
        per_src[h.sourceId] = per_src.get(h.sourceId, 0) + 1
        for f in t.FrameFlags:
            if h.flags & f.value: flags[f.name] = flags.get(f.name, 0) + 1
        if h.flags & t.FrameFlags.lastInBatch: batches += 1
    print(f"{a.path}: {n} frames, seq {first}..{last}, {batches} batches")
    for k, v in sorted(per_t.items(), key=lambda kv: -kv[1]): print(f"  {k:22s} {v}")
    print("  by source:", ", ".join(f"{k}:{v}" for k, v in sorted(per_src.items())))
    print("  flags:", ", ".join(f"{k}:{v}" for k, v in sorted(flags.items())) or "none")

def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    d = sub.add_parser("dump"); d.add_argument("path"); d.add_argument("--from", dest="frm", type=int, default=0)
    d.add_argument("--to", type=int, default=0); d.add_argument("--template", action="append"); d.add_argument("--limit", type=int, default=0)
    d.add_argument("--json", action="store_true"); d.set_defaults(fn=cmd_dump)
    c = sub.add_parser("chain"); c.add_argument("path"); c.add_argument("seq", type=int); c.add_argument("--stream", type=int, default=0); c.set_defaults(fn=cmd_chain)
    f = sub.add_parser("diff"); f.add_argument("a"); f.add_argument("b"); f.set_defaults(fn=cmd_diff)
    s = sub.add_parser("stats"); s.add_argument("path"); s.set_defaults(fn=cmd_stats)
    a = ap.parse_args(); a.fn(a)

if __name__ == "__main__":
    main()
