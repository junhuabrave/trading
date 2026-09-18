#!/usr/bin/env python3
"""
make_scenario.py SNAPSHOT OUT.log [--orders N] [--seed S]

Writes a scenario as a log of external events: BookDelta/Trade frames on stream 1 and
NewOrder/CancelOrder frames on stream 0, each with originTs set and seq zero. The harness
(run_sim --events OUT.log) feeds them in originTs order. A scenario is therefore itself a
log, and a captured day can be cut into the same shape.
"""
import sys, os, random, argparse
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "..", "gen", "py"))
sys.path.insert(0, os.path.join(HERE, "..", "..", "tools"))
import trading as t
from snapshot import Snapshot

TICK = 1_000_000

def main():
    ap = argparse.ArgumentParser(); ap.add_argument("snapshot"); ap.add_argument("out")
    ap.add_argument("--orders", type=int, default=2000); ap.add_argument("--seed", type=int, default=1)
    a = ap.parse_args()
    rng = random.Random(a.seed)
    snap = Snapshot(a.snapshot)
    syms = [r for r in snap.instruments() if r.status == t.SymbolStatusCode.Active and r.refPrice > 0]
    mid = {r.symbolIdx: r.refPrice for r in syms}
    ts = 1_700_000_000_000_000_000
    out = bytearray(); live = []; venue_seq = 0; order_id = 0; cl = 0
    flags = int(t.FrameFlags.simulated)
    for i in range(a.orders):
        # a burst of market data, then one client action
        for _ in range(rng.randrange(0, 6)):
            ts += rng.randrange(500, 5000); s = rng.choice(syms).symbolIdx; venue_seq += 1
            if rng.random() < 0.75:
                side = t.BookSide.Bid if rng.random() < 0.5 else t.BookSide.Ask
                lvl = rng.randrange(0, 4)
                px = mid[s] - TICK * (1 + lvl) if side == t.BookSide.Bid else mid[s] + TICK * (1 + lvl)
                m = t.BookDelta(symbolIdx=s, venueId=2, action=t.BookAction.Set, side=side, price=px, qty=100 * rng.randrange(1, 30),
                                orderCount=1, flags=t.BookFlags.endOfPacket, venueSeq=venue_seq, venueTs=ts)
            else:
                agg = t.BookSide.Bid if rng.random() < 0.5 else t.BookSide.Ask
                px = mid[s] + TICK if agg == t.BookSide.Bid else mid[s] - TICK
                m = t.Trade(symbolIdx=s, venueId=2, aggressorSide=agg, price=px, qty=100 * rng.randrange(1, 8), venueSeq=venue_seq, venueTs=ts, tradeId=venue_seq)
                if rng.random() < 0.25: mid[s] += TICK if rng.random() < 0.5 else -TICK
            out += t.frame(m, origin_ts=ts, source_id=8, stream_id=1, flags=flags)
        ts += rng.randrange(1000, 20000)
        if live and rng.random() < 0.3:
            oid = live.pop(rng.randrange(len(live))); cl += 1
            out += t.frame(t.CancelOrder(orderId=oid, clOrdId=cl, accountIdx=42, sessionId=9), origin_ts=ts, source_id=2, flags=flags)
            continue
        s = rng.choice(syms).symbolIdx; order_id += 1; cl += 1
        side = t.Side.SellShort if rng.random() < 0.15 else (t.Side.Buy if rng.random() < 0.5 else t.Side.Sell)
        o = t.NewOrder(clOrdId=cl, orderId=(2 << 48) | order_id, accountIdx=42, symbolIdx=s, side=side, ordType=t.OrdType.Limit,
                       tif=t.Tif.Day, qty=100 * rng.randrange(1, 10), price=mid[s] + TICK * rng.randrange(-3, 4), sessionId=9, clientTag=order_id)
        out += t.frame(o, origin_ts=ts, source_id=2, flags=flags)
        live.append(o.orderId)
        if len(live) > 300: live.pop(rng.randrange(len(live)))
    open(a.out, "wb").write(out)
    print(f"wrote {a.out}: {len(out)} bytes, {a.orders} client steps")

if __name__ == "__main__":
    main()
