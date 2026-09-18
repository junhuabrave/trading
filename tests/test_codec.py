#!/usr/bin/env python3
import sys, os, re
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "gen", "py"))
import trading as t

def read_cpp_log(path):
    buf = open(path, "rb").read()
    frames = list(t.iter_frames(buf))
    assert len(frames) == 5, len(frames)
    h, ss = frames[0]
    assert isinstance(ss, t.SessionStart) and ss.businessDate == 20260915 and ss.snapshotVersion == 3
    assert ss.snapshotHash == [i * 7 & 0xFF for i in range(32)]
    h, no = frames[1]
    assert isinstance(no, t.NewOrder)
    assert t.Side(no.side) == t.Side.SellShort and no.qty == 500 and no.price == 12_345_000_000
    assert t.OrderFlags(no.orderFlags) & t.OrderFlags.locateAttached
    assert h.seq == 2 and h.originTs == h.seqTs - 600
    h, rd = frames[2]
    assert isinstance(rd, t.RiskDecision) and h.causeSeq == 2 and t.RiskVerdict(rd.verdict) == t.RiskVerdict.Accept
    assert rd.notional == 500 * 12_345_000_000
    h, bd = frames[3]
    assert isinstance(bd, t.BookDelta) and h.streamId == 1 and bd.qty == 1200
    h, er = frames[4]
    assert isinstance(er, t.ExecReport) and er.venueExecId == "XNAS0000000000000042" and er.fee == -30_000_000
    # causality chain: ExecReport -> RiskDecision -> NewOrder
    by_seq = {h.seq: (h, b) for h, b in frames if h.streamId == 0}
    chain = []
    seq = frames[4][0].seq
    while seq:
        h, b = by_seq[seq]; chain.append(type(b).__name__); seq = h.causeSeq
    assert chain == ["ExecReport", "RiskDecision", "NewOrder"], chain
    print("python read C++ log ok; cause chain:", " <- ".join(chain))

def write_py_log(path):
    out = b""
    ss = t.SessionStart(businessDate=20260915, snapshotVersion=3, schemaVersion=t.SCHEMA_VERSION,
                        snapshotHash=[i * 7 & 0xFF for i in range(32)])
    out += t.frame(ss, seq=1, seq_ts=1_700_000_000_000_000_000, source_id=1)
    no = t.NewOrder(clOrdId=0xC10, orderId=(2 << 48) | 1, accountIdx=42, symbolIdx=7,
                    side=t.Side.SellShort, ordType=t.OrdType.Limit, tif=t.Tif.Day,
                    orderFlags=t.OrderFlags.locateAttached | t.OrderFlags.allowDark,
                    qty=500, price=12_345_000_000, locateId=(10 << 48) | 77, routingProfile=3, sessionId=9, clientTag=0xBEEF)
    out += t.frame(no, seq=2, seq_ts=1_700_000_000_000_001_500, origin_ts=1_700_000_000_000_000_900, source_id=2)
    rd = t.RiskDecision(orderId=no.orderId, accountIdx=42, symbolIdx=7, verdict=t.RiskVerdict.Accept,
                        checkMask=0x7F, notional=500 * 12_345_000_000, buyingPowerAfter=10**14)
    out += t.frame(rd, seq=3, seq_ts=1_700_000_000_000_002_500, cause_seq=2, source_id=3)
    bd = t.BookDelta(symbolIdx=7, venueId=2, action=t.BookAction.Set, side=t.BookSide.Bid,
                     price=12_344_000_000, qty=1200, orderCount=4, flags=t.BookFlags.endOfPacket, venueSeq=555, venueTs=1)
    out += t.frame(bd, seq=1001, stream_id=1, source_id=8)
    er = t.ExecReport(orderId=no.orderId, clOrdId=0xC10, execId=(7 << 48) | 5, accountIdx=42, symbolIdx=7,
                      execType=t.ExecType.Fill, ordStatus=t.OrdStatus.Filled, side=t.Side.SellShort,
                      liquidityFlag=t.Liquidity.Removed, venueId=2, lastQty=500, lastPx=12_345_000_000, cumQty=500,
                      avgPx=12_345_000_000, fee=-30_000_000, nbboBid=12_344_000_000, nbboAsk=12_346_000_000,
                      venueExecId="XNAS0000000000000042", clientTag=0xBEEF)
    out += t.frame(er, seq=4, cause_seq=3, source_id=7)
    open(path, "wb").write(out)
    print(f"python wrote {len(out)} bytes, 5 frames")

def check_sizes_against_cpp(header_path):
    src = open(header_path).read()
    for name, cls in t.MESSAGES.items():
        m = re.search(rf"static_assert\(sizeof\({cls.__name__}\) == (\d+)", src)
        assert m and int(m.group(1)) == cls.BLOCK_LENGTH, cls.__name__
    for comp in ("FrameHeader", "SymbolRecord", "SnapshotHeader", "SectionEntry"):
        m = re.search(rf"static_assert\(sizeof\({comp}\) == (\d+)", src)
        assert m and int(m.group(1)) == getattr(t, comp).SIZE, comp
    assert t.FrameHeader.SIZE == 48 and t.NewOrder.BLOCK_LENGTH == 128
    print(f"python/C++ sizes agree for {len(t.MESSAGES)} messages")

if __name__ == "__main__":
    cmd, path = sys.argv[1], sys.argv[2]
    if cmd == "read": read_cpp_log(path)
    elif cmd == "write": write_py_log(path)
    elif cmd == "sizes": check_sizes_against_cpp(path)
