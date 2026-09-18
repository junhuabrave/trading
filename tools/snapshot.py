#!/usr/bin/env python3
"""
snapshot.py : reference-data snapshot builder / verifier / differ

  snapshot.py build  fixture.json out.bin --date YYYYMMDD [--version N]
  snapshot.py verify snap.bin
  snapshot.py dump   snap.bin [--symbols N]
  snapshot.py diff   old.bin new.bin out.log      # sequenced update messages, SBE frames
  snapshot.py diff   old.bin new.bin --json       # same, human readable

File layout (all sections page aligned, records fixed size, see schema composites):
  header (4096)  | instruments | listings | venues | tick tables | calendars
  | corporate actions | lists (bitsets) | fees | accounts | components | ticker index
"""
import sys, os, json, struct, argparse, time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "gen", "py"))
import trading as t
import blake3

PAGE = 4096
MAGIC = "RFDS"
FORMAT_VERSION = 1
MAX_SYMBOLS = 65536
LIST_BYTES = MAX_SYMBOLS // 8

SEC_INSTRUMENTS, SEC_LISTINGS, SEC_VENUES, SEC_TICKTABLES, SEC_CALENDARS, \
SEC_CORPACTIONS, SEC_LISTS, SEC_FEES, SEC_ACCOUNTS, SEC_COMPONENTS, SEC_TICKERINDEX = range(1, 12)
SECTION_NAMES = {1: "instruments", 2: "listings", 3: "venues", 4: "tickTables", 5: "calendars",
                 6: "corporateActions", 7: "lists", 8: "fees", 9: "accounts", 10: "components", 11: "tickerIndex"}
LIST_IDS = {"etb": 1, "htb": 2, "restricted": 3, "threshold": 4, "watch": 5}

def pad_page(b: bytes) -> bytes:
    r = len(b) % PAGE
    return b + (b"\0" * (PAGE - r) if r else b"")

def enum_val(cls, v):
    return cls[v].value if isinstance(v, str) else int(v)

def flags_val(cls, names):
    v = 0
    for n in names: v |= cls[n].value
    return v

# ------------------------------------------------------------------ build
def build(fixture: dict, business_date: int, version: int) -> bytes:
    instruments = fixture["instruments"]
    max_idx = max(i["symbolIdx"] for i in instruments)
    assert max_idx < MAX_SYMBOLS
    n_slots = max_idx + 1
    # listings sorted by (symbolIdx, venueId), with per-symbol offset/count
    listings = sorted(fixture.get("listings", []), key=lambda l: (l["symbolIdx"], l["venueId"]))
    lst_off, lst_cnt = {}, {}
    for i, l in enumerate(listings):
        lst_off.setdefault(l["symbolIdx"], i); lst_cnt[l["symbolIdx"]] = lst_cnt.get(l["symbolIdx"], 0) + 1
    # lists bitsets
    lists = fixture.get("lists", {})
    def has(list_name, idx): return idx in set(lists.get(list_name, []))

    recs = [t.SymbolRecord() for _ in range(n_slots)]
    for i in instruments:
        idx = i["symbolIdx"]
        fl = flags_val(t.SymbolFlags, i.get("flags", []))
        for ln, bit in (("etb", "etb"), ("htb", "hardToBorrow"), ("restricted", "restricted"), ("threshold", "threshold")):
            if has(ln, idx): fl |= t.SymbolFlags[bit].value
        recs[idx] = t.SymbolRecord(
            symbolIdx=idx, status=enum_val(t.SymbolStatusCode, i.get("status", "Active")),
            instrumentType=enum_val(t.InstrumentType, i.get("instrumentType", "CommonStock")),
            listingVenue=i.get("listingVenue", 0), tickTableId=i.get("tickTableId", 1),
            currency=i.get("currency", 840), lotSize=i.get("lotSize", 100), flags=fl,
            priceScale=i.get("priceScale", 8), qtyScale=i.get("qtyScale", 0),
            settlementDays=i.get("settlementDays", 1), marginClass=i.get("marginClass", 1),
            refPrice=i.get("refPrice", 0), prevClose=i.get("prevClose", i.get("refPrice", 0)),
            adv30=i.get("adv30", 0), sharesOutstanding=i.get("sharesOutstanding", 0),
            issuerId=i.get("issuerId", 0), sector=i.get("sector", 0), shareClass=i.get("shareClass", 0),
            listingCount=lst_cnt.get(idx, 0), listingOffset=lst_off.get(idx, 0), underlyingIdx=i.get("underlyingIdx", 0),
            ticker=i["ticker"], cusip=i.get("cusip", ""), isin=i.get("isin", ""), figi=i.get("figi", ""),
            sedol=i.get("sedol", ""), name=i.get("name", ""), validFrom=i.get("validFrom", 0),
            lastCorpActionId=i.get("lastCorpActionId", 0))
    sec_instr = b"".join(r.pack() for r in recs)
    sec_list = b"".join(t.ListingRecord(symbolIdx=l["symbolIdx"], venueId=l["venueId"], tickTableId=l.get("tickTableId", 0),
                                        tradable=1 if l.get("tradable", True) else 0, venueSymbol=l.get("venueSymbol", ""),
                                        validFrom=l.get("validFrom", 0)).pack() for l in listings)
    sec_ven = b"".join(t.VenueRecord(venueId=v["venueId"], venueType=enum_val(t.VenueType, v.get("venueType", "Exchange")),
                                     protocol=v.get("protocol", 0), cancelOnDisconnect=1 if v.get("cancelOnDisconnect", True) else 0,
                                     mic=v.get("mic", ""), name=v.get("name", ""),
                                     sessionIds=(v.get("sessionIds", []) + [0] * 8)[:8],
                                     oneWayLatencyNs=v.get("oneWayLatencyNs", 0)).pack()
                       for v in sorted(fixture.get("venues", []), key=lambda v: v["venueId"]))
    def tick_rec(tt):
        bands = []
        for fl, tk in tt["bands"][:16]: bands += [fl, tk]
        bands += [0] * (32 - len(bands))
        return t.TickTableRecord(tickTableId=tt["tickTableId"], bandCount=min(16, len(tt["bands"])), bands=bands).pack()
    sec_tick = b"".join(tick_rec(tt) for tt in sorted(fixture.get("tickTables", []), key=lambda x: x["tickTableId"]))
    sec_cal = b"".join(t.CalendarRecord(venueId=c["venueId"], flags=flags_val(t.CalendarFlags, c.get("flags", [])), date=c["date"],
                                        openTs=c.get("openTs", 0), closeTs=c.get("closeTs", 0), auctionTs=c.get("auctionTs", 0)).pack()
                       for c in sorted(fixture.get("calendars", []), key=lambda c: (c["venueId"], c["date"])))
    sec_ca = b"".join(t.CorporateActionRecord(actionId=a["actionId"], symbolIdx=a["symbolIdx"], newSymbolIdx=a.get("newSymbolIdx", 0),
                                              actionType=enum_val(t.CorpActionType, a["actionType"]), applied=1 if a.get("applied") else 0,
                                              exDate=a.get("exDate", 0), recordDate=a.get("recordDate", 0), payDate=a.get("payDate", 0),
                                              ratioNum=a.get("ratioNum", 1), ratioDen=a.get("ratioDen", 1), cashAmount=a.get("cashAmount", 0),
                                              currency=a.get("currency", 840), effectiveSeq=a.get("effectiveSeq", 0)).pack()
                      for a in sorted(fixture.get("corporateActions", []), key=lambda a: a["actionId"]))
    bitsets = []
    for name, lid in sorted(LIST_IDS.items(), key=lambda kv: kv[1]):
        bs = bytearray(LIST_BYTES)
        for idx in lists.get(name, []):
            bs[idx >> 3] |= 1 << (idx & 7)
        bitsets.append(bytes(bs))
    sec_lists = b"".join(bitsets)
    sec_fees = b"".join(t.FeeRecord(venueId=f["venueId"], feeCode=f["feeCode"], liquidity=enum_val(t.Liquidity, f.get("liquidity", "Removed")),
                                    feePerShare=f.get("feePerShare", 0), tierThreshold=f.get("tierThreshold", 0),
                                    capPerOrder=f.get("capPerOrder", 0), effectiveDate=f.get("effectiveDate", 0)).pack()
                        for f in sorted(fixture.get("fees", []), key=lambda f: (f["venueId"], f["feeCode"])))
    accts = fixture.get("accounts", [])
    n_acct = (max(a["accountIdx"] for a in accts) + 1) if accts else 0
    arecs = [t.AccountRecord() for _ in range(n_acct)]
    for a in accts:
        arecs[a["accountIdx"]] = t.AccountRecord(accountIdx=a["accountIdx"], accountType=enum_val(t.AccountType, a.get("accountType", "Margin")),
                                                 routingProfile=a.get("routingProfile", 1), limitSetId=a.get("limitSetId", 0),
                                                 entitlements=a.get("entitlements", 0), baseCurrency=a.get("baseCurrency", 840),
                                                 externalRef=a.get("externalRef", ""))
    sec_acct = b"".join(r.pack() for r in arecs)
    sec_comp = b"".join(t.ComponentRecord(sourceId=c["sourceId"], role=enum_val(t.ComponentRole, c["role"]),
                                          streamMask=c.get("streamMask", 1), name=c.get("name", "")).pack()
                        for c in sorted(fixture.get("components", []), key=lambda c: c["sourceId"]))
    sec_tix = b"".join(t.TickerIndexEntry(ticker=i["ticker"], symbolIdx=i["symbolIdx"]).pack()
                       for i in sorted(instruments, key=lambda i: i["ticker"]))

    sections = [
        (SEC_INSTRUMENTS, t.SymbolRecord.SIZE, n_slots, sec_instr),
        (SEC_LISTINGS, t.ListingRecord.SIZE, len(listings), sec_list),
        (SEC_VENUES, t.VenueRecord.SIZE, len(sec_ven) // t.VenueRecord.SIZE, sec_ven),
        (SEC_TICKTABLES, t.TickTableRecord.SIZE, len(sec_tick) // t.TickTableRecord.SIZE, sec_tick),
        (SEC_CALENDARS, t.CalendarRecord.SIZE, len(sec_cal) // t.CalendarRecord.SIZE, sec_cal),
        (SEC_CORPACTIONS, t.CorporateActionRecord.SIZE, len(sec_ca) // t.CorporateActionRecord.SIZE, sec_ca),
        (SEC_LISTS, LIST_BYTES, len(bitsets), sec_lists),
        (SEC_FEES, t.FeeRecord.SIZE, len(sec_fees) // t.FeeRecord.SIZE, sec_fees),
        (SEC_ACCOUNTS, t.AccountRecord.SIZE, n_acct, sec_acct),
        (SEC_COMPONENTS, t.ComponentRecord.SIZE, len(sec_comp) // t.ComponentRecord.SIZE, sec_comp),
        (SEC_TICKERINDEX, t.TickerIndexEntry.SIZE, len(instruments), sec_tix),
    ]
    body = b""
    entries = []
    off = PAGE
    content = blake3.blake3()
    for stype, rsize, count, data in sections:
        entries.append(t.SectionEntry(sectionType=stype, recordSize=rsize, recordCount=count, offset=off,
                                      sectionHash=list(blake3.blake3(data).digest())))
        content.update(data)
        padded = pad_page(data)
        body += padded; off += len(padded)
    sec_bytes = b"".join(e.pack() for e in entries) + b"\0" * (2048 - 64 * len(entries))
    src_entries = fixture.get("sources", [])[:16]
    src_bytes = b"".join(t.SourceEntry(sourceId=s["sourceId"], sourceTs=s.get("sourceTs", 0),
                                       fileHash=list(bytes.fromhex(s["fileHash"]))).pack() for s in src_entries)
    src_bytes += b"\0" * (768 - len(src_bytes))
    hdr = t.SnapshotHeader(magic=MAGIC, formatVersion=FORMAT_VERSION, schemaVersion=t.SCHEMA_VERSION, sectionCount=len(entries),
                           businessDate=business_date, snapshotVersion=version, sourceCount=len(src_entries),
                           generatedTs=time.time_ns(), fileSize=PAGE + len(body), contentHash=list(content.digest()),
                           sections=list(sec_bytes), sources=list(src_bytes))
    hb = hdr.pack()
    assert len(hb) == PAGE
    return hb + body

# ------------------------------------------------------------------ read / verify
class Snapshot:
    def __init__(self, path):
        self.path = path
        self.buf = open(path, "rb").read()
        self.hdr = t.SnapshotHeader.unpack(self.buf, 0)
        self.sections = {}
        raw = bytes(self.hdr.sections)
        for i in range(self.hdr.sectionCount):
            e = t.SectionEntry.unpack(raw, i * 64)
            self.sections[e.sectionType] = e
    def verify(self):
        errs = []
        h = self.hdr
        if h.magic != MAGIC: errs.append("bad magic")
        if h.formatVersion != FORMAT_VERSION: errs.append(f"format version {h.formatVersion}")
        if h.schemaVersion > t.SCHEMA_VERSION: errs.append(f"schema version {h.schemaVersion} newer than codec {t.SCHEMA_VERSION}")
        if h.fileSize != len(self.buf): errs.append(f"fileSize {h.fileSize} != {len(self.buf)}")
        content = blake3.blake3()
        for stype in sorted(self.sections, key=lambda s: self.sections[s].offset):
            e = self.sections[stype]
            if e.offset % PAGE: errs.append(f"section {stype} not page aligned")
            data = self.buf[e.offset:e.offset + e.recordSize * e.recordCount]
            if len(data) != e.recordSize * e.recordCount: errs.append(f"section {stype} truncated")
            if list(blake3.blake3(data).digest()) != e.sectionHash: errs.append(f"section {SECTION_NAMES.get(stype, stype)} hash mismatch")
            content.update(data)
        if list(content.digest()) != h.contentHash: errs.append("content hash mismatch")
        # semantic checks
        inst = self.instruments()
        seen = {}
        for r in inst:
            if r.status == 0: continue
            if r.symbolIdx != inst.index(r): errs.append(f"slot/symbolIdx mismatch at {r.symbolIdx}")
            if r.ticker in seen: errs.append(f"duplicate ticker {r.ticker} ({seen[r.ticker]}, {r.symbolIdx})")
            seen[r.ticker] = r.symbolIdx
            if r.lotSize == 0: errs.append(f"{r.ticker}: lotSize 0")
            if r.status == t.SymbolStatusCode.Active and r.refPrice <= 0: errs.append(f"{r.ticker}: active with no refPrice")
        return errs
    def records(self, stype, cls):
        e = self.sections.get(stype)
        if not e: return []
        return [cls.unpack(self.buf, e.offset + i * e.recordSize) for i in range(e.recordCount)]
    def instruments(self):
        if not hasattr(self, "_inst"): self._inst = self.records(SEC_INSTRUMENTS, t.SymbolRecord)
        return self._inst
    def list_members(self, list_id):
        e = self.sections[SEC_LISTS]
        bs = self.buf[e.offset + (list_id - 1) * LIST_BYTES: e.offset + list_id * LIST_BYTES]
        return {i for i in range(len(bs) * 8) if bs[i >> 3] & (1 << (i & 7))}
    def content_hash_hex(self):
        return bytes(self.hdr.contentHash).hex()

# ------------------------------------------------------------------ diff -> messages
UPDATE_FIELDS = ["status", "tickTableId", "lotSize", "flags", "refPrice", "adv30", "marginClass", "sharesOutstanding"]

def diff(old: Snapshot, new: Snapshot, source_id=15):
    """Yield (message, description) so old + messages == new for everything the hot path holds."""
    oi, ni = old.instruments(), new.instruments()
    for idx in range(len(ni)):
        n = ni[idx]
        if n.status == 0: continue
        o = oi[idx] if idx < len(oi) else t.SymbolRecord()
        if o.status == 0:
            yield t.SymbolAdd(record=n), f"SymbolAdd {n.ticker} idx={idx}"
            continue
        mask = 0
        for f in UPDATE_FIELDS:
            if getattr(o, f) != getattr(n, f): mask |= t.SymbolUpdateMask[f].value
        if mask:
            yield t.SymbolUpdate(symbolIdx=idx, mask=mask, status=n.status, marginClass=n.marginClass, tickTableId=n.tickTableId,
                                 lotSize=n.lotSize, flags=n.flags, refPrice=n.refPrice, adv30=n.adv30,
                                 sharesOutstanding=n.sharesOutstanding), \
                  f"SymbolUpdate {n.ticker} mask=" + "|".join(f.name for f in t.SymbolUpdateMask if f.value & mask)
    for name, lid in sorted(LIST_IDS.items(), key=lambda kv: kv[1]):
        om, nm = old.list_members(lid), new.list_members(lid)
        for idx in sorted(nm - om):
            yield t.ListUpdate(listId=lid, op=t.ListOp.Add, symbolIdx=idx), f"ListUpdate add {name} idx={idx}"
        for idx in sorted(om - nm):
            yield t.ListUpdate(listId=lid, op=t.ListOp.Remove, symbolIdx=idx), f"ListUpdate remove {name} idx={idx}"
    oca = {a.actionId: a for a in old.records(SEC_CORPACTIONS, t.CorporateActionRecord)}
    for a in new.records(SEC_CORPACTIONS, t.CorporateActionRecord):
        if a.actionId not in oca or oca[a.actionId].pack() != a.pack():
            yield t.CorporateAction(record=a), f"CorporateAction id={a.actionId} sym={a.symbolIdx}"
    ott = {r.tickTableId: r.pack() for r in old.records(SEC_TICKTABLES, t.TickTableRecord)}
    for r in new.records(SEC_TICKTABLES, t.TickTableRecord):
        if ott.get(r.tickTableId) != r.pack():
            yield t.TickTableUpdate(record=r), f"TickTableUpdate id={r.tickTableId}"
    ocal = {(r.venueId, r.date): r.pack() for r in old.records(SEC_CALENDARS, t.CalendarRecord)}
    for r in new.records(SEC_CALENDARS, t.CalendarRecord):
        if ocal.get((r.venueId, r.date)) != r.pack():
            yield t.CalendarUpdate(record=r), f"CalendarUpdate venue={r.venueId} date={r.date}"
    ofee = {(r.venueId, r.feeCode): r.pack() for r in old.records(SEC_FEES, t.FeeRecord)}
    for r in new.records(SEC_FEES, t.FeeRecord):
        if ofee.get((r.venueId, r.feeCode)) != r.pack():
            yield t.FeeScheduleUpdate(record=r), f"FeeScheduleUpdate venue={r.venueId} code={r.feeCode}"

# ------------------------------------------------------------------ cli
def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("build"); b.add_argument("fixture"); b.add_argument("out"); b.add_argument("--date", type=int, required=True); b.add_argument("--version", type=int, default=1)
    v = sub.add_parser("verify"); v.add_argument("snap")
    d = sub.add_parser("dump"); d.add_argument("snap"); d.add_argument("--symbols", type=int, default=10)
    f = sub.add_parser("diff"); f.add_argument("old"); f.add_argument("new"); f.add_argument("out", nargs="?"); f.add_argument("--json", action="store_true")
    a = ap.parse_args()
    if a.cmd == "build":
        fx = json.load(open(a.fixture))
        data = build(fx, a.date, a.version)
        open(a.out, "wb").write(data)
        s = Snapshot(a.out); errs = s.verify()
        print(f"built {a.out}: {len(data)} bytes, {s.hdr.sectionCount} sections, hash {s.content_hash_hex()[:16]}")
        if errs:
            for e in errs: print("VALIDATION:", e, file=sys.stderr)
            sys.exit(1)
    elif a.cmd == "verify":
        s = Snapshot(a.snap); errs = s.verify()
        for e in errs: print("INVALID:", e, file=sys.stderr)
        print(f"{'invalid' if errs else 'valid'}: {a.snap} date={s.hdr.businessDate} v{s.hdr.snapshotVersion} hash={s.content_hash_hex()[:16]}")
        sys.exit(1 if errs else 0)
    elif a.cmd == "dump":
        s = Snapshot(a.snap)
        print(f"{a.snap}: date={s.hdr.businessDate} v{s.hdr.snapshotVersion} schema=v{s.hdr.schemaVersion} size={s.hdr.fileSize} hash={s.content_hash_hex()}")
        for stype, e in sorted(s.sections.items()):
            print(f"  section {stype:2d} {SECTION_NAMES[stype]:17s} off={e.offset:8d} rec={e.recordSize:5d} x {e.recordCount}")
        for r in [r for r in s.instruments() if r.status][:a.symbols]:
            print(f"  [{r.symbolIdx:5d}] {r.ticker:8s} {t.SymbolStatusCode(r.status).name:8s} tick={r.tickTableId} lot={r.lotSize} ref={r.refPrice/1e8:.2f} flags={t.SymbolFlags(r.flags)!s}")
    elif a.cmd == "diff":
        o, n = Snapshot(a.old), Snapshot(a.new)
        msgs = list(diff(o, n))
        if a.json or not a.out:
            for m, desc in msgs: print(desc)
            print(f"{len(msgs)} updates")
        if a.out:
            out = b"".join(t.frame(m, source_id=15) for m, _ in msgs)
            open(a.out, "wb").write(out)
            print(f"wrote {len(msgs)} update frames ({len(out)} bytes) to {a.out}")

if __name__ == "__main__":
    main()
