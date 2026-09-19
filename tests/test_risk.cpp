// tests/test_risk.cpp <day1 snapshot> <workdir>
// Every reject reason exercised on purpose, credit maths checked, then random flow and a
// cold replay that must reproduce every verdict and the final state hash.
#include "sequencer.hpp"
#include "risk.hpp"
#include <cstdio>
#include <filesystem>
#include <random>
#include <memory>
#include <chrono>

using namespace trading; using namespace trading::seq; using namespace trading::risk;
namespace fs = std::filesystem;
#define CHECK(c) do { if (!(c)) { std::fprintf(stderr, "CHECK failed %s:%d: %s\n", __FILE__, __LINE__, #c); return 1; } } while (0)

struct Harness {
    Journal& j; Sequencer& s; RiskEngine& e; BroadcastRing& ring; int rid; uint64_t nextOrder = 1, clOrd = 1;
    void drain() { while (ring.poll(rid)) ring.advance(rid); }
    void put(FrameHeader* f) { while (!s.submit(f)) drain(); }
    template <class M> uint64_t send(Frame<M>& f, uint16_t src = 13) { f.header.sourceId = src; put(&f.header); e.apply(&f.header); return f.header.seq; }
    // Sequence a NewOrder, evaluate it, sequence the RiskDecision. Returns the reason (NoReason on accept).
    Reason order(NewOrder o, uint64_t* orderIdOut = nullptr) {
        Frame<NewOrder> f; f.init(); f.header.sourceId = 2; f.body = o;
        if (f.body.orderId == 0) f.body.orderId = (uint64_t(2) << 48) | nextOrder++;
        if (f.body.clOrdId == 0) f.body.clOrdId = clOrd++;
        if (f.body.ordType == OrdType::Unset) f.body.ordType = OrdType::Limit;
        if (f.body.tif == Tif::Unset) f.body.tif = Tif::Day;
        put(&f.header);
        Frame<RiskDecision> d; d.init(); d.header.sourceId = 3; d.header.causeSeq = f.header.seq;
        e.evaluate(&f.header, f.body, d.body);
        put(&d.header);
        if (orderIdOut) *orderIdOut = f.body.orderId;
        return Reason(d.body.reason);
    }
    NewOrder mk(uint32_t acct, uint32_t sym, Side side, int64_t qty, int64_t px, uint64_t locate = 0) {
        NewOrder o{}; o.accountIdx = acct; o.symbolIdx = sym; o.side = side; o.qty = qty; o.price = px; o.locateId = locate; o.sessionId = 9; return o;
    }
    void limits(uint32_t acct, uint32_t sym, int64_t maxQty, int64_t maxNotional, int64_t gross, int64_t net, uint32_t collar, uint32_t rate, uint32_t open, uint32_t dupMs = 0) {
        Frame<LimitUpdate> f; f.init(); f.body.accountIdx = acct; f.body.symbolIdx = sym; f.body.maxOrderQty = maxQty;
        f.body.maxOrderNotional = maxNotional; f.body.maxGrossExposure = gross; f.body.maxNetExposure = net;
        f.body.priceCollarBps = collar; f.body.maxMsgRate = rate; f.body.maxOpenOrders = open; f.body.dupWindowMs = dupMs; send(f);
    }
    // Sequence a ReplaceOrder, evaluate it, sequence the RiskDecision.
    Reason replace(uint64_t orderId, uint32_t acct, int64_t qty, int64_t px) {
        Frame<ReplaceOrder> f; f.init(); f.header.sourceId = 2; f.body.orderId = orderId; f.body.accountIdx = acct;
        f.body.clOrdId = clOrd++; f.body.sessionId = 9; f.body.qty = qty; f.body.price = px;
        put(&f.header);
        Frame<RiskDecision> d; d.init(); d.header.sourceId = 3; d.header.causeSeq = f.header.seq;
        e.evaluateReplace(&f.header, f.body, d.body);
        put(&d.header);
        return Reason(d.body.reason);
    }
    // Firm-wide limits expressed at 1e-4 units, which is how a limit above $92bn crosses the wire.
    void limitsWide(uint32_t acct, int64_t maxQty, int64_t maxNotional, int64_t gross1e4, int64_t net1e4) {
        Frame<LimitUpdate> f; f.init(); f.body.accountIdx = acct; f.body.maxOrderQty = maxQty; f.body.maxOrderNotional = maxNotional;
        f.body.maxGrossExposure = gross1e4; f.body.maxNetExposure = net1e4; f.body.moneyScale = MoneyScale::Unit1e4; send(f);
    }
    void buyingPower(uint32_t acct, int64_t bp) { Frame<BuyingPowerUpdate> f; f.init(); f.body.accountIdx = acct; f.body.buyingPower = bp; send(f, 11); }
    void cancel(uint64_t orderId) { Frame<OrderState> f; f.init(); f.body.orderId = orderId; f.body.status = OrdStatus::Cancelled; send(f, 4); }
    void fill(uint64_t orderId, int64_t qty, int64_t px, bool last) {
        Frame<ExecReport> f; f.init(); f.body.orderId = orderId; f.body.lastQty = qty; f.body.lastPx = px;
        f.body.execType = last ? ExecType::Fill : ExecType::PartialFill; send(f, 7);
    }
};
static constexpr int64_t USD = 100'000'000;   // 1e-8 units

int main(int argc, char** argv) {
    if (argc != 3) return 2;
    fs::create_directories(argv[2]); std::string jp = std::string(argv[2]) + "/risk.jnl"; fs::remove_all(jp);
    Snapshot snap(argv[1]); RefData rd; rd.load(snap);
    auto eng = std::make_unique<RiskEngine>(rd); eng->load(snap);
    CHECK(eng->accountKnown(42) && !eng->accountKnown(7));                                   // snapshot accounts section
    FakeClock clock; BroadcastRing ring(1 << 12); Journal j(jp, 0); Sequencer seq(0, 1, j, ring, clock, 0);
    int rid = ring.subscribe();
    seq.openSession(snap.businessDate(), snap.version(), snap.contentHash());
    Harness h{j, seq, *eng, ring, rid};
    auto drain = [&] { h.drain(); };
    const uint32_t AAPL = 1, MSFT = 2, GS = 4, MS = 5, GME = 7, TSLA = 8, XYZQ = 9, NVDA = 10;
    const int64_t ap = rd.refPrice(AAPL);

    // ---- session setup, all as sequenced messages
    h.limits(0, 0, 100000, 50'000'000 * USD, 0, 0, 1000, 0, 0);                              // firm
    h.limits(42, 0, 5000, 1'000'000 * USD, 5'000'000 * USD, 3'000'000 * USD, 500, 50, 10);   // account 42
    h.buyingPower(42, 10'000'000 * USD); h.buyingPower(7, 100'000 * USD);
    { Frame<LocateGranted> l; l.init(); l.body.locateId = 1; l.body.accountIdx = 42; l.body.symbolIdx = GME; l.body.qty = 500; l.body.sourceId = 77; l.body.result = LocateResult::Granted; l.body.expiryTs = INT64_MAX; h.send(l, 10);
      l.body.locateId = 2; l.body.symbolIdx = AAPL; h.send(l, 10);
      l.body.locateId = 3; l.body.symbolIdx = GME; l.body.expiryTs = 1; h.send(l, 10); }
    { Frame<ShortSaleRestriction> r; r.init(); r.body.symbolIdx = TSLA; r.body.active = 1; h.send(r, 8); }
    { Frame<Nbbo> n; n.init(); n.body.symbolIdx = TSLA; n.body.bid = rd.refPrice(TSLA); n.body.ask = rd.refPrice(TSLA) + 1'000'000; h.send(n, 8); }
    { Frame<SymbolStatus> st; st.init(); st.body.symbolIdx = GS; st.body.status = TradingStatus::Halted; h.send(st, 8); }
    { Frame<ListUpdate> lu; lu.init(); lu.body.listId = ListId::Restricted; lu.body.op = ListOp::Add; lu.body.symbolIdx = MS; h.send(lu, 12); }
    { Frame<SymbolUpdate> su; su.init(); su.body.symbolIdx = XYZQ; su.body.mask = SymbolUpdateMask::status; su.body.status = SymbolStatusCode::Suspended; h.send(su, 15);
      su.body.symbolIdx = NVDA; su.body.mask = SymbolUpdateMask::flags; su.body.flags = SymbolFlags::marginable; h.send(su, 15); }
    drain();

    // ---- one case per reason
    uint64_t oid = 0;
    CHECK(h.order(h.mk(42, AAPL, Side::Buy, 100, ap), &oid) == Reason::NoReason);
    { NewOrder o = h.mk(42, AAPL, Side::Buy, 100, ap); o.clOrdId = 1; CHECK(h.order(o) == Reason::DuplicateClOrdId); }
    CHECK(h.order(h.mk(999, AAPL, Side::Buy, 100, ap)) == Reason::UnknownAccount);
    CHECK(h.order(h.mk(42, 60000, Side::Buy, 100, ap)) == Reason::UnknownSymbol);
    CHECK(h.order(h.mk(42, XYZQ, Side::Buy, 100, rd.refPrice(XYZQ))) == Reason::SymbolNotTradable);
    CHECK(h.order(h.mk(42, GS, Side::Buy, 100, rd.refPrice(GS))) == Reason::SymbolHalted);
    CHECK(h.order(h.mk(42, MS, Side::Buy, 100, rd.refPrice(MS))) == Reason::SymbolRestricted);
    CHECK(h.order(h.mk(42, AAPL, Side::Unset, 100, ap)) == Reason::BadSide);
    CHECK(h.order(h.mk(42, AAPL, Side::Buy, 0, ap)) == Reason::QtyZero);
    { NewOrder o = h.mk(42, AAPL, Side::Buy, 100, ap); o.minQty = 200; CHECK(h.order(o) == Reason::MinQtyExceedsQty); }
    CHECK(h.order(h.mk(42, AAPL, Side::Buy, 6000, ap)) == Reason::MaxOrderQty);
    CHECK(h.order(h.mk(42, MSFT, Side::Buy, 4000, rd.refPrice(MSFT))) == Reason::MaxOrderNotional);
    CHECK(h.order(h.mk(42, AAPL, Side::Buy, 100, 0)) == Reason::MissingPrice);
    CHECK(h.order(h.mk(42, AAPL, Side::Buy, 100, ap + 5000)) == Reason::PriceNotOnTick);
    CHECK(h.order(h.mk(42, AAPL, Side::Buy, 100, ap + 30 * USD)) == Reason::PriceOutsideCollar);
    CHECK(h.order(h.mk(42, NVDA, Side::SellShort, 100, rd.refPrice(NVDA))) == Reason::ShortNotShortable);
    CHECK(h.order(h.mk(42, GME, Side::SellShort, 100, rd.refPrice(GME))) == Reason::LocateRequired);
    CHECK(h.order(h.mk(42, GME, Side::SellShort, 100, rd.refPrice(GME), 999)) == Reason::LocateUnknown);
    CHECK(h.order(h.mk(42, GME, Side::SellShort, 600, rd.refPrice(GME), 1)) == Reason::LocateInsufficient);
    CHECK(h.order(h.mk(42, GME, Side::SellShort, 100, rd.refPrice(GME), 2)) == Reason::LocateWrongSymbol);
    CHECK(h.order(h.mk(42, GME, Side::SellShort, 100, rd.refPrice(GME), 3)) == Reason::LocateExpired);
    CHECK(h.order(h.mk(42, GME, Side::SellShort, 300, rd.refPrice(GME), 1), &oid) == Reason::NoReason);
    CHECK(eng->locateRemaining(1) == 200);
    h.cancel(oid); CHECK(eng->locateRemaining(1) == 500);                                  // cancel returns the locate
    CHECK(h.order(h.mk(42, TSLA, Side::SellShort, 100, rd.refPrice(TSLA))) == Reason::SsrPriceTest);   // at the bid
    CHECK(h.order(h.mk(42, TSLA, Side::SellShort, 100, rd.refPrice(TSLA) + 1'000'000), &oid) == Reason::NoReason);
    h.cancel(oid);
    CHECK(h.order(h.mk(7, AAPL, Side::Buy, 1000, ap)) == Reason::InsufficientBuyingPower);  // $227k x 50% > $100k
    CHECK(h.order(h.mk(7, AAPL, Side::Buy, 100, ap), &oid) == Reason::NoReason);
    CHECK(eng->buyingPower(7) == 100'000 * USD - (100 * ap) / 2);
    h.cancel(oid); CHECK(eng->buyingPower(7) == 100'000 * USD);

    // ---- exposure: net then gross, with cancels restoring everything
    h.cancel((uint64_t(2) << 48) | 1);                                                       // close the first accept
    CHECK(eng->openOrders(42) == 0);
    int64_t q900 = (900'000 * USD) / ap; q900 -= q900 % 100;                                 // ~$900k of AAPL
    std::vector<uint64_t> ids;
    for (int i = 0; i < 3; ++i) { CHECK(h.order(h.mk(42, AAPL, Side::Buy, q900, ap), &oid) == Reason::NoReason); ids.push_back(oid); }
    CHECK(h.order(h.mk(42, AAPL, Side::Buy, q900, ap)) == Reason::NetExposure);              // 4 x 0.9M > 3M net
    CHECK(h.order(h.mk(42, AAPL, Side::Sell, q900, ap), &oid) == Reason::NoReason); ids.push_back(oid);   // gross 3.6M, net 1.8M
    CHECK(h.order(h.mk(42, AAPL, Side::Sell, q900, ap), &oid) == Reason::NoReason); ids.push_back(oid);   // gross 4.5M, net 0.9M
    CHECK(h.order(h.mk(42, AAPL, Side::Sell, q900, ap)) == Reason::GrossExposure);            // 5.4M > 5M
    for (auto id : ids) h.cancel(id);
    CHECK(eng->openOrders(42) == 0 && eng->grossExposure(42) == 0 && eng->buyingPower(42) == 10'000'000 * USD);

    // ---- fills move open notional into position, buying power stays committed
    CHECK(h.order(h.mk(42, AAPL, Side::Buy, 200, ap), &oid) == Reason::NoReason);
    h.fill(oid, 100, ap, false); h.fill(oid, 100, ap, true);
    CHECK(eng->position(42, AAPL) == 200 * ap && eng->grossExposure(42) == 200 * ap && eng->openOrders(42) == 0);
    CHECK(eng->buyingPower(42) == 10'000'000 * USD - (200 * ap) / 2);
    h.buyingPower(42, 10'000'000 * USD);                                                      // margin engine re-bases

    // ---- open orders cap, then message rate (same sequencer second)
    ids.clear();
    for (int i = 0; i < 10; ++i) { CHECK(h.order(h.mk(42, AAPL, Side::Buy, 100, ap), &oid) == Reason::NoReason); ids.push_back(oid); }
    CHECK(h.order(h.mk(42, AAPL, Side::Buy, 100, ap)) == Reason::MaxOpenOrders);
    for (auto id : ids) h.cancel(id);
    clock.t += 1'000'000'000LL;                                                               // new rate window
    int accepted = 0, rateRejected = 0, openRejected = 0;
    for (int i = 0; i < 60; ++i) {
        Reason r = h.order(h.mk(42, AAPL, Side::Buy, 100, ap));
        if (r == Reason::NoReason) ++accepted; else if (r == Reason::MsgRateExceeded) ++rateRejected; else if (r == Reason::MaxOpenOrders) ++openRejected;
    }
    CHECK(accepted == 10 && openRejected == 40 && rateRejected == 10);
    std::printf("rate window: 10 accepted, 40 open-order rejects, 10 rate rejects\n");

    // ---- kill switches
    { Frame<KillSwitch> k; k.init(); k.body.accountIdx = 42; h.send(k, 13); }
    CHECK(h.order(h.mk(42, AAPL, Side::Buy, 100, ap)) == Reason::AccountKilled);
    { Frame<KillSwitch> k; k.init(); h.send(k, 13); }
    CHECK(h.order(h.mk(7, AAPL, Side::Buy, 100, ap)) == Reason::FirmKilled);
    { Frame<KillSwitch> k; k.init(); k.body.release = 1; h.send(k, 13); k.body.accountIdx = 42; h.send(k, 13); }   // lift both
    { Frame<KillSwitch> k; k.init(); k.body.strategyId = 5; h.send(k, 13); }
    { NewOrder o = h.mk(42, AAPL, Side::Buy, 100, ap); o.strategyId = 5; CHECK(h.order(o) == Reason::StrategyKilled); }
    { Frame<KillSwitch> k; k.init(); k.body.strategyId = 5; k.body.release = 1; h.send(k, 13); }
    CHECK(h.order(h.mk(7, AAPL, Side::Buy, 100, ap), &oid) == Reason::NoReason); h.cancel(oid);
    // ---- duplicate-order window (v3): identical symbol/side/qty/price within 100 ms
    h.buyingPower(8, 1'000'000 * USD); h.limits(8, 0, 0, 0, 0, 0, 0, 0, 0, 100);
    CHECK(h.order(h.mk(8, AAPL, Side::Buy, 100, ap), &oid) == Reason::NoReason);
    CHECK(h.order(h.mk(8, AAPL, Side::Buy, 100, ap)) == Reason::DuplicateOrder);
    CHECK(h.order(h.mk(8, AAPL, Side::Buy, 200, ap), &oid) == Reason::NoReason);              // different qty is not a duplicate
    clock.t += 200'000'000LL;
    CHECK(h.order(h.mk(8, AAPL, Side::Buy, 100, ap), &oid) == Reason::NoReason);              // window passed
    int64_t bp8 = eng->buyingPower(8);

    // ---- replace: unknown, qty up (more buying power), price off tick (state unchanged), qty down, qty at or below filled
    CHECK(h.replace(0xDEAD, 8, 100, ap) == Reason::UnknownOrder);
    CHECK(h.replace(oid, 8, 300, ap) == Reason::NoReason);                                    // 100 -> 300
    CHECK(eng->openLeaves(oid) == 300 && eng->buyingPower(8) == bp8 - (200 * ap) / 2);
    CHECK(h.replace(oid, 8, 300, ap + 5000) == Reason::PriceNotOnTick);
    CHECK(eng->openLeaves(oid) == 300 && eng->buyingPower(8) == bp8 - (200 * ap) / 2);        // reject left it untouched
    CHECK(h.replace(oid, 8, 50, ap) == Reason::NoReason);                                     // 300 -> 50 releases
    CHECK(eng->openLeaves(oid) == 50 && eng->buyingPower(8) == bp8 + (50 * ap) / 2);
    h.fill(oid, 30, ap, false);
    CHECK(h.replace(oid, 8, 30, ap) == Reason::QtyZero);                                      // nothing would be left open
    CHECK(h.replace(oid, 8, 40, ap) == Reason::NoReason && eng->openLeaves(oid) == 10);
    CHECK(h.replace(oid, 8, 40'000'000, ap) == Reason::InsufficientBuyingPower && eng->openLeaves(oid) == 10);
    std::printf("duplicate window and replace paths ok\n");

    std::printf("all %d reject reasons exercised; accepts=%llu rejects=%llu\n", 29, (unsigned long long)eng->accepts(), (unsigned long long)eng->rejects());
    drain();

    // ---- firm-scale exposure: the aggregate ceiling used to be $92bn, which is the whole point of CR-1.
    // Fifteen orders of about $10bn each, on a limit that can only be expressed at 1e-4 units.
    {
        const uint32_t FIRM = 1;
        h.buyingPower(FIRM, 90'000'000'000LL * USD / 1000 * 1000);            // just under the per-account cap
        h.limitsWide(FIRM, 100'000'000, 20'000'000'000LL * USD, 500'000'000'000LL * 10'000, 500'000'000'000LL * 10'000);
        int64_t big = 10'000'000'000LL * USD / ap;                             // ~$10bn of AAPL, in shares
        big -= big % 100;
        int accepted2 = 0;
        for (int i = 0; i < 15; ++i) if (h.order(h.mk(FIRM, AAPL, Side::Buy, big, ap)) == Reason::NoReason) ++accepted2;
        money::i128 gross = eng->grossExposure(FIRM);
        std::printf("firm scale: %d of 15 accepted, gross exposure $%.1f bn (the old int64 ceiling was $92.2 bn)\n",
                    accepted2, double(gross) / 1e8 / 1e9);
        CHECK(accepted2 == 15);
        CHECK(gross > money::MAX_NANO);                                        // would have overflowed or been rejected before
        CHECK(money::reportable(gross));
        // and an ExposureSnapshot can carry it: the unit says so
        MoneyScale sc = MoneyScale::Unset; int64_t wire = money::toWire(gross, sc);
        CHECK(sc == MoneyScale::Unit1e4 && money::toNano(wire, sc) / 10000 == gross / 10000);
        // beyond what a snapshot could report, the engine refuses rather than wrapping
        CHECK(!money::reportable(money::MAX_REPORTABLE + 1));
    }

    // ---- random flow on a fresh account, then cold replay must reproduce every verdict
    h.buyingPower(500, 50'000'000 * USD); h.limits(500, 0, 10000, 5'000'000 * USD, 200'000'000 * USD, 100'000'000 * USD, 300, 0, 500);
    std::mt19937_64 rng(99); std::vector<uint64_t> live;
    auto t0 = std::chrono::steady_clock::now();
    const int N = 200000; int acc = 0;
    for (int i = 0; i < N; ++i) {
        uint32_t sym = uint32_t(1 + rng() % 10); int64_t ref = rd.refPrice(sym);
        int64_t px = ref + (int64_t(rng() % 200) - 100) * 1'000'000;
        NewOrder o = h.mk(500, sym, (rng() % 5 == 0) ? Side::SellShort : (rng() & 1 ? Side::Buy : Side::Sell), int64_t(1 + rng() % 500) * 10, px);
        if (rng() % 7 == 0) o.price += 3;  // off-tick
        if (h.order(o, &oid) == Reason::NoReason) { live.push_back(oid); ++acc; }
        if (rng() % 5 == 0 && !live.empty()) {
            size_t k = rng() % live.size(); int64_t q = int64_t(1 + rng() % 600) * 10;
            if (h.replace(live[k], 500, q, ref + (int64_t(rng() % 100) - 50) * 1'000'000) == Reason::UnknownOrder) { live[k] = live.back(); live.pop_back(); }
        }
        if (live.size() > 200 || (rng() % 3 == 0 && !live.empty())) {
            size_t k = rng() % live.size(); h.cancel(live[k]); live[k] = live.back(); live.pop_back();
        }
        if ((i & 4095) == 0) drain();
    }
    auto t1 = std::chrono::steady_clock::now();
    double nsPer = double(std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count()) / N;
    auto liveHash = eng->stateHash();
    std::printf("random flow: %d orders, %d accepted, %.0f ns per order incl. sequencing both frames\n", N, acc, nsPer);

    {   // cold replay: a second engine over the journal; its evaluate() must match every sequenced RiskDecision
        RefData rd2; rd2.load(snap); auto cold = std::make_unique<RiskEngine>(rd2); cold->load(snap);
        NewOrder last{}; ReplaceOrder lastRep{}; bool isReplace = false; FrameHeader lastHdr{}; uint64_t checked = 0, mismatch = 0;
        j.replay(1, 0, [&](const FrameHeader* f) {
            if (auto* o = as<NewOrder>(f)) { last = *o; lastHdr = *f; isReplace = false; return; }
            if (auto* r = as<ReplaceOrder>(f)) { lastRep = *r; lastHdr = *f; isReplace = true; return; }
            if (auto* d = as<RiskDecision>(f)) {
                RiskDecision mine{}; if (isReplace) cold->evaluateReplace(&lastHdr, lastRep, mine); else cold->evaluate(&lastHdr, last, mine); ++checked;
                if (mine.verdict != d->verdict || mine.reason != d->reason || mine.buyingPowerAfter != d->buyingPowerAfter) ++mismatch;
                return;
            }
            cold->apply(f);
        });
        std::printf("cold replay: %llu verdicts checked, %llu mismatches, state hash %s\n", (unsigned long long)checked,
            (unsigned long long)mismatch, cold->stateHash() == liveHash ? "identical" : "DIFFERENT");
        CHECK(mismatch == 0 && cold->stateHash() == liveHash && checked == eng->accepts() + eng->rejects());
    CHECK(acc > N / 4);
    }
    std::printf("risk tests ok\n");
    return 0;
}
