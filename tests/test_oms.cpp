// tests/test_oms.cpp <snapshot> <workdir>
// The order manager: the transition table's invariants, every lifecycle on purpose (accept, fills
// with fees and NBBO, cancel, cancel reject, venue reject with re-route and give-up, IOC, replace
// accepted and rejected, kill switch, bust), illegal events, then random flow and a replay that
// must regenerate every client report byte for byte and reach the same state hash.
#include "oms.hpp"
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <random>
#include <deque>

using namespace trading; using namespace trading::oms;
#define CHECK(c) do { if (!(c)) { std::fprintf(stderr, "CHECK failed %s:%d: %s\n", __FILE__, __LINE__, #c); return 1; } } while (0)

// A sequenced log: inputs are pushed, the OMS reacts, its outputs are sequenced and fed back (as the
// stream would), recursively, so the OMS sees exactly what it would see in production.
struct Log {
    Oms& oms; std::vector<std::vector<std::byte>> frames; std::vector<std::vector<std::byte>> outputs; uint64_t seq = 0;
    explicit Log(Oms& o) : oms(o) {}
    const FrameHeader* at(size_t i) const { return reinterpret_cast<const FrameHeader*>(frames[i].data()); }
    template <class M> Frame<M>& stamp(Frame<M>& f, uint16_t src) { f.header.sourceId = src; return f; }
    void push(FrameHeader* f) {
        f->seq = ++seq; f->seqTs = int64_t(1'700'000'000'000'000'000LL) + int64_t(seq) * 250;
        const auto* p = reinterpret_cast<const std::byte*>(f); frames.emplace_back(p, p + f->frameLength);
        std::deque<std::vector<std::byte>> pending;
        oms.apply(at(frames.size() - 1), [&](FrameHeader* o) { const auto* q = reinterpret_cast<const std::byte*>(o); pending.emplace_back(q, q + o->frameLength); });
        while (!pending.empty()) {
            std::vector<std::byte> b = std::move(pending.front()); pending.pop_front();
            auto* h = reinterpret_cast<FrameHeader*>(b.data()); h->seq = ++seq; h->seqTs = int64_t(1'700'000'000'000'000'000LL) + int64_t(seq) * 250;
            frames.push_back(b); outputs.push_back(b);
            oms.apply(at(frames.size() - 1), [&](FrameHeader* o) { const auto* q = reinterpret_cast<const std::byte*>(o); pending.emplace_back(q, q + o->frameLength); });
        }
    }
    // last output of a type
    template <class M> const M* last() const { for (size_t i = outputs.size(); i-- > 0;) if (auto* m = as<M>(reinterpret_cast<const FrameHeader*>(outputs[i].data()))) return m; return nullptr; }
    template <class M> size_t count() const { size_t n = 0; for (auto& o : outputs) if (as<M>(reinterpret_cast<const FrameHeader*>(o.data()))) ++n; return n; }
    const FrameHeader* lastHeader() const { return reinterpret_cast<const FrameHeader*>(outputs.back().data()); }
};

struct Driver {
    Log& log; uint64_t nextOrder = 1, nextChild = 1, cl = 1;
    uint64_t newOrder(uint32_t sym, Side side, int64_t qty, int64_t px, Tif tif = Tif::Day) {
        Frame<NewOrder> f; f.init(); f.header.sourceId = 2; f.body.orderId = (uint64_t(2) << 48) | nextOrder++; f.body.clOrdId = cl++;
        f.body.accountIdx = 42; f.body.symbolIdx = sym; f.body.side = side; f.body.ordType = OrdType::Limit; f.body.tif = tif; f.body.qty = qty; f.body.price = px; f.body.sessionId = 9; f.body.clientTag = 77;
        log.push(&f.header); return f.body.orderId;
    }
    void risk(uint64_t oid, bool accept, uint16_t reason = 0, bool replace = false) {
        Frame<RiskDecision> f; f.init(); f.header.sourceId = 3; f.body.orderId = oid; f.body.accountIdx = 42; f.body.verdict = accept ? RiskVerdict::Accept : RiskVerdict::Reject; f.body.reason = reason;
        f.body.checkMask = replace ? (1u << 16) : 0; log.push(&f.header);
    }
    uint64_t child(uint64_t parent, int64_t qty, int64_t px, uint16_t session = 0, Tif tif = Tif::Day) {
        const Order* p = log.oms.find(parent);
        Frame<ChildOrder> f; f.init(); f.header.sourceId = 3; f.body.childOrderId = (uint64_t(3) << 48) | nextChild++; f.body.parentOrderId = parent;
        f.body.accountIdx = 42; f.body.symbolIdx = p ? p->symbolIdx : 1; f.body.venueId = 2; f.body.venueSessionIdx = session; f.body.side = p ? p->side : Side::Buy;
        f.body.ordType = OrdType::Limit; f.body.tif = tif; f.body.qty = qty; f.body.price = px; log.push(&f.header); return f.body.childOrderId;
    }
    void ack(uint64_t childId) { Frame<VenueAck> f; f.init(); f.header.sourceId = 7; f.body.childOrderId = childId; f.body.venueId = 2; log.push(&f.header); }
    void venueReject(uint64_t childId, uint16_t reason) { Frame<VenueReject> f; f.init(); f.header.sourceId = 7; f.body.childOrderId = childId; f.body.venueId = 2; f.body.reason = reason; log.push(&f.header); }
    void venueExec(uint64_t parent, uint64_t childId, ExecType t, int64_t qty, int64_t px, Liquidity liq = Liquidity::Removed, uint16_t reason = 0) {
        Frame<ExecReport> f; f.init(); f.header.sourceId = 7; f.body.orderId = parent; f.body.childOrderId = childId; f.body.execType = t; f.body.lastQty = qty; f.body.lastPx = px;
        f.body.venueId = 2; f.body.liquidityFlag = liq; f.body.rejectReason = reason; std::memcpy(f.body.venueExecId, "SIM0000000000000001", 20); f.body.venueTs = 5; log.push(&f.header);
    }
    void cancel(uint64_t oid, uint64_t origCl = 0) { Frame<CancelOrder> f; f.init(); f.header.sourceId = 2; f.body.orderId = oid; f.body.origClOrdId = origCl; f.body.clOrdId = cl++; f.body.accountIdx = 42; f.body.sessionId = 9; log.push(&f.header); }
    void replace(uint64_t oid, int64_t qty, int64_t px) { Frame<ReplaceOrder> f; f.init(); f.header.sourceId = 2; f.body.orderId = oid; f.body.clOrdId = cl++; f.body.accountIdx = 42; f.body.sessionId = 9; f.body.qty = qty; f.body.price = px; log.push(&f.header); }
    void nbbo(uint32_t sym, int64_t bid, int64_t ask) { Frame<Nbbo> f; f.init(); f.header.sourceId = 8; f.body.symbolIdx = sym; f.body.bid = bid; f.body.ask = ask; log.push(&f.header); }
    void kill(uint32_t acct = 0) { Frame<KillSwitch> f; f.init(); f.header.sourceId = 13; f.body.accountIdx = acct; log.push(&f.header); }
};

int main(int argc, char** argv) {
    if (argc != 3) return 2;
    std::filesystem::create_directories(argv[2]);
    refdata::Snapshot snap(argv[1]);

    // ---- table invariants
    for (size_t s = 0; s < STATUS_COUNT; ++s) for (size_t e = 0; e < size_t(Event::COUNT); ++e) {
        OrdStatus from = OrdStatus(s), to = TABLE[s][e];
        if (terminal(from)) CHECK(to == U || (from == F && Event(e) == Event::Bust));                  // terminal states stay terminal
        if (to != U && terminal(to)) CHECK(Event(e) != Event::ChildAck && Event(e) != Event::PartialFill);   // acks and partials never end an order
    }
    CHECK(next(PN, Event::RiskAccept) == L && next(PN, Event::RiskReject) == R && next(L, Event::CancelRequest) == PC && next(PC, Event::CancelAck) == C);
    for (OrdStatus s : {L, PF, PR}) CHECK(next(s, Event::Kill) == PC);
    CHECK(next(PN, Event::Kill) == R);

    Oms oms(4, 1 << 16); oms.load(snap); Log log(oms); Driver d{log};
    const uint32_t AAPL = 1; const int64_t px = 227'45'000'000LL;
    d.nbbo(AAPL, px - 1'000'000, px + 1'000'000);

    // ---- accept, child, ack, partial fill with fee and NBBO, fill, release
    uint64_t o1 = d.newOrder(AAPL, Side::Buy, 300, px);
    CHECK(oms.find(o1) && oms.find(o1)->status == PN);
    d.risk(o1, true);
    { auto* r = log.last<ExecReport>(); CHECK(r && r->execType == ExecType::New && r->ordStatus == L && r->leavesQty == 300 && r->clientTag == 77); }
    { auto* s = log.last<OrderState>(); CHECK(s && s->status == L && s->prevStatus == PN); }
    uint64_t c1 = d.child(o1, 300, px); d.ack(c1);
    CHECK(oms.find(o1)->status == L && oms.find(o1)->liveChildren == 1 && oms.find(c1) && oms.find(c1)->status == L);
    d.venueExec(o1, c1, ExecType::PartialFill, 100, px, Liquidity::Removed);
    { auto* r = log.last<ExecReport>(); CHECK(r && r->childOrderId == 0 && r->execType == ExecType::PartialFill && r->ordStatus == PF && r->lastQty == 100 && r->leavesQty == 200 && r->cumQty == 100);
      CHECK(r->fee == 100 * 300000 && r->feeCode == 1 && r->nbboBid == px - 1'000'000 && r->nbboAsk == px + 1'000'000 && r->avgPx == px && r->venueId == 2 && std::memcmp(r->venueExecId, "SIM", 3) == 0); }
    d.venueExec(o1, c1, ExecType::Fill, 200, px + 1'000'000, Liquidity::Added);
    { auto* r = log.last<ExecReport>(); CHECK(r && r->execType == ExecType::Fill && r->ordStatus == F && r->leavesQty == 0 && r->cumQty == 300 && r->fee == 200 * -200000 && r->feeCode == 2);
      CHECK(r->avgPx == (100 * px + 200 * (px + 1'000'000)) / 300); }
    CHECK(oms.find(o1) == nullptr && oms.find(c1) == nullptr && oms.live() == 0);                  // released with its child
    { auto* s = log.last<OrderState>(); CHECK(s && s->status == F); }

    // ---- risk reject
    uint64_t o2 = d.newOrder(AAPL, Side::Buy, 100, px); d.risk(o2, false, uint16_t(Reason::MaxOrderQty));
    { auto* r = log.last<ExecReport>(); CHECK(r && r->execType == ExecType::Rejected && r->ordStatus == R && r->rejectReason == uint16_t(Reason::MaxOrderQty)); }
    CHECK(oms.find(o2) == nullptr && oms.rejected() == 1);

    // ---- client cancel: pending, child cancel addressed to the venue, confirmation, then cancel rejects
    uint64_t o3 = d.newOrder(AAPL, Side::Sell, 500, px); d.risk(o3, true); uint64_t c3 = d.child(o3, 500, px); d.ack(c3);
    size_t before = log.count<CancelOrder>();
    d.cancel(o3);
    CHECK(oms.find(o3)->status == PC && log.count<CancelOrder>() == before + 1);
    { auto* k = log.last<CancelOrder>(); CHECK(k && k->orderId == c3 && log.lastHeader()->sourceId == 4); }
    d.cancel(o3);                                                                                    // second cancel while pending
    { auto* r = log.last<ExecReport>(); CHECK(r && r->execType == ExecType::Rejected && r->ordStatus == PC && r->rejectReason == uint16_t(Reason::OrderNotLive)); }
    d.venueExec(o3, c3, ExecType::Cancelled, 0, 0, Liquidity::Unset);
    { auto* r = log.last<ExecReport>(); CHECK(r && r->execType == ExecType::Cancelled && r->ordStatus == C && r->leavesQty == 500); }
    CHECK(oms.find(o3) == nullptr);
    d.cancel(0xBAD);
    { auto* r = log.last<ExecReport>(); CHECK(r && r->execType == ExecType::Rejected && r->rejectReason == uint16_t(Reason::UnknownOrder)); }
    // cancel by original clOrdId
    uint64_t o4 = d.newOrder(AAPL, Side::Buy, 100, px); d.risk(o4, true); uint64_t c4 = d.child(o4, 100, px); d.ack(c4);
    d.cancel(0, oms.find(o4)->clOrdId); CHECK(oms.find(o4)->status == PC);
    d.venueExec(o4, c4, ExecType::Cancelled, 0, 0); CHECK(oms.find(o4) == nullptr);
    // cancel racing the router: the parent is accepted but its child is not sequenced yet; the cancel
    // waits, the child is cancelled on arrival, and the venue's confirmation completes it
    uint64_t o4b = d.newOrder(AAPL, Side::Buy, 100, px); d.risk(o4b, true);
    d.cancel(o4b); CHECK(oms.find(o4b) && oms.find(o4b)->status == PC);
    before = log.count<CancelOrder>();
    uint64_t c4b = d.child(o4b, 100, px);
    { auto* k = log.last<CancelOrder>(); CHECK(log.count<CancelOrder>() == before + 1 && k && k->orderId == c4b); }
    d.ack(c4b); d.venueExec(o4b, c4b, ExecType::Cancelled, 0, 0); CHECK(oms.find(o4b) == nullptr);
    // the router declines: a waiting cancel completes without a child
    uint64_t o4c = d.newOrder(AAPL, Side::Buy, 100, px); d.risk(o4c, true); d.cancel(o4c);
    { Frame<RouteDecision> f; f.init(); f.header.sourceId = 3; f.body.parentOrderId = o4c; f.body.childCount = 0; f.body.reason = uint16_t(Reason::SymbolHalted); log.push(&f.header); }
    { auto* r = log.last<ExecReport>(); CHECK(r && r->execType == ExecType::Cancelled && r->orderId == o4c); } CHECK(oms.find(o4c) == nullptr);
    // the router declines a parent nobody cancelled: the client is told
    uint64_t o4d = d.newOrder(AAPL, Side::Buy, 100, px); d.risk(o4d, true);
    { Frame<RouteDecision> f; f.init(); f.header.sourceId = 3; f.body.parentOrderId = o4d; f.body.childCount = 0; f.body.reason = uint16_t(Reason::SymbolHalted); log.push(&f.header); }
    { auto* r = log.last<ExecReport>(); CHECK(r && r->execType == ExecType::Rejected && r->rejectReason == uint16_t(Reason::SymbolHalted)); } CHECK(oms.find(o4d) == nullptr);
    std::printf("accept, fills with fee and nbbo, reject, cancel paths ok\n");

    // ---- venue reject: re-route three times, then the order is rejected to the client
    uint64_t o5 = d.newOrder(AAPL, Side::Buy, 100, px); d.risk(o5, true);
    for (int i = 0; i < 3; ++i) {
        uint64_t c = d.child(o5, 100, px); d.venueReject(c, uint16_t(Reason::OrderNotLive));
        auto* s = log.last<OrderState>(); CHECK(s && s->status == L && s->reason == uint16_t(Reason::RerouteRequired) && s->leavesQty == 100);
    }
    { uint64_t c = d.child(o5, 100, px); d.venueReject(c, uint16_t(Reason::OrderNotLive)); }
    { auto* r = log.last<ExecReport>(); CHECK(r && r->execType == ExecType::Rejected && r->ordStatus == R && r->rejectReason == uint16_t(Reason::OrderNotLive)); }
    CHECK(oms.find(o5) == nullptr && oms.reroutes() == 3);
    // venue-initiated cancel (session drop) with a fill already: re-route, then give up as Cancelled
    uint64_t o6 = d.newOrder(AAPL, Side::Buy, 200, px); d.risk(o6, true); uint64_t c6 = d.child(o6, 200, px); d.ack(c6);
    d.venueExec(o6, c6, ExecType::PartialFill, 50, px);
    d.venueExec(o6, c6, ExecType::Cancelled, 0, 0, Liquidity::Unset, 2);
    { auto* s = log.last<OrderState>(); CHECK(s && s->reason == uint16_t(Reason::RerouteRequired) && s->leavesQty == 150 && s->cumQty == 50 && s->status == PF); }
    for (int i = 0; i < 3; ++i) { uint64_t c = d.child(o6, 150, px); d.ack(c); d.venueExec(o6, c, ExecType::Cancelled, 0, 0, Liquidity::Unset, 2); }
    { auto* r = log.last<ExecReport>(); CHECK(r && r->execType == ExecType::Cancelled && r->cumQty == 50 && r->leavesQty == 150); }
    CHECK(oms.find(o6) == nullptr);
    // IOC: the venue cancels the remainder and the parent is done
    uint64_t o7 = d.newOrder(AAPL, Side::Buy, 100, px, Tif::Ioc); d.risk(o7, true); uint64_t c7 = d.child(o7, 100, px, 0, Tif::Ioc); d.ack(c7);
    d.venueExec(o7, c7, ExecType::Cancelled, 0, 0);
    { auto* r = log.last<ExecReport>(); CHECK(r && r->execType == ExecType::Cancelled); } CHECK(oms.find(o7) == nullptr);
    std::printf("re-route, give-up, ioc paths ok\n");

    // ---- replace: pending, accepted (children cancelled, re-planned at the new shape), rejected (back to Live)
    uint64_t o8 = d.newOrder(AAPL, Side::Buy, 100, px); d.risk(o8, true); uint64_t c8 = d.child(o8, 100, px); d.ack(c8);
    d.replace(o8, 300, px - 1'000'000);
    CHECK(oms.find(o8)->status == PR);
    d.risk(o8, true, 0, true);
    { auto* r = log.last<ExecReport>(); CHECK(r && r->execType == ExecType::Replaced); }
    { auto* k = log.last<CancelOrder>(); CHECK(k && k->orderId == c8); }
    d.venueExec(o8, c8, ExecType::Cancelled, 0, 0);
    { auto* s = log.last<OrderState>(); CHECK(s && s->status == L && s->prevStatus == PR && s->reason == uint16_t(Reason::RerouteRequired) && s->leavesQty == 300); }
    CHECK(oms.find(o8)->qty == 300 && oms.find(o8)->price == px - 1'000'000 && oms.replaced() == 1);
    uint64_t c8b = d.child(o8, 300, px - 1'000'000); d.ack(c8b);
    d.replace(o8, 50, 0); d.risk(o8, false, uint16_t(Reason::QtyZero), true);
    { auto* r = log.last<ExecReport>(); CHECK(r && r->execType == ExecType::Rejected && r->ordStatus == L && r->rejectReason == uint16_t(Reason::QtyZero)); }
    CHECK(oms.find(o8)->status == L && oms.find(o8)->qty == 300);
    d.cancel(o8); d.venueExec(o8, c8b, ExecType::Cancelled, 0, 0); CHECK(oms.find(o8) == nullptr);
    std::printf("replace paths ok\n");

    // ---- kill switch: every live parent for the scope goes to pending cancel, children are cancelled
    uint64_t o9 = d.newOrder(AAPL, Side::Buy, 100, px); d.risk(o9, true); uint64_t c9 = d.child(o9, 100, px); d.ack(c9);
    uint64_t o10 = d.newOrder(AAPL, Side::Sell, 100, px); d.risk(o10, true);                          // accepted, not yet routed
    uint64_t o11 = d.newOrder(AAPL, Side::Sell, 100, px);                                             // not yet decided
    before = log.count<CancelOrder>();
    d.kill();
    CHECK(log.count<CancelOrder>() == before + 1 && oms.find(o9)->status == PC);
    CHECK(oms.find(o10) && oms.find(o10)->status == PC && oms.find(o11) == nullptr);              // unrouted: waits for the router; undecided: rejected
    { Frame<RouteDecision> f; f.init(); f.header.sourceId = 3; f.body.parentOrderId = o10; f.body.childCount = 0; log.push(&f.header); }
    CHECK(oms.find(o10) == nullptr);
    d.venueExec(o9, c9, ExecType::Cancelled, 0, 0); CHECK(oms.find(o9) == nullptr && oms.live() == 0);

    // ---- bust reverses a partial fill on an open parent
    uint64_t o12 = d.newOrder(AAPL, Side::Buy, 200, px); d.risk(o12, true); uint64_t c12 = d.child(o12, 200, px); d.ack(c12);
    d.venueExec(o12, c12, ExecType::PartialFill, 100, px);
    d.venueExec(o12, c12, ExecType::TradeBust, 100, px);
    CHECK(oms.find(o12)->cum == 0 && oms.find(o12)->leaves == 200 && oms.find(o12)->status == PF);
    { auto* r = log.last<ExecReport>(); CHECK(r && r->execType == ExecType::TradeBust && r->cumQty == 0); }
    d.venueExec(o12, c12, ExecType::Fill, 200, px); CHECK(oms.find(o12) == nullptr);

    // ---- illegal events are counted and ignored
    uint64_t ill = oms.illegal();
    d.ack(0xC0FFEE); d.risk(o12, true); { Frame<ChildOrder> f; f.init(); f.header.sourceId = 3; f.body.parentOrderId = 0xC0FFEE; f.body.childOrderId = 5; log.push(&f.header); }
    CHECK(oms.illegal() == ill + 1 && oms.live() == 0);                                              // only the orphan child counts; unknown acks and decisions are dropped
    std::printf("kill, bust, illegal paths ok\n");

    // ---- arena exhaustion rejects the order to the client rather than crashing or dropping it
    { Oms tiny(4, 4); Log tl(tiny); Driver td{tl};
      uint64_t a1 = td.newOrder(AAPL, Side::Buy, 100, px), a2 = td.newOrder(AAPL, Side::Buy, 100, px), a3 = td.newOrder(AAPL, Side::Buy, 100, px), a4 = td.newOrder(AAPL, Side::Buy, 100, px);
      CHECK(tiny.find(a1) && tiny.find(a2) && tiny.find(a3) && !tiny.find(a4));
      auto* r = tl.last<ExecReport>(); CHECK(r && r->orderId == a4 && r->execType == ExecType::Rejected && r->rejectReason == uint16_t(Reason::MaxOpenOrders)); }

    // ---- random flow
    std::mt19937_64 rng(3); std::vector<std::pair<uint64_t, uint64_t>> live;   // (parent, child)
    for (int i = 0; i < 20000; ++i) {
        uint32_t roll = uint32_t(rng() % 100);
        if ((roll < 40 && live.size() < 1500) || live.empty()) {
            // One draw per statement: argument evaluation order is unspecified, so several rng() calls
        // in one argument list make the run depend on the compiler.
        const uint32_t sym = uint32_t(1 + rng() % 10);
        const Side side = (rng() & 1) ? Side::Buy : Side::Sell;
        const int64_t qty = int64_t(100 * (1 + rng() % 10));
        uint64_t o = d.newOrder(sym, side, qty, px);
            if (rng() % 10 == 0) { d.risk(o, false, uint16_t(Reason::MaxOrderQty)); continue; }
            d.risk(o, true); uint64_t c = d.child(o, oms.find(o)->leaves, px, uint16_t(rng() % 3)); d.ack(c); live.push_back({o, c});
        } else {
            size_t k = rng() % live.size(); auto [o, c] = live[k];
            const Order* p = oms.find(o);
            if (!p) { live[k] = live.back(); live.pop_back(); continue; }
            uint32_t r2 = uint32_t(rng() % 100);
            if (r2 < 45) { int64_t q = std::min<int64_t>(p->leaves, int64_t(100 * (1 + rng() % 3))); d.venueExec(o, c, q >= p->leaves ? ExecType::Fill : ExecType::PartialFill, q, px + int64_t(rng() % 3) * 1'000'000, (rng() & 1) ? Liquidity::Added : Liquidity::Removed); }
            else if (r2 < 75) { d.cancel(o); }
            else if (r2 < 90) { if (p->status == PC || p->status == PR) d.venueExec(o, c, ExecType::Cancelled, 0, 0); else d.replace(o, p->qty + 100, px); }
            else { d.venueExec(o, c, ExecType::Cancelled, 0, 0, Liquidity::Unset, 2); }
            p = oms.find(o);
            if (p && p->status == PR) { d.risk(o, rng() % 4 != 0, uint16_t(Reason::MaxOrderQty), true); p = oms.find(o); }
            if (p && p->liveChildren == 0 && !terminal(p->status) && p->status != PC) { uint64_t nc = d.child(o, p->leaves, p->price); d.ack(nc); live[k].second = nc; }
            if (!oms.find(o)) { live[k] = live.back(); live.pop_back(); }
            else if (oms.find(o)->status == PC) { /* the venue answers next time this order is picked */ }
        }
    }
    d.kill();
    for (auto& [o, c] : live) if (oms.find(o)) d.venueExec(o, c, ExecType::Cancelled, 0, 0);
    std::printf("random flow: %llu orders, %llu fills, %llu cancelled, %llu replaced, %llu reroutes, %llu illegal, %zu live at end\n",
        (unsigned long long)oms.orders(), (unsigned long long)oms.fills(), (unsigned long long)oms.cancelled(), (unsigned long long)oms.replaced(),
        (unsigned long long)oms.reroutes(), (unsigned long long)oms.illegal(), oms.live());
    CHECK(oms.live() == 0);

    // ---- write the log (for reason coverage) and replay: outputs must be byte-identical, hash identical
    { std::ofstream out(std::string(argv[2]) + "/oms.log", std::ios::binary); for (auto& f : log.frames) out.write(reinterpret_cast<const char*>(f.data()), std::streamsize(f.size())); }
    blake3_hasher h1; blake3_hasher_init(&h1); oms.hashInto(h1); uint8_t d1[32]; blake3_hasher_finalize(&h1, d1, 32);
    Oms cold(4, 1 << 16); cold.load(snap);
    size_t next = 0, mismatches = 0;
    for (size_t i = 0; i < log.frames.size(); ++i) {
        const FrameHeader* f = log.at(i);
        if (f->sourceId == 4) continue;                       // the OMS's own frames are consumed by the risk engine, not re-applied here
        cold.apply(f, [&](FrameHeader* o) {
            if (next >= log.outputs.size()) { ++mismatches; return; }
            const auto* exp = reinterpret_cast<const FrameHeader*>(log.outputs[next].data());
            // compare header (minus seq/seqTs, which the sequencer assigns) and body
            if (o->templateId != exp->templateId || o->causeSeq != exp->causeSeq || std::memcmp(reinterpret_cast<const std::byte*>(o) + 48, log.outputs[next].data() + 48, o->frameLength - 48) != 0) ++mismatches;
            ++next;
        });
        // the log also carries the OMS's own outputs, which it does react to for Nbbo only (none here)
    }
    blake3_hasher h2; blake3_hasher_init(&h2); cold.hashInto(h2); uint8_t d2[32]; blake3_hasher_finalize(&h2, d2, 32);
    std::printf("replay: %zu of %zu client reports regenerated byte-identical, %zu mismatches, state hash %s\n", next, log.outputs.size(), mismatches, std::memcmp(d1, d2, 32) == 0 ? "identical" : "DIFFERENT");
    CHECK(mismatches == 0 && next == log.outputs.size() && std::memcmp(d1, d2, 32) == 0);
    std::printf("oms tests ok\n");
    return 0;
}
