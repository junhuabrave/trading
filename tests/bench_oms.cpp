// OMS benchmark: one parent lifecycle is five sequenced frames (order, decision, child, ack, fill);
// the figure is nanoseconds per transition. Fails if the path allocates after warm-up.
#define TRADING_COUNT_ALLOCS
#include "alloc_guard.hpp"
#include "bench.hpp"
#include "oms.hpp"
#include <random>
using namespace trading; using namespace trading::oms; using namespace trading::bench; using trading::util::AllocScope;

int main(int argc, char** argv) {
    if (argc != 2) return 2;
    refdata::Snapshot snap(argv[1]); Oms oms(4, 1 << 16); oms.load(snap);
    const size_t N = 400'000, B = 8;                                   // 8 lifecycles = 40 transitions per block
    std::mt19937_64 rng(9);
    std::vector<Frame<NewOrder>> orders(N); std::vector<Frame<RiskDecision>> decisions(N); std::vector<Frame<ChildOrder>> children(N);
    std::vector<Frame<VenueAck>> acks(N); std::vector<Frame<ExecReport>> fills(N);
    for (size_t i = 0; i < N; ++i) {
        uint64_t oid = (uint64_t(2) << 48) | (i + 1), cid = (uint64_t(3) << 48) | (i + 1);
        auto& o = orders[i]; o.init(); o.header.seq = i * 5 + 1; o.header.sourceId = 2; o.body.orderId = oid; o.body.clOrdId = i + 1; o.body.accountIdx = 42; o.body.symbolIdx = uint32_t(1 + rng() % 10);
        o.body.side = Side::Buy; o.body.ordType = OrdType::Limit; o.body.tif = Tif::Day; o.body.qty = 100; o.body.price = 1'000'000'000; o.body.sessionId = 9;
        auto& d = decisions[i]; d.init(); d.header.seq = i * 5 + 2; d.header.sourceId = 3; d.body.orderId = oid; d.body.verdict = RiskVerdict::Accept;
        auto& c = children[i]; c.init(); c.header.seq = i * 5 + 3; c.header.sourceId = 3; c.body.childOrderId = cid; c.body.parentOrderId = oid; c.body.qty = 100; c.body.price = 1'000'000'000; c.body.venueId = 2; c.body.side = Side::Buy;
        auto& a = acks[i]; a.init(); a.header.seq = i * 5 + 4; a.header.sourceId = 7; a.body.childOrderId = cid; a.body.venueId = 2;
        auto& f = fills[i]; f.init(); f.header.seq = i * 5 + 5; f.header.sourceId = 7; f.body.orderId = oid; f.body.childOrderId = cid; f.body.execType = ExecType::Fill; f.body.lastQty = 100; f.body.lastPx = 1'000'000'000; f.body.venueId = 2; f.body.liquidityFlag = Liquidity::Removed;
    }
    uint64_t emitted = 0; auto sink = [&](FrameHeader*) { ++emitted; };
    auto one = [&](size_t i) { oms.apply(&orders[i].header, sink); oms.apply(&decisions[i].header, sink); oms.apply(&children[i].header, sink); oms.apply(&acks[i].header, sink); oms.apply(&fills[i].header, sink); };
    const size_t W = N / 10;
    for (size_t i = 0; i < W; ++i) one(i);
    Recorder r(B * 5, N / B); AllocScope a;
    for (size_t i = W; i + B <= N; i += B) { r.begin(); for (size_t k = 0; k < B; ++k) one(i + k); r.end(); }
    uint64_t allocs = a.delta();
    report("oms.transition", r.finish(), allocs, "per sequenced frame over order, decision, child, ack, fill; " + std::to_string(emitted) + " reports emitted");
    if (allocs) { std::fprintf(stderr, "FAIL: oms path allocated %llu times after warm-up\n", (unsigned long long)allocs); return 1; }
    return oms.live() == 0 ? 0 : 1;
}
