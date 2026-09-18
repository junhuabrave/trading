// Risk engine benchmark: evaluate with the full check list, cancelling 63 of 64 accepts so the
// cancel/close path is in the number. Fails if the order path allocates. JSON for tools/bench.py.
#define TRADING_COUNT_ALLOCS
#include "alloc_guard.hpp"
#include "bench.hpp"
#include "risk.hpp"
#include <random>
#include <memory>
using namespace trading; using namespace trading::risk; using namespace trading::bench; using trading::util::AllocScope;

int main(int argc, char** argv) {
    if (argc != 2) return 2;
    Snapshot snap(argv[1]); RefData rd; rd.load(snap); auto eng = std::make_unique<RiskEngine>(rd, 1 << 22, 1 << 17);
    Frame<LimitUpdate> l; l.init(); l.body.accountIdx = 500; l.body.maxOrderQty = 10000; l.body.maxOrderNotional = 500'000'000'000'000LL;
    l.body.maxGrossExposure = INT64_MAX / 4; l.body.maxNetExposure = INT64_MAX / 4; l.body.priceCollarBps = 300; l.body.maxOpenOrders = 0; l.body.maxMsgRate = 0; eng->apply(&l.header);
    Frame<BuyingPowerUpdate> b; b.init(); b.body.accountIdx = 500; b.body.buyingPower = INT64_MAX / 4; eng->apply(&b.header);
    const size_t N = 2'000'000, B = 32; std::mt19937_64 rng(5);
    std::vector<Frame<NewOrder>> orders(N);
    for (size_t i = 0; i < N; ++i) {
        auto& f = orders[i]; f.init(); f.header.seq = i + 1; f.header.seqTs = 1'700'000'000'000'000'000LL + int64_t(i) * 100;
        f.body.accountIdx = 500; f.body.symbolIdx = uint32_t(1 + rng() % 10); f.body.sessionId = 1; f.body.clOrdId = i + 1;
        f.body.orderId = (uint64_t(2) << 48) | (i + 1); f.body.side = (rng() & 1) ? Side::Buy : Side::Sell; f.body.ordType = OrdType::Limit;
        f.body.qty = int64_t(1 + rng() % 10) * 100; f.body.price = rd.refPrice(f.body.symbolIdx) + (int64_t(rng() % 40) - 20) * 1'000'000;
    }
    RiskDecision d{}; uint64_t acc = 0;
    Frame<OrderState> cx; cx.init(); cx.body.status = OrdStatus::Cancelled;
    auto one = [&](size_t i) {
        if (eng->evaluate(&orders[i].header, orders[i].body, d) == RiskVerdict::Accept) {
            ++acc; if (i % 64 != 0) { cx.body.orderId = orders[i].body.orderId; eng->apply(&cx.header); }   // keep 1 in 64 open, cancel the rest
        }
    };
    const size_t W = N / 10;
    for (size_t i = 0; i < W; ++i) one(i);                                                       // warm-up
    Recorder r(B, N / B); AllocScope a;
    for (size_t i = W; i + B <= N; i += B) { r.begin(); for (size_t k = 0; k < B; ++k) one(i + k); r.end(); }
    uint64_t allocs = a.delta();
    report("risk.evaluate_with_cancel", r.finish(), allocs, "full check list, accept path, then cancel of 63 in 64; ~" + std::to_string(acc / 64) + " open at end");
    if (allocs) { std::fprintf(stderr, "FAIL: risk order path allocated %llu times after warm-up\n", (unsigned long long)allocs); return 1; }
    return 0;
}
