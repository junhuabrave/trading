// evaluate() alone, no sequencing: the number the risk latency budget cares about
#include "risk.hpp"
#include <chrono>
#include <cstdio>
#include <random>
#include <memory>
using namespace trading; using namespace trading::risk;
int main(int argc, char** argv) {
    if (argc != 2) return 2;
    Snapshot snap(argv[1]); RefData rd; rd.load(snap); auto eng = std::make_unique<RiskEngine>(rd, 1 << 22);
    Frame<LimitUpdate> l; l.init(); l.body.accountIdx = 500; l.body.maxOrderQty = 10000; l.body.maxOrderNotional = 500'000'000'000'000LL;
    l.body.maxGrossExposure = INT64_MAX / 4; l.body.maxNetExposure = INT64_MAX / 4; l.body.priceCollarBps = 300; l.body.maxOpenOrders = 0; eng->apply(&l.header);
    Frame<BuyingPowerUpdate> b; b.init(); b.body.accountIdx = 500; b.body.buyingPower = INT64_MAX / 4; eng->apply(&b.header);
    const size_t N = 2'000'000; std::mt19937_64 rng(5);
    std::vector<Frame<NewOrder>> orders(N);
    for (size_t i = 0; i < N; ++i) {
        auto& f = orders[i]; f.init(); f.header.seq = i + 1; f.header.seqTs = 1'700'000'000'000'000'000LL + int64_t(i) * 100;
        f.body.accountIdx = 500; f.body.symbolIdx = uint32_t(1 + rng() % 10); f.body.sessionId = 1; f.body.clOrdId = i + 1;
        f.body.orderId = (uint64_t(2) << 48) | (i + 1); f.body.side = (rng() & 1) ? Side::Buy : Side::Sell; f.body.ordType = OrdType::Limit;
        f.body.qty = int64_t(1 + rng() % 10) * 100; f.body.price = rd.refPrice(f.body.symbolIdx) + (int64_t(rng() % 40) - 20) * 1'000'000;
    }
    RiskDecision d{}; uint64_t acc = 0;
    Frame<OrderState> cx; cx.init(); cx.body.status = OrdStatus::Cancelled;
    auto t0 = std::chrono::steady_clock::now();
    for (size_t i = 0; i < N; ++i) {
        if (eng->evaluate(&orders[i].header, orders[i].body, d) == RiskVerdict::Accept) {
            ++acc;
            if (i % 64 != 0) { cx.body.orderId = orders[i].body.orderId; eng->apply(&cx.header); }   // keep 1 in 64 open (~30k), cancel the rest
        }
    }
    auto t1 = std::chrono::steady_clock::now();
    std::printf("risk evaluate + cancel of 63/64 (full check list, ~%zu open orders at end): %.1f ns/order, %llu accepted of %zu\n", size_t(acc / 64),
        double(std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count()) / N, (unsigned long long)acc, N);
    return 0;
}
