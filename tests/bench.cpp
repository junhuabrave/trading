// Component benchmarks for the reference-data path: decode + dispatch + hot lookups, applying a
// ListUpdate, and the checkpoint hash. Emits one JSON line per benchmark for tools/bench.py.
#define TRADING_COUNT_ALLOCS
#include "alloc_guard.hpp"
#include "bench.hpp"
#include "refdata.hpp"
#include <vector>
#include <random>

using namespace trading; using namespace trading::refdata; using namespace trading::bench; using trading::util::AllocScope;

int main(int argc, char** argv) {
    if (argc != 2) return 2;
    Snapshot snap(argv[1]); RefData rd; rd.load(snap);
    const size_t N = 1'000'000, B = 32;
    std::vector<Frame<NewOrder>> frames(N);
    std::mt19937_64 rng(1);
    for (size_t i = 0; i < N; ++i) {
        frames[i].init(); frames[i].header.seq = i + 1;
        frames[i].body.symbolIdx = uint32_t(1 + rng() % 10); frames[i].body.accountIdx = 42;
        frames[i].body.side = (rng() & 1) ? Side::Buy : Side::SellShort;
        frames[i].body.qty = int64_t(100 + rng() % 900); frames[i].body.price = int64_t(1'000'000'000 + rng() % 50'000'000'000LL);
    }
    auto check = [&](size_t i) {
        const FrameHeader* h = &frames[i].header; bool ok = false;
        if (const NewOrder* o = as<NewOrder>(h)) {
            uint32_t s = o->symbolIdx; ok = rd.tradable(s);
            if (o->side == Side::SellShort) ok = ok && rd.shortable(s) && (rd.easyToBorrow(s) || o->locateId != 0);
            int64_t ref = rd.refPrice(s); ok = ok && o->price > ref / 2 && o->price < ref * 2; ok = ok && (o->qty % rd.lotSize(s) == 0);
        }
        return ok;
    };
    uint64_t accepted = 0;
    for (size_t i = 0; i < N / 10; ++i) accepted += check(i);                              // warm-up
    { Recorder r(B, N / B); AllocScope a;
      for (size_t i = 0; i + B <= N; i += B) { r.begin(); for (size_t k = 0; k < B; ++k) accepted += check(i + k); r.end(); }
      uint64_t al = a.delta(); report("refdata.decode_dispatch_checks", r.finish(), al, "decode + dispatch + 5 reference-data checks per order"); }
    Frame<ListUpdate> lu; lu.init(); lu.body.listId = ListId::EasyToBorrow;
    { Recorder r(B, N / B); AllocScope a;
      for (size_t i = 0; i + B <= N; i += B) { r.begin(); for (size_t k = 0; k < B; ++k) { lu.body.symbolIdx = uint32_t(1 + (i + k) % 10); lu.body.op = ((i + k) & 1) ? ListOp::Add : ListOp::Remove; rd.apply(&lu.header); } r.end(); }
      uint64_t al = a.delta(); report("refdata.apply_list_update", r.finish(), al); }
    { std::vector<double> v; AllocScope a; for (int i = 0; i < 50; ++i) { int64_t t0 = nowNs(); auto sh = rd.stateHash(); int64_t t1 = nowNs(); v.push_back(double(t1 - t0)); (void)sh; }
      uint64_t al = a.delta(); report("refdata.state_hash", percentiles(v), al, "checkpoint cost, whole hot arrays"); }
    std::fprintf(stderr, "accepted %llu\n", (unsigned long long)accepted);
    return 0;
}
