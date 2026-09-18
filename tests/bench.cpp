// Rough single-core numbers for the two things the hot path does with this code:
// decode a frame and dispatch on template id; reference-data lookups; applying a ListUpdate.
#include "refdata.hpp"
#include <chrono>
#include <cstdio>
#include <vector>
#include <random>

using namespace trading;
using namespace trading::refdata;
using clk = std::chrono::steady_clock;

int main(int argc, char** argv) {
    if (argc != 2) return 2;
    Snapshot snap(argv[1]); RefData rd; rd.load(snap);

    // 1M NewOrder frames in a contiguous buffer
    const size_t N = 1'000'000;
    std::vector<Frame<NewOrder>> frames(N);
    std::mt19937_64 rng(1);
    for (size_t i = 0; i < N; ++i) {
        frames[i].init(); frames[i].header.seq = i + 1;
        frames[i].body.symbolIdx = uint32_t(1 + rng() % 10); frames[i].body.accountIdx = 42;
        frames[i].body.side = (rng() & 1) ? Side::Buy : Side::SellShort;
        frames[i].body.qty = int64_t(100 + rng() % 900); frames[i].body.price = int64_t(1'000'000'000 + rng() % 50'000'000'000LL);
    }
    // decode + a risk-style check: tradable, shortable if short, ETB or locate, price collar vs refPrice
    uint64_t accepted = 0;
    auto t0 = clk::now();
    for (size_t i = 0; i < N; ++i) {
        const FrameHeader* h = &frames[i].header;
        if (const NewOrder* o = as<NewOrder>(h)) {
            uint32_t s = o->symbolIdx;
            bool ok = rd.tradable(s);
            if (o->side == Side::SellShort) ok = ok && rd.shortable(s) && (rd.easyToBorrow(s) || o->locateId != 0);
            int64_t ref = rd.refPrice(s);
            ok = ok && o->price > ref / 2 && o->price < ref * 2;
            ok = ok && (o->qty % rd.lotSize(s) == 0);
            accepted += ok;
        }
    }
    auto t1 = clk::now();
    double ns = double(std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count()) / N;
    std::printf("decode+dispatch+5 refdata checks: %.1f ns/order (%llu accepted)\n", ns, (unsigned long long)accepted);

    // apply 1M ListUpdate messages
    Frame<ListUpdate> lu; lu.init(); lu.body.listId = ListId::EasyToBorrow;
    t0 = clk::now();
    for (size_t i = 0; i < N; ++i) {
        lu.body.symbolIdx = uint32_t(1 + i % 10); lu.body.op = (i & 1) ? ListOp::Add : ListOp::Remove;
        rd.apply(&lu.header);
    }
    t1 = clk::now();
    ns = double(std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count()) / N;
    std::printf("apply ListUpdate:                 %.1f ns/message\n", ns);

    t0 = clk::now(); auto sh = rd.stateHash(); t1 = clk::now();
    std::printf("stateHash over hot arrays:        %.1f us (checkpoint cost) %02x%02x..\n",
        double(std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count()) / 1000.0, sh[0], sh[1]);
    return 0;
}
