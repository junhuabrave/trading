// tests/test_poskeeper.cpp <workdir>
//
// The position keeper's hot view (PK-1).
//
//   1. the fold on worked examples, with the expected figures worked out by hand: adding at a
//      weighted average, reducing and realising against it, going through zero, rounding
//   2. what a checkpoint publishes: only what changed, in the order it first changed, as of the
//      checkpoint, with the account totals beside it; nothing at all when nothing changed
//   3. the shared view: a second mapping of the same file reads what the keeper wrote, and a reader
//      thread hammering it while the keeper writes never sees a position half written
//   4. random fills folded twice give the same frames byte for byte and the same hash, and every
//      quantity equals the plain sum of signed fills
#include "poskeeper.hpp"
#include <array>
#include <atomic>
#include <map>
#include <cstdio>
#include <filesystem>
#include <random>
#include <thread>
#include <vector>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

using namespace trading;
using namespace trading::pos;
namespace fs = std::filesystem;
#define CHECK(c) do { if (!(c)) { std::fprintf(stderr, "CHECK failed %s:%d: %s\n", __FILE__, __LINE__, #c); return 1; } } while (0)

static constexpr int64_t USD = 100'000'000;

static Frame<ExecReport> report(uint32_t acct, uint32_t sym, Side side, ExecType t, int64_t qty, int64_t px) {
    Frame<ExecReport> f; f.init(); f.header.sourceId = 4;
    f.body.accountIdx = acct; f.body.symbolIdx = sym; f.body.side = side; f.body.execType = t; f.body.lastQty = qty; f.body.lastPx = px;
    return f;
}

int main(int argc, char** argv) {
    if (argc != 2) { std::fprintf(stderr, "usage: %s workdir\n", argv[0]); return 2; }
    const std::string work = argv[1];
    fs::create_directories(work);

    // ---- 1: the fold, against arithmetic done by hand
    {
        Position p;
        fill(p, +100, 10 * USD);                            // buy 100 at $10
        CHECK(p.qty == 100 && p.avgCost == 10 * USD && p.realized == 0);
        fill(p, +300, 12 * USD);                            // buy 300 at $12: (1000 + 3600) / 400 = $11.50
        CHECK(p.qty == 400 && p.avgCost == 1150 * USD / 100 && p.realized == 0);
        fill(p, -100, 13 * USD);                            // sell 100 at $13: realise 100 x $1.50 = $150, cost unchanged
        CHECK(p.qty == 300 && p.avgCost == 1150 * USD / 100 && p.realized == 150 * USD);
        CHECK(exposure(p) == money::i128(300) * 1150 * USD / 100);
        fill(p, -500, 11 * USD);                            // sell 500 at $11: close 300 at -$0.50 (-$150), open 200 short at $11
        CHECK(p.qty == -200 && p.avgCost == 11 * USD && p.realized == 0);
        fill(p, +200, 10 * USD);                            // buy 200 at $10 to cover: a short that buys back lower made $200
        CHECK(p.qty == 0 && p.avgCost == 0 && p.realized == 200 * USD);
        CHECK(exposure(p) == 0);                            // flat carries no exposure, whatever it made on the way
        Position r;
        fill(r, +1, 1 * USD); fill(r, +2, 2 * USD);         // (1 + 4) / 3 = $1.6666..., kept toward zero in 1e-8
        CHECK(r.avgCost == 166'666'666);
        Position s;
        fill(s, -300, 50 * USD); fill(s, -100, 54 * USD);   // shorts average the same way: (15000 + 5400) / 400 = $51
        CHECK(s.qty == -400 && s.avgCost == 51 * USD);
        std::printf("fold: average cost, realised on reduction, through zero, rounding - all as worked by hand\n");
    }

    // ---- 2: what a checkpoint publishes
    {
        std::vector<std::byte> mem(PositionView::bytesFor(64, 16));
        PositionKeeper pk(PositionKeeper::Params{6, 64, 16}, mem.data());
        std::vector<std::vector<std::byte>> out;
        auto keep = [&](FrameHeader* f) { const auto* b = reinterpret_cast<const std::byte*>(f); out.emplace_back(b, b + f->frameLength); };
        auto feed = [&](Frame<ExecReport> r, uint64_t seq) { r.header.seq = seq; pk.apply(&r.header, keep); };
        auto checkpoint = [&](uint64_t seq) { Frame<Checkpoint> c; c.init(); c.header.seq = seq; pk.apply(&c.header, keep); };
        feed(report(42, 7, Side::Buy, ExecType::PartialFill, 100, 20 * USD), 10);
        feed(report(42, 3, Side::SellShort, ExecType::Fill, 200, 30 * USD), 11);
        feed(report(42, 7, Side::Buy, ExecType::Fill, 100, 22 * USD), 12);
        feed(report(42, 7, Side::Buy, ExecType::Fill, 0, 0), 13);                // closes the order, carries no shares
        Frame<ExecReport> venueLevel = report(42, 9, Side::Buy, ExecType::Fill, 500, USD); venueLevel.body.childOrderId = 77;
        feed(venueLevel, 14);                                                     // the OMS's input, not a client fill
        checkpoint(20);
        CHECK(out.size() == 3);
        const auto* a = as<PositionSnapshot>(reinterpret_cast<const FrameHeader*>(out[0].data()));
        const auto* b = as<PositionSnapshot>(reinterpret_cast<const FrameHeader*>(out[1].data()));
        const auto* x = as<ExposureSnapshot>(reinterpret_cast<const FrameHeader*>(out[2].data()));
        CHECK(a && b && x);
        CHECK(a->symbolIdx == 7 && a->qty == 200 && a->avgCost == 21 * USD && a->asOfSeq == 20);   // first to change, published first
        CHECK(b->symbolIdx == 3 && b->qty == -200 && b->avgCost == 30 * USD && b->asOfSeq == 20);
        // gross |4200| + |-6000| = $10,200; net 4200 - 6000 = -$1,800
        CHECK(x->accountIdx == 42 && x->moneyScale == MoneyScale::Unit1e8 && x->grossExposure == 10'200 * USD && x->netExposure == -1'800 * USD);
        CHECK(pk.find(42, 9) == nullptr);
        out.clear();
        checkpoint(30);
        CHECK(out.empty());                                                       // nothing changed, nothing published
        feed(report(42, 3, Side::SellShort, ExecType::TradeBust, 200, 30 * USD), 31);   // the short sale is busted: same side, undone
        checkpoint(40);
        CHECK(out.size() == 2);
        const auto* c = as<PositionSnapshot>(reinterpret_cast<const FrameHeader*>(out[0].data()));
        CHECK(c && c->symbolIdx == 3 && c->qty == 0 && c->asOfSeq == 40);         // only the position that moved
        std::printf("checkpoints: changed positions only, first-changed first, as of the checkpoint; account totals beside them\n");
    }

    // ---- 3: the shared view, read through a second mapping while the keeper writes
    {
        const uint32_t PS = 1024, AS = 16;
        const size_t bytes = PositionView::bytesFor(PS, AS);
        const std::string path = work + "/positions.view";
        int fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
        CHECK(fd >= 0 && ::ftruncate(fd, off_t(bytes)) == 0);
        void* wm = ::mmap(nullptr, bytes, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        CHECK(wm != MAP_FAILED);
        PositionKeeper pk(PositionKeeper::Params{6, PS, AS}, static_cast<std::byte*>(wm));
        void* rm = ::mmap(nullptr, bytes, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);   // another process would map it the same way
        CHECK(rm != MAP_FAILED);
        PositionView reader(static_cast<std::byte*>(rm), PS, AS, false);
        auto feed = [&](uint32_t sym, Side side, int64_t qty, int64_t px) { auto r = report(42, sym, side, ExecType::Fill, qty, px); pk.apply(&r.header, [](const FrameHeader*) {}); };
        feed(5, Side::Buy, 100, 40 * USD);
        Position got{};
        CHECK(reader.position(42, 5, got) && got.qty == 100 && got.avgCost == 40 * USD);
        CHECK(!reader.position(42, 6, got));
        money::i128 gross = 0, net = 0;
        CHECK(reader.account(42, gross, net) && gross == 4000 * USD && net == 4000 * USD);

        // Every write leaves the position with qty and avgCost in a fixed relation: the i-th share is
        // bought at 2i - 1, which keeps the average exactly equal to the quantity. A read that mixed
        // two writes would break it.
        std::atomic<bool> stop{false};
        std::atomic<uint64_t> reads{0}, torn{0};
        std::thread t([&] {
            Position p{};
            while (!stop.load(std::memory_order_relaxed)) {
                if (!reader.position(42, 11, p) || p.qty == 0) continue;   // not there yet, or claimed and not yet written
                reads.fetch_add(1, std::memory_order_relaxed);
                if (p.avgCost != p.qty) torn.fetch_add(1, std::memory_order_relaxed);
            }
        });
        const int N = 200'000;
        for (int i = 1; i <= N; ++i) feed(11, Side::Buy, 1, 2 * int64_t(i) - 1);
        stop = true; t.join();
        CHECK(reader.position(42, 11, got) && got.qty == N && got.avgCost == N);
        std::printf("shared view: a second mapping reads what was written; %llu reads during %d writes, %llu torn\n",
                    (unsigned long long)reads.load(), N, (unsigned long long)torn.load());
        CHECK(torn.load() == 0);
        ::munmap(rm, bytes); ::munmap(wm, bytes); ::close(fd);
    }

    // ---- 4: replay determinism, and quantities against the plain sum
    {
        std::vector<Frame<ExecReport>> day;
        std::mt19937_64 rng(42);
        for (int i = 0; i < 50'000; ++i) {
            const uint32_t acct = uint32_t(1 + rng() % 5);
            const uint32_t sym = uint32_t(1 + rng() % 40);
            const uint64_t sideRoll = rng() % 10;
            const Side side = sideRoll < 5 ? Side::Buy : sideRoll < 8 ? Side::Sell : Side::SellShort;
            const int64_t qty = int64_t(100 * (1 + rng() % 9));
            const int64_t px = int64_t(20 + rng() % 200) * USD / 4;
            const ExecType t = rng() % 50 == 0 ? ExecType::TradeBust : ExecType::PartialFill;
            day.push_back(report(acct, sym, side, t, qty, px));
        }
        auto run = [&](std::vector<std::vector<std::byte>>& frames, std::array<uint8_t, 32>& hash, std::map<uint64_t, int64_t>* sums) {
            std::vector<std::byte> mem(PositionView::bytesFor(1024, 16));
            PositionKeeper pk(PositionKeeper::Params{6, 1024, 16}, mem.data());
            auto keep = [&](FrameHeader* f) { const auto* b = reinterpret_cast<const std::byte*>(f); frames.emplace_back(b, b + f->frameLength); };
            uint64_t seq = 0;
            for (auto r : day) {
                r.header.seq = ++seq; pk.apply(&r.header, keep);
                if (sums) (*sums)[positionKey(r.body.accountIdx, r.body.symbolIdx)] += (r.body.execType == ExecType::TradeBust ? -1 : 1) * signedQty(r.body.side, r.body.lastQty);
                if (seq % 1000 == 0) { Frame<Checkpoint> c; c.init(); c.header.seq = ++seq; pk.apply(&c.header, keep); }
            }
            blake3_hasher h; blake3_hasher_init(&h); pk.hashInto(h); blake3_hasher_finalize(&h, hash.data(), 32);
            if (sums) for (const auto& [k, q] : *sums) { const Position* p = pk.find(uint32_t(k >> 32), uint32_t(k)); if (!p || p->qty != q) return false; }
            return true;
        };
        std::vector<std::vector<std::byte>> f1, f2; std::array<uint8_t, 32> h1{}, h2{}; std::map<uint64_t, int64_t> sums;
        CHECK(run(f1, h1, &sums));
        CHECK(run(f2, h2, nullptr));
        CHECK(f1.size() > 1000 && f1 == f2 && h1 == h2);
        std::printf("replay: %zu fills twice, %zu snapshot frames byte-identical, same hash; %zu quantities equal the plain sum of fills\n",
                    day.size(), f1.size(), sums.size());
    }

    std::printf("position keeper tests ok\n");
    return 0;
}
