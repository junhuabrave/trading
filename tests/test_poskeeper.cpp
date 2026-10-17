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
//
// Corporate actions (PK-2). Each runs the keeper and the risk engine side by side on the same frames,
// with the keeper's output sequenced back in, and requires after every frame that the two hold the
// same positions and account totals, and that the risk engine never had to be corrected.
//   5. split, reverse split, cash dividend, symbol change (same index and a new one), delist and an
//      action it does not apply, against figures worked out by hand, and what the checkpoint publishes
//   6. effectiveSeq: an action sequenced early acts at the frame it names, before that frame's fill;
//      an announcement does nothing until it is applied; a re-sent action acts once
//   7. a risk engine restarted from the keeper's snapshots while an action is pending still applies it
//      at its sequence, and does not apply again one the snapshots already reflect
//   8. the day-1 to day-2 fixture pair: day-1 positions, then the security master's diff (the NVDA
//      4:1 split and the XYZQ delisting among it), compared with the day-2 reference prices
#include "poskeeper.hpp"
#include "risk.hpp"
#include <array>
#include <atomic>
#include <fstream>
#include <map>
#include <set>
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

static Frame<CorporateAction> action(uint64_t id, CorpActionType type, uint32_t sym, int64_t num = 0, int64_t den = 0, int64_t cash = 0,
                                     uint32_t newSym = 0, uint64_t effectiveSeq = 0, bool applied = true) {
    Frame<CorporateAction> f; f.init(); f.header.sourceId = 15;
    CorporateActionRecord& r = f.body.record;
    r.actionId = id; r.actionType = uint8_t(type); r.symbolIdx = sym; r.newSymbolIdx = newSym; r.ratioNum = num; r.ratioDen = den;
    r.cashAmount = cash; r.currency = 840; r.effectiveSeq = effectiveSeq; r.applied = applied ? 1 : 0;
    return f;
}

// The keeper and a risk engine reading one sequenced stream. Every frame fed gets the next sequence
// number; what the keeper publishes at a checkpoint is sequenced straight after it, to both.
struct Lockstep {
    std::vector<std::byte> mem = std::vector<std::byte>(PositionView::bytesFor(256, 16));
    PositionKeeper pk{PositionKeeper::Params{6, 256, 16}, mem.data()};
    refdata::RefData rd;
    risk::RiskEngine risk{rd, 1 << 12, 1 << 10};
    uint64_t seq = 0;
    std::vector<std::vector<std::byte>> published, recorded;   // the keeper's frames; every frame sequenced
    std::set<uint64_t> keys; std::set<uint32_t> accounts;

    bool feed(FrameHeader* f) {
        f->seq = ++seq; f->streamId = 0;
        if (const auto* e = as<ExecReport>(f)) { keys.insert(positionKey(e->accountIdx, e->symbolIdx)); accounts.insert(e->accountIdx); }
        if (const auto* a = as<CorporateAction>(f); a && a->record.newSymbolIdx) for (uint32_t ac : accounts) keys.insert(positionKey(ac, a->record.newSymbolIdx));
        std::vector<std::vector<std::byte>> out;
        pk.apply(f, [&](FrameHeader* o) { const auto* b = reinterpret_cast<const std::byte*>(o); out.emplace_back(b, b + o->frameLength); });
        risk.apply(f);
        record(f);
        for (auto& o : out) {
            auto* h = reinterpret_cast<FrameHeader*>(o.data());
            h->seq = ++seq; h->causeSeq = f->seq;
            pk.apply(h, [](const FrameHeader*) {}); risk.apply(h);
            published.push_back(o); record(h);
        }
        return agree();
    }
    void record(const FrameHeader* f) { const auto* b = reinterpret_cast<const std::byte*>(f); recorded.emplace_back(b, b + f->frameLength); }
    bool checkpoint() { Frame<Checkpoint> c; c.init(); c.header.sourceId = 1; return feed(&c.header); }
    Position at(uint32_t acct, uint32_t sym) const { const Position* p = pk.find(acct, sym); return p ? *p : Position{}; }
    bool agree() const {
        for (uint64_t k : keys) {
            const Position a = at(uint32_t(k >> 32), uint32_t(k)), b = risk.positionOf(uint32_t(k >> 32), uint32_t(k));
            if (a.qty != b.qty || a.avgCost != b.avgCost || a.realized != b.realized) return false;
        }
        for (uint32_t ac : accounts) {
            money::i128 g = 0, n = 0;
            pk.exposure(ac, g, n);
            if (g != risk.grossPosition(ac) || n != risk.netPosition(ac)) return false;
        }
        return risk.positionCorrections() == 0;
    }
};

static bool same(const Position& p, int64_t qty, int64_t avg, int64_t realized) { return p.qty == qty && p.avgCost == avg && p.realized == realized; }
static const FrameHeader* hdr(const std::vector<std::byte>& b) { return reinterpret_cast<const FrameHeader*>(b.data()); }

int main(int argc, char** argv) {
    if (argc != 4) { std::fprintf(stderr, "usage: %s workdir day1-snapshot day1-to-day2-diff\n", argv[0]); return 2; }
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

    // ---- 5: each kind of action, by hand, with the keeper and the risk engine side by side
    {
        Lockstep L;
        auto fillOf = [&](uint32_t acct, uint32_t sym, Side side, int64_t qty, int64_t px) { auto r = report(acct, sym, side, ExecType::Fill, qty, px); return L.feed(&r.header); };
        auto act = [&](Frame<CorporateAction> a) { return L.feed(&a.header); };
        CHECK(fillOf(42, 10, Side::Buy, 1000, 40137000000));            // long 1000 at $401.37
        CHECK(fillOf(7, 10, Side::SellShort, 250, 40137000003));        // short 250 at a cost a split cannot divide
        CHECK(fillOf(9, 10, Side::Buy, 100, 10 * USD) && fillOf(9, 10, Side::Sell, 100, 11 * USD));   // flat, $100 made
        CHECK(fillOf(42, 20, Side::Buy, 1005, 2 * USD));
        CHECK(fillOf(7, 20, Side::SellShort, 5, 3 * USD));
        CHECK(fillOf(42, 30, Side::Buy, 1000, 50 * USD));
        CHECK(fillOf(7, 30, Side::SellShort, 400, 50 * USD));
        CHECK(fillOf(42, 40, Side::Buy, 10, USD));
        CHECK(fillOf(42, 50, Side::Buy, 200, 10 * USD) && fillOf(42, 51, Side::Buy, 100, 13 * USD) && fillOf(7, 50, Side::Buy, 50, 10 * USD));
        CHECK(fillOf(42, 60, Side::Buy, 10, USD) && fillOf(42, 61, Side::Buy, 10, USD));
        CHECK(L.checkpoint());
        L.published.clear();

        CHECK(act(action(6001, CorpActionType::Split, 10, 4, 1)));
        CHECK(act(action(6002, CorpActionType::ReverseSplit, 20, 1, 10)));
        CHECK(act(action(6003, CorpActionType::CashDividend, 30, 0, 0, 24'000'000)));            // $0.24 a share
        CHECK(act(action(6004, CorpActionType::SymbolChange, 40, 0, 0, 0, 40)));                  // a new ticker on the same index
        CHECK(act(action(6005, CorpActionType::SymbolChange, 40)));                               // the same, newSymbolIdx unset
        CHECK(act(action(6006, CorpActionType::SymbolChange, 50, 0, 0, 0, 51)));                  // onto an index already held
        CHECK(act(action(6007, CorpActionType::Delist, 60)));
        CHECK(act(action(6008, CorpActionType::Merger, 61, 1, 1, 0, 62)));                        // not applied here
        // 4:1. 1000 x 4 at $401.37 / 4 = $100.3425; the short's $401.37000003 / 4 rounds toward zero
        CHECK(same(L.at(42, 10), 4000, 10034250000, 0) && same(L.at(7, 10), -1000, 10034250000, 0));
        CHECK(same(L.at(9, 10), 0, 0, 100 * USD));                                                // flat: nothing to adjust
        // 1:10. 1005 shares become 100 and half a share leaves at cost; a short of 5 rounds to nothing
        CHECK(same(L.at(42, 20), 100, 20 * USD, 0) && same(L.at(7, 20), 0, 0, 0));
        // $0.24 a share: the long receives $240, the short owes $96; neither quantity nor cost moves
        CHECK(same(L.at(42, 30), 1000, 50 * USD, 240 * USD) && same(L.at(7, 30), -400, 50 * USD, -96 * USD));
        CHECK(same(L.at(42, 40), 10, USD, 0));
        // 200 at $10 joins 100 at $13: 300 at $11; the other account's 50 move alone
        CHECK(same(L.at(42, 50), 0, 0, 0) && same(L.at(42, 51), 300, 11 * USD, 0));
        CHECK(same(L.at(7, 50), 0, 0, 0) && same(L.at(7, 51), 50, 10 * USD, 0));
        CHECK(same(L.at(42, 60), 10, USD, 0) && same(L.at(42, 61), 10, USD, 0));
        CHECK(L.pk.actions().taken() == 7 && L.pk.actions().unapplied() == 1);
        CHECK(L.risk.actions().taken() == 7 && L.risk.actions().unapplied() == 1);
        CHECK(L.published.empty());                                     // the keeper publishes at the checkpoint

        CHECK(L.checkpoint());
        struct Row { uint64_t id; uint32_t acct, sym; int64_t before, after, cash; };
        const Row rows[] = {{6001, 42, 10, 1000, 4000, 0}, {6001, 7, 10, -250, -1000, 0}, {6002, 42, 20, 1005, 100, 0}, {6002, 7, 20, -5, 0, 0},
                            {6003, 42, 30, 1000, 1000, 240 * USD}, {6003, 7, 30, -400, -400, -96 * USD}, {6006, 42, 50, 200, 200, 0}, {6006, 7, 50, 50, 50, 0}};
        const std::pair<uint32_t, uint32_t> snaps[] = {{42, 10}, {7, 10}, {42, 20}, {7, 20}, {42, 30}, {7, 30}, {42, 50}, {42, 51}, {7, 50}, {7, 51}};
        CHECK(L.published.size() == 8 + 10 + 2);
        for (size_t i = 0; i < 8; ++i) {
            const auto* c = as<CorporateActionApplied>(hdr(L.published[i]));
            CHECK(c && c->actionId == rows[i].id && c->accountIdx == rows[i].acct && c->symbolIdx == rows[i].sym
                  && c->qtyBefore == rows[i].before && c->qtyAfter == rows[i].after && c->cashAmount == rows[i].cash);
        }
        for (size_t i = 0; i < 10; ++i) {                               // the renamed, delisted and unapplied ones did not change
            const auto* p = as<PositionSnapshot>(hdr(L.published[8 + i]));
            CHECK(p && p->accountIdx == snaps[i].first && p->symbolIdx == snaps[i].second);
        }
        CHECK(as<ExposureSnapshot>(hdr(L.published[18])) && as<ExposureSnapshot>(hdr(L.published[19])));
        std::printf("actions: split, reverse split, dividend, symbol change and delist as worked by hand; %zu frames at the checkpoint\n", L.published.size());
    }

    // ---- 6: effectiveSeq, announcements, and an action sent twice
    {
        Lockstep L;
        auto fillOf = [&](int64_t qty, int64_t px) { auto r = report(42, 70, Side::Buy, ExecType::Fill, qty, px); return L.feed(&r.header); };
        CHECK(fillOf(100, 10 * USD));                                                   // seq 1
        { auto a = action(7001, CorpActionType::Split, 70, 2, 1, 0, 0, 8); CHECK(L.feed(&a.header)); }   // seq 2, to act at 8
        CHECK(fillOf(100, 12 * USD));                                                   // seq 3: still before the split
        CHECK(same(L.at(42, 70), 200, 11 * USD, 0) && L.pk.actions().pending() == 1 && L.risk.actions().pending() == 1);
        {   // market data reaches the risk engine with its own feed's sequence numbers, which are not ours
            Frame<Trade> t; t.init(); t.header.streamId = 1; t.header.seq = 500; t.body.symbolIdx = 70; t.body.price = 11 * USD; t.body.qty = 100;
            L.risk.apply(&t.header);
            CHECK(L.risk.actions().pending() == 1 && L.agree());
        }
        CHECK(L.checkpoint() && L.seq == 6);                                            // 4, and the keeper's two frames
        { auto a = action(7002, CorpActionType::Split, 70, 3, 1, 0, 0, 0, false); CHECK(L.feed(&a.header)); }   // 7: announced only
        CHECK(same(L.at(42, 70), 200, 11 * USD, 0));
        // seq 8: the split comes first (400 at $5.50), then the fill: (2200 + 54) / 410 = $5.49756097...
        CHECK(fillOf(10, 540'000'000) && L.seq == 8);
        CHECK(same(L.at(42, 70), 410, 549'756'097, 0) && L.pk.actions().pending() == 0);
        { auto a = action(7001, CorpActionType::Split, 70, 2, 1); CHECK(L.feed(&a.header)); }               // re-sent: acts once
        CHECK(same(L.at(42, 70), 410, 549'756'097, 0));
        { auto a = action(7002, CorpActionType::Split, 70, 3, 1); CHECK(L.feed(&a.header)); }               // now applied: 3:1
        CHECK(same(L.at(42, 70), 1230, 183'252'032, 0));
        CHECK(L.pk.actions().taken() == 2 && L.risk.actions().taken() == 2);
        std::printf("effectiveSeq: an early action waited for seq 8 and acted before its fill; an announcement waited to be applied\n");
    }

    // ---- 7: a restart from the keeper's snapshots with an action still to come
    {
        Lockstep L;
        auto fillOf = [&](uint32_t acct, uint32_t sym, Side side, int64_t qty, int64_t px) { auto r = report(acct, sym, side, ExecType::Fill, qty, px); return L.feed(&r.header); };
        CHECK(fillOf(42, 80, Side::Buy, 100, 20 * USD));
        { auto a = action(8001, CorpActionType::Split, 80, 2, 1); CHECK(L.feed(&a.header)); }                    // in effect now
        CHECK(fillOf(7, 81, Side::Buy, 300, 9 * USD));
        { auto a = action(8002, CorpActionType::CashDividend, 81, 0, 0, 50'000'000, 0, 20); CHECK(L.feed(&a.header)); }   // at 20
        CHECK(L.checkpoint());
        const uint64_t C = L.seq - L.published.size();                 // the checkpoint's own sequence number
        CHECK(fillOf(42, 80, Side::Sell, 50, 11 * USD));
        { auto a = action(8001, CorpActionType::Split, 80, 2, 1); CHECK(L.feed(&a.header)); }                    // re-sent after the checkpoint
        while (L.seq < 19) { auto r = report(42, 82, Side::Buy, ExecType::Fill, 1, USD); CHECK(L.feed(&r.header)); }
        CHECK(fillOf(7, 81, Side::Buy, 100, 10 * USD) && L.seq == 20);  // the dividend on 300 ($150), then 400 at $9.25
        CHECK(L.checkpoint());
        CHECK(same(L.at(7, 81), 400, 925'000'000, 150 * USD) && same(L.at(42, 80), 150, 10 * USD, 50 * USD));

        refdata::RefData rd2; risk::RiskEngine r2(rd2, 1 << 12, 1 << 10);
        for (const auto& b : L.recorded) {
            const FrameHeader* f = hdr(b);
            r2.restoreAction(f, C);
            if (f->sourceId == 6 && f->causeSeq == C) r2.apply(f);      // the keeper's figures as of the checkpoint
        }
        for (const auto& b : L.recorded) if (hdr(b)->seq > C && hdr(b)->sourceId != 6) r2.apply(hdr(b));
        for (uint64_t k : L.keys) {
            const Position a = L.at(uint32_t(k >> 32), uint32_t(k)), b = r2.positionOf(uint32_t(k >> 32), uint32_t(k));
            CHECK(a.qty == b.qty && a.avgCost == b.avgCost && a.realized == b.realized);
        }
        CHECK(r2.positionRebases() > 0 && r2.actions().taken() == 1);    // only the dividend acted after the restart
        std::printf("restart: from the snapshots at seq %llu, the pending dividend acted at seq 20 and the re-sent split did not act again\n", (unsigned long long)C);
    }

    // ---- 8: the day-1 to day-2 fixture pair
    {
        refdata::Snapshot day1(argv[2]);
        Lockstep L;
        L.rd.load(day1); L.risk.load(day1);
        const uint32_t nvda = day1.lookupTicker("NVDA"), xyzq = day1.lookupTicker("XYZQ"), aapl = day1.lookupTicker("AAPL");
        const int64_t ref1 = L.rd.refPrice(nvda);
        auto fillOf = [&](uint32_t acct, uint32_t sym, Side side, int64_t qty, int64_t px) { auto r = report(acct, sym, side, ExecType::Fill, qty, px); return L.feed(&r.header); };
        CHECK(fillOf(42, nvda, Side::Buy, 1000, ref1 + 3));
        CHECK(fillOf(1, nvda, Side::SellShort, 400, ref1));             // opened at day 1's reference price
        CHECK(fillOf(42, xyzq, Side::Buy, 500, 3 * USD));
        CHECK(fillOf(1, aapl, Side::Buy, 100, 200 * USD));
        CHECK(L.checkpoint());
        L.published.clear();
        std::ifstream in(argv[3], std::ios::binary);
        std::vector<char> log((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
        size_t frames = 0;
        for (size_t off = 0; off + sizeof(FrameHeader) <= log.size(); ++frames) {
            const auto* h = reinterpret_cast<const FrameHeader*>(log.data() + off);
            std::vector<std::byte> f(reinterpret_cast<const std::byte*>(h), reinterpret_cast<const std::byte*>(h) + h->frameLength);
            CHECK(L.feed(reinterpret_cast<FrameHeader*>(f.data())));
            off += h->frameLength;
        }
        CHECK(L.checkpoint());
        CHECK(same(L.at(42, nvda), 4000, (ref1 + 3) / 4, 0) && same(L.at(1, nvda), -1600, ref1 / 4, 0));
        CHECK(L.rd.refPrice(nvda) == L.at(1, nvda).avgCost);           // the master's adjusted reference and ours, independently
        CHECK(same(L.at(42, xyzq), 500, 3 * USD, 0) && same(L.at(1, aapl), 100, 200 * USD, 0));   // delisted: still held
        CHECK(L.pk.actions().taken() == 2 && L.pk.actions().unapplied() == 0);                    // the split and the delisting
        CHECK(L.published.size() == 6);                                 // two applications, two NVDA snapshots, two accounts
        const auto* c0 = as<CorporateActionApplied>(hdr(L.published[0])); const auto* c1 = as<CorporateActionApplied>(hdr(L.published[1]));
        CHECK(c0 && c1 && c0->actionId == 5001 && c0->accountIdx == 42 && c0->qtyAfter == 4000 && c1->accountIdx == 1 && c1->qtyAfter == -1600);
        const auto* p0 = as<PositionSnapshot>(hdr(L.published[2])); const auto* p1 = as<PositionSnapshot>(hdr(L.published[3]));
        CHECK(p0 && p1 && p0->symbolIdx == nvda && p1->symbolIdx == nvda);
        std::printf("fixture pair: %zu diff frames; NVDA 4:1 on both accounts matches the day-2 reference price; XYZQ delisted and still held\n", frames);
    }

    std::printf("position keeper tests ok\n");
    return 0;
}
