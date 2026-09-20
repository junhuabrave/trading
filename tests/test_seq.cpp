// tests/test_seq.cpp  <day1 snapshot> <workdir>
//
// Proves the properties the rest of the stack relies on:
//  1. determinism: a cold consumer replaying the core journal (with market data pulled
//     to each MdWatermark) recomputes every routing decision the live engine made,
//     even though the live engine saw market data and orders in an arbitrary interleaving
//  2. checkpoint hashes agree between the live engine, a late-starting reader that
//     refilled a gap from the journal, and the cold replay
//  3. atomic batches: MdWatermark and its decision are adjacent with correct flags
//  4. crash recovery: a torn tail is truncated on open; the sequencer resumes seq
//  5. back-pressure: the ring refuses when the slowest reader is a full ring behind
#include "sequencer.hpp"
#include "reader.hpp"
#include "watermark.hpp"
#include "refdata.hpp"
#include "blake3.h"
#include <map>
#include <random>
#include <string>
#include <cstdio>
#include <filesystem>
#include <deque>

using namespace trading;
using namespace trading::seq;
using namespace trading::refdata;
namespace fs = std::filesystem;

#define CHECK(c) do { if (!(c)) { std::fprintf(stderr, "CHECK failed %s:%d: %s\n", __FILE__, __LINE__, #c); return 1; } } while (0)

// ---------------------------------------------------------------- toy state machines
struct BookLite {                       // best-level book per symbol, one venue
    std::map<int64_t, int64_t> bids[16], asks[16];
    void apply(const BookDelta& d) {
        auto& side = d.side == BookSide::Bid ? bids[d.symbolIdx] : asks[d.symbolIdx];
        if (d.action == BookAction::Set) side[d.price] = d.qty;
        else if (d.action == BookAction::Delete) side.erase(d.price);
        else if (d.action == BookAction::ClearSide) side.clear();
        else if (d.action == BookAction::ClearBook) { bids[d.symbolIdx].clear(); asks[d.symbolIdx].clear(); }
    }
    int64_t bestBid(uint32_t s) const { return bids[s].empty() ? 0 : bids[s].rbegin()->first; }
    int64_t bestAsk(uint32_t s) const { return asks[s].empty() ? 0 : asks[s].begin()->first; }
};

struct OmsLite {                         // what a consumer derives from the core stream alone
    std::map<uint64_t, uint8_t> status;  // orderId -> RiskVerdict
    uint64_t orders = 0, accepted = 0, rejected = 0, decisions = 0, refUpdates = 0;
    std::array<uint8_t, 32> hash(const RefData& rd) const {
        blake3_hasher h; blake3_hasher_init(&h);
        for (auto& [id, st] : status) { blake3_hasher_update(&h, &id, 8); blake3_hasher_update(&h, &st, 1); }
        blake3_hasher_update(&h, &orders, 8); blake3_hasher_update(&h, &accepted, 8);
        blake3_hasher_update(&h, &rejected, 8); blake3_hasher_update(&h, &decisions, 8);
        auto rh = rd.stateHash(); blake3_hasher_update(&h, rh.data(), 32);
        std::array<uint8_t, 32> out{}; blake3_hasher_finalize(&h, out.data(), 32); return out;
    }
};

// Shared logic for "what does an engine do with a core-stream frame". Returns true if it was
// a NewOrder that needs a routing decision.
static bool onCore(const FrameHeader* f, RefData& rd, OmsLite& oms, const NewOrder** orderOut) {
    if (rd.apply(f)) { ++oms.refUpdates; return false; }
    if (auto* o = as<NewOrder>(f)) { ++oms.orders; *orderOut = o; return true; }
    if (auto* r = as<RiskDecision>(f)) {
        oms.status[r->orderId] = uint8_t(r->verdict);
        if (r->verdict == RiskVerdict::Accept) ++oms.accepted; else ++oms.rejected;
    } else if (as<RouteDecision>(f)) ++oms.decisions;
    return false;
}
static std::string hex8(const std::array<uint8_t, 32>& h) {
    char b[17]; for (int i = 0; i < 8; ++i) std::snprintf(b + 2 * i, 3, "%02x", h[i]); return b;
}

int main(int argc, char** argv) {
    if (argc != 3) { std::fprintf(stderr, "usage: %s day1.bin workdir\n", argv[0]); return 2; }
    fs::create_directories(argv[2]);
    std::string coreJ = std::string(argv[2]) + "/core.jnl", mdJ = std::string(argv[2]) + "/md.jnl";
    fs::remove_all(coreJ); fs::remove_all(mdJ);

    Snapshot snap(argv[1]);
    const uint32_t NSYM = snap.lookupTicker("NVDA");            // symbols 1..10
    FakeClock clock;
    BroadcastRing coreRing(1 << 14), mdRing(1 << 16);
    Journal coreJournal(coreJ, 0, 1, 256 * 1024), mdJournal(mdJ, 1);   // small segments: rotation is exercised
    Sequencer core(0, 1, coreJournal, coreRing, clock, 2000);   // checkpoint every 2000 frames
    Sequencer md(1, 8, mdJournal, mdRing, clock, 0);

    // ----- the live engine (sourceId 3): reads both streams, decides, writes back to core
    RefData liveRd; liveRd.load(snap);
    OmsLite liveOms; BookLite liveBook; WatermarkTracker liveWm;
    std::map<uint64_t, int64_t> liveDecisions;                   // core seq of RouteDecision -> expectedCost
    std::vector<std::pair<uint64_t, std::array<uint8_t, 32>>> liveCheckpoints;
    StreamReader liveCore(coreRing, coreJournal, 1), liveMd(mdRing, mdJournal, 1);
    std::vector<FrameHeader*> pending;                            // decisions to sequence after this poll
    std::deque<std::vector<std::byte>> arena;                    // owns the pending frames (stable addresses)
    auto keep = [&](const auto& fr) {
        const auto* p = reinterpret_cast<const std::byte*>(&fr);
        arena.emplace_back(p, p + sizeof(fr));
        pending.push_back(reinterpret_cast<FrameHeader*>(arena.back().data()));
    };

    auto liveHandleCore = [&](const FrameHeader* f) {
        if (auto* ss = as<SessionStart>(f)) {
            if (std::memcmp(ss->snapshotHash.data(), liveRd.snapshotHash().data(), 32) != 0) std::abort();
            return;
        }
        if (as<Checkpoint>(f)) { liveCheckpoints.push_back({f->seq, liveOms.hash(liveRd)}); return; }
        if (auto* rt = as<RouteDecision>(f)) liveDecisions[f->seq] = rt->expectedCost;
        const NewOrder* o = nullptr;
        if (!onCore(f, liveRd, liveOms, &o)) return;
        // risk: tradable, short-sale needs ETB or locate, lot size
        bool ok = liveRd.tradable(o->symbolIdx) && (o->qty % liveRd.lotSize(o->symbolIdx) == 0);
        if (o->side == Side::SellShort) ok = ok && liveRd.shortable(o->symbolIdx) && (liveRd.easyToBorrow(o->symbolIdx) || o->locateId != 0);
        Frame<RiskDecision> rd; rd.init(); rd.header.causeSeq = f->seq; rd.header.sourceId = 3;
        rd.body.orderId = o->orderId; rd.body.accountIdx = o->accountIdx; rd.body.symbolIdx = o->symbolIdx;
        rd.body.verdict = ok ? RiskVerdict::Accept : RiskVerdict::Reject; rd.body.reason = ok ? 0 : 42;
        keep(rd);
        if (!ok) return;
        // route: the decision depends on the book as the live engine currently sees it
        Frame<MdWatermark> wm; wm.init(); wm.header.causeSeq = f->seq; wm.header.sourceId = 3;
        liveWm.fill(wm.body);
        Frame<RouteDecision> rt; rt.init(); rt.header.causeSeq = f->seq; rt.header.sourceId = 3;
        rt.body.parentOrderId = o->orderId; rt.body.childCount = 1; rt.body.venueIds[0] = 2; rt.body.qtys[0] = o->qty;
        rt.body.expectedCost = (o->side == Side::Buy ? liveBook.bestAsk(o->symbolIdx) : liveBook.bestBid(o->symbolIdx)) * o->qty;
        keep(wm); keep(rt);
    };
    auto liveHandleMd = [&](const FrameHeader* f) {
        if (auto* d = as<BookDelta>(f)) liveBook.apply(*d);
        liveWm.observe(f->streamId, f->seq);
    };
    auto liveDrain = [&]() {
        liveCore.poll(liveHandleCore);
        // sequence what the engine produced: RiskDecision alone, then [MdWatermark, RouteDecision] as a batch
        for (size_t i = 0; i < pending.size();) {
            if (pending[i]->templateId == uint16_t(TemplateId::MdWatermark)) {
                FrameHeader* b[2] = {pending[i], pending[i + 1]};
                while (!core.submitBatch(std::span<FrameHeader*>(b, 2))) liveCore.poll(liveHandleCore);
                i += 2;
            } else { while (!core.submit(pending[i])) liveCore.poll(liveHandleCore); ++i; }
        }
        pending.clear(); arena.clear();
    };

    // ----- drive: feed handler and gateway simulators with random interleaving
    core.openSession(snap.businessDate(), snap.version(), snap.contentHash());
    std::mt19937_64 rng(7);
    const int ORDERS = 20000;
    uint64_t mdCount = 0, gwOrders = 0;
    for (int step = 0; step < ORDERS; ++step) {
        int k = int(rng() % 12);
        for (int j = 0; j < k; ++j) {                             // market data burst
            Frame<BookDelta> d; d.init(); d.header.sourceId = 8;
            d.body.symbolIdx = uint32_t(1 + rng() % NSYM); d.body.venueId = 2;
            int64_t ref = liveRd.refPrice(d.body.symbolIdx);
            d.body.side = (rng() & 1) ? BookSide::Bid : BookSide::Ask;
            int64_t tick = 1'000'000; int64_t levels = int64_t(rng() % 20);
            d.body.price = d.body.side == BookSide::Bid ? ref - tick * (1 + levels) : ref + tick * (1 + levels);
            d.body.action = (rng() % 5 == 0) ? BookAction::Delete : BookAction::Set;
            d.body.qty = int64_t(100 * (1 + rng() % 50)); d.body.venueSeq = ++mdCount;
            while (!md.submit(&d.header)) liveMd.poll(liveHandleMd);
        }
        if (rng() % 3 == 0) {                                     // occasional reference update on the core stream
            Frame<ListUpdate> lu; lu.init(); lu.header.sourceId = 15;
            lu.body.listId = ListId::EasyToBorrow; lu.body.symbolIdx = uint32_t(1 + rng() % NSYM);
            lu.body.op = (rng() & 1) ? ListOp::Add : ListOp::Remove;
            while (!core.submit(&lu.header)) liveDrain();
        }
        Frame<NewOrder> o; o.init(); o.header.sourceId = 2; o.header.originTs = clock.nowNs();
        o.body.clOrdId = uint64_t(step); o.body.orderId = (uint64_t(2) << 48) | uint64_t(++gwOrders);
        o.body.accountIdx = 42; o.body.symbolIdx = uint32_t(1 + rng() % NSYM);
        o.body.side = (rng() % 4 == 0) ? Side::SellShort : Side::Buy; o.body.ordType = OrdType::Limit; o.body.tif = Tif::Day;
        // Two draws, sequenced: the operands of + are evaluated in an unspecified order.
        const int64_t lot = int64_t(100 * (1 + rng() % 10));
        const int64_t odd = (rng() % 20 == 0) ? 7 : 0;
        o.body.qty = lot + odd;                                                      // some odd lots -> rejects
        o.body.price = liveRd.refPrice(o.body.symbolIdx); o.body.locateId = (rng() & 1) ? 1 : 0;
        while (!core.submit(&o.header)) liveDrain();
        // the engine polls in an arbitrary order: sometimes md first, sometimes core first
        if (rng() & 1) { liveMd.poll(liveHandleMd); liveDrain(); } else { liveDrain(); liveMd.poll(liveHandleMd); }
    }
    liveMd.poll(liveHandleMd); liveDrain(); liveDrain();
    core.checkpoint(); liveDrain();
    std::printf("live: core seq=%llu md seq=%llu orders=%llu accepted=%llu rejected=%llu decisions=%zu checkpoints=%zu\n",
        (unsigned long long)core.lastSeq(), (unsigned long long)md.lastSeq(), (unsigned long long)liveOms.orders,
        (unsigned long long)liveOms.accepted, (unsigned long long)liveOms.rejected, liveDecisions.size(), liveCheckpoints.size());
    CHECK(liveOms.orders == ORDERS && liveOms.rejected > 0 && liveOms.accepted > 0);
    CHECK(liveOms.decisions == liveOms.accepted);
    CHECK(liveCheckpoints.size() >= 10);
    CHECK(coreJournal.segments() > 4);
    std::printf("journal: %zu segments, %llu frames, %llu bytes\n", coreJournal.segments(), (unsigned long long)coreJournal.frames(), (unsigned long long)coreJournal.sizeBytes());

    // ----- 1. cold replay from the journals, core-only order, md pulled to each watermark
    {
        RefData rd; rd.load(snap); OmsLite oms; BookLite book;
        uint64_t mdPos = 0; size_t verified = 0, mismatches = 0;
        std::vector<std::pair<uint64_t, std::array<uint8_t, 32>>> cps;
        NewOrder lastOrder{}; uint64_t lastWmSeq = 0; size_t riskVerified = 0, riskMismatch = 0;
        coreJournal.replay(1, 0, [&](const FrameHeader* f) {
            if (as<SessionStart>(f)) return;
            if (as<Checkpoint>(f)) { cps.push_back({f->seq, oms.hash(rd)}); return; }
            if (auto* wm = as<MdWatermark>(f)) {
                uint64_t target = 0; for (int i = 0; i < wm->count; ++i) if (wm->streamIds[i] == 1) target = wm->seqs[i];
                if (target > mdPos) { mdJournal.replay(mdPos + 1, target, [&](const FrameHeader* m) { if (auto* d = as<BookDelta>(m)) book.apply(*d); }); mdPos = target; }
                lastWmSeq = f->seq; if (f->flags & FrameFlags::lastInBatch) std::abort();   // watermark is never last in its batch
                return;
            }
            if (auto* rt = as<RouteDecision>(f)) {
                if (lastWmSeq + 1 != f->seq || !(f->flags & FrameFlags::lastInBatch)) std::abort();   // adjacency + flag
                // recompute what the live engine computed, from replayed state only
                const NewOrder& o = lastOrder;
                int64_t expect = (o.side == Side::Buy ? book.bestAsk(o.symbolIdx) : book.bestBid(o.symbolIdx)) * o.qty;
                ++verified; if (expect != rt->expectedCost || liveDecisions.at(f->seq) != rt->expectedCost) ++mismatches;
            }
            if (auto* rk = as<RiskDecision>(f)) {                       // recompute the risk verdict from replayed refdata
                const NewOrder& o = lastOrder;
                bool ok = rd.tradable(o.symbolIdx) && (o.qty % rd.lotSize(o.symbolIdx) == 0);
                if (o.side == Side::SellShort) ok = ok && rd.shortable(o.symbolIdx) && (rd.easyToBorrow(o.symbolIdx) || o.locateId != 0);
                ++riskVerified; if ((rk->verdict == RiskVerdict::Accept) != ok) ++riskMismatch;
            }
            const NewOrder* o = nullptr;
            if (onCore(f, rd, oms, &o)) lastOrder = *o;                  // copy: the replay buffer is reused per frame
        });
        std::printf("cold replay: verified %zu routing decisions (%zu mismatches), %zu risk verdicts (%zu mismatches), %zu checkpoints\n",
            verified, mismatches, riskVerified, riskMismatch, cps.size());
        CHECK(verified == liveDecisions.size() && mismatches == 0);
        CHECK(riskVerified == liveOms.orders && riskMismatch == 0);
        CHECK(cps == liveCheckpoints);
        std::printf("checkpoint hashes identical, last = %s @ seq %llu\n", hex8(cps.back().second).c_str(), (unsigned long long)cps.back().first);
    }

    // ----- 2. late starter: subscribes now (ring head), must refill everything from the journal
    {
        RefData rd; rd.load(snap); OmsLite oms;
        std::vector<std::pair<uint64_t, std::array<uint8_t, 32>>> cps;
        StreamReader late(coreRing, coreJournal, 1);
        core.heartbeat(ComponentState::Live);                        // one live frame after the gap
        uint64_t replayedFlags = 0;
        uint64_t n = late.poll([&](const FrameHeader* f) {
            if (f->flags & FrameFlags::replayed) ++replayedFlags;
            if (as<Checkpoint>(f)) { cps.push_back({f->seq, oms.hash(rd)}); return; }
            const NewOrder* o = nullptr; onCore(f, rd, oms, &o);
        });
        std::printf("late reader: delivered %llu frames, %llu refilled from journal across %llu gap(s), %llu live\n",
            (unsigned long long)n, (unsigned long long)late.refilled(), (unsigned long long)late.gaps(), (unsigned long long)late.live());
        CHECK(late.refilled() > 0 && late.live() >= 1 && cps == liveCheckpoints);
        CHECK(replayedFlags == late.refilled());
        CHECK(late.expected() == core.lastSeq() + 1);
    }

    // ----- 3. crash recovery: torn tail is truncated, sequencer resumes
    uint64_t before = core.lastSeq();
    {
        std::FILE* fp = std::fopen(coreJournal.activeSegmentPath().c_str(), "ab");
        Frame<Heartbeat> hb; hb.init(); hb.header.seq = before + 1;
        std::fwrite(&hb, 1, 30, fp);                                 // partial frame
        std::fclose(fp);
        Journal reopened(coreJ, 0, 1, 256 * 1024);
        CHECK(reopened.lastSeq() == before && reopened.truncatedOnOpen() == 30);
        CHECK(reopened.segments() == coreJournal.segments() && reopened.frames() == coreJournal.frames());
        Sequencer resumed(0, 1, reopened, coreRing, clock, 0);
        CHECK(resumed.nextSeq() == before + 1);
        resumed.heartbeat(ComponentState::Live);
        CHECK(reopened.lastSeq() == before + 1);
        std::printf("crash recovery: truncated 30 torn bytes, resumed at seq %llu\n", (unsigned long long)before + 1);
        // replay from a checkpoint works on the reopened file
        uint64_t got = reopened.replay(liveCheckpoints[3].first + 1, liveCheckpoints[4].first, [](const FrameHeader*) {});
        CHECK(got == liveCheckpoints[4].first - liveCheckpoints[3].first);
    }

    // ----- 4. back-pressure: tiny ring, slow reader
    {
        BroadcastRing tiny(8); FakeClock c2; fs::remove_all(std::string(argv[2]) + "/bp.jnl");
        Journal j(std::string(argv[2]) + "/bp.jnl", 5); Sequencer s(5, 1, j, tiny, c2, 0);
        int r = tiny.subscribe();
        int accepted = 0; Frame<Heartbeat> hb;
        for (int i = 0; i < 20; ++i) { hb.init(); if (s.submit(&hb.header)) ++accepted; }
        CHECK(accepted == 8 && tiny.lag(r) == 8);
        while (tiny.poll(r)) tiny.advance(r);
        hb.init(); CHECK(s.submit(&hb.header));
        CHECK(j.lastSeq() == 9);                                     // nothing was journaled that the ring refused
        std::printf("back-pressure: ring of 8 accepted 8, refused 12, journal seq=%llu\n", (unsigned long long)j.lastSeq());
    }
    // ---- timers, which are messages
    //
    // A strategy that rests and crosses after a while is waiting on a clock, and a clock is the one
    // thing a replay cannot reproduce. So the wait is armed here and fires as an ordinary sequenced
    // frame: replay reads when it went off instead of having to work it out, and the position in
    // the stream is the same both times.
    {
        std::string dir = std::string(argv[2]) + "/timers";
        fs::remove_all(dir);
        Journal j(dir, 0, 1);
        BroadcastRing ring(1 << 10);
        FakeClock clock;
        Sequencer s(0, 1, j, ring, clock, 0);
        clock.t = 1'000;
        CHECK(s.fireTimers(clock.t) == 0);                  // nothing armed, nothing fired
        // armed out of order and with a tie, which is where an unstable order would show
        s.armTimer(7, 300, 3'000);
        s.armTimer(7, 100, 1'000);
        s.armTimer(7, 200, 2'000);
        s.armTimer(9, 150, 1'000);                          // the same instant as timer 100
        CHECK(s.armedTimers() == 4);
        CHECK(s.fireTimers(500) == 0);                      // none due yet
        CHECK(s.armedTimers() == 4);
        const uint64_t before = j.lastSeq();
        CHECK(s.fireTimers(2'000) == 3);                    // the two at 1000 and the one at 2000
        CHECK(s.armedTimers() == 1);
        std::vector<std::pair<uint32_t, int64_t>> fired;
        j.replay(before + 1, 0, [&](const FrameHeader* f) {
            if (const auto* t = as<Timer>(f)) fired.emplace_back(t->timerId, t->fireTs);
        });
        CHECK(fired.size() == 3);
        // earliest first, and the tie broken by id so two runs produce one order
        CHECK(fired[0] == std::make_pair(uint32_t(100), int64_t(1'000)));
        CHECK(fired[1] == std::make_pair(uint32_t(150), int64_t(1'000)));
        CHECK(fired[2] == std::make_pair(uint32_t(200), int64_t(2'000)));
        CHECK(s.fireTimers(10'000) == 1);
        CHECK(s.armedTimers() == 0);
        CHECK(s.fireTimers(10'000) == 0);                   // and a fired timer does not fire again
        std::printf("timers: four armed, fired earliest first with the tie broken by id, "
                    "all four on the journal\n");
    }

    std::printf("sequencer tests ok\n");
    return 0;
}
