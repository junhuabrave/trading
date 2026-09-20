// sim/harness.hpp : wires one core stream, one market-data stream, the engine (risk +
// OMS stand-in + stub router), a venue simulator, feed and client simulators, and drives
// them with a deterministic clock. After the live run, verify() replays the core journal
// cold, pulls market data to each watermark, recomputes every risk verdict, routing plan
// and checkpoint hash, and compares them with what the live engine sequenced.
#pragma once
#include "sequencer.hpp"
#include "reader.hpp"
#include "watermark.hpp"
#include "risk.hpp"
#include "oms.hpp"
#include "identity.hpp"
#include "venue_sim.hpp"
#include "router.hpp"
#include "md_stack.hpp"
#include "client_sim.hpp"
#include <deque>
#include <random>
#include <string>
#include <filesystem>
#include <cstdio>
#include <fstream>
#include <map>

namespace trading::sim {

using namespace trading::seq;
using namespace trading::refdata;
using namespace trading::risk;

// The simulator drives one feed today: Nasdaq ITCH, feedId 1 in the fixture registry. A
// market-data stream is one feed, so the stream id and the feed id are the same number by
// construction rather than by coincidence.
inline constexpr uint32_t FEED_XNAS_ITCH = 1;
// ITCH stamps nanoseconds since the venue day's midnight; this is that midnight, so every venueTs
// the harness sees is an ordinary epoch timestamp.
inline constexpr int64_t SESSION_MIDNIGHT_NS = 1'757'894'400'000'000'000LL;

struct Config {
    std::string snapshot, workdir, eventsFile, drill;
    uint64_t seed = 7; uint32_t steps = 5000; uint32_t checkpointEvery = 2000; uint32_t sessions = 3; uint32_t throttlePerSec = 2000;
    bool adversarial = false; uint64_t segmentBytes = 4 << 20;
    bool benchMode = false;          // steady clock instead of the fake one: seqTs deltas are real elapsed time
};

struct Stats {
    uint64_t orders = 0, accepted = 0, rejected = 0, fills = 0, cancelled = 0, venueRejects = 0, children = 0, checkpoints = 0, coreSeq = 0, mdSeq = 0, dropCopyFills = 0, delayed = 0;
    std::array<uint64_t, 16> placed{};
    std::array<uint8_t, 32> finalHash{};
    uint64_t maxRingLag = 0, unidentifiedMd = 0;
};

inline std::string hex(const std::array<uint8_t, 32>& h, size_t n = 32) { std::string s; char b[3]; for (size_t i = 0; i < n; ++i) { std::snprintf(b, 3, "%02x", h[i]); s += b; } return s; }

// The engine: reacts only to sequenced frames; in Live mode it emits, in Verify mode it recomputes and compares.
class Engine {
public:
    enum class Mode { Live, Verify };
    static constexpr uint32_t BOOK_MAX_VENUE = 9;          // the fixture's venues, internal included

    Engine(const Snapshot& snap, uint32_t sessions, Mode mode, const Journal* mdJournal = nullptr, uint16_t sourceId = 3)
        : mode_(mode), src_(sourceId), risk_(rd_), oms_(4, 1 << 18),
          bookMem_(md::BookSegment::bytesFor(uint32_t(snap.instruments().size()) + 1, BOOK_MAX_VENUE)),
          book_(rd_, bookParams(snap), bookMem_.data()),
          bookReader_(bookMem_.data(), uint32_t(snap.instruments().size()) + 1, BOOK_MAX_VENUE),
          router_(snap, routerParams(sessions)),
          mdJournal_(mdJournal) {
        rd_.load(snap); risk_.load(snap); oms_.load(snap); std::memcpy(snapHash_.data(), snap.contentHash().data(), 32);
        router_.setBook(&bookReader_);
    }

    // Verify mode: md target per cause seq, pre-scanned from the journal, so a decision is
    // evaluated at the same point (and on the same market data) as the live engine did.
    void setWatermarkIndex(const std::vector<std::pair<uint64_t, uint64_t>>* idx) { wmIndex_ = idx; }

    // ----- inputs
    void onMd(const FrameHeader* f) {
        // The engine keeps its own book, built from the sequenced stream. Not because the book
        // builder's shared view is unavailable - in production the router reads exactly that - but
        // because a replay has to reconstruct the book the live engine saw from the journal alone,
        // and the only way to be sure it did is to build it the same way both times.
        book_.apply(f, 0, [](const FrameHeader*) {});
        if (auto* n = as<Nbbo>(f)) oms_.onNbbo(*n);        // the NBBO on every client report
        risk_.apply(f);                                     // Trade and Nbbo feed reference prices and the SSR bid
        md::Identity id = md::identity(f);
        if (id.venueSeq == 0) ++unidentified_;              // a market-data frame nothing can replay to
        wm_.observe(id.feedId, id.venueSeq);                // (feedId, venueSeq), never a sequence of ours
        mdCursor_ = f->seq;                                 // how far we have read in the journal; an implementation detail
    }
    void onCore(const FrameHeader* f) {
        if (auto* ss = as<SessionStart>(f)) { if (std::memcmp(ss->snapshotHash.data(), snapHash_.data(), 32) != 0) throw std::runtime_error("engine: snapshot hash mismatch at SessionStart"); return; }
        if (as<Checkpoint>(f)) { onCheckpoint(f); return; }
        if (auto* ack = as<CheckpointAck>(f)) { if (mode_ == Mode::Verify && f->sourceId == src_) verifyAck(*ack); return; }
        if (auto* w = as<MdWatermark>(f)) { if (mode_ == Mode::Verify && f->sourceId == src_) pullMd(*w); return; }
        // the OMS's own frames come back on the stream: in Verify they must equal what this OMS produced
        if (mode_ == Mode::Verify && f->sourceId == 4) verifyOmsFrame(f);
        if (mode_ == Mode::Verify) pullTo(f->seq);         // market data to where the live engine was when it handled this frame
        // the OMS and the router see everything (they only react to what concerns them). Live: the OMS's
        // outputs for a frame go out as one batch behind a watermark, so replay can regenerate the NBBO on them
        if (mode_ == Mode::Live) {
            size_t before = pending_.size(); bool first = true;
            oms_.apply(f, [&](FrameHeader* out) { if (first) { emitWatermarks(f->seq); first = false; } emit(out, f->seq, false); });
            (void)before;
        } else oms_.apply(f, [&](FrameHeader* out) { const auto* p = reinterpret_cast<const std::byte*>(out); omsExpected_.emplace_back(p, p + out->frameLength); });
        router_.apply(f);
        risk_.apply(f);
        if (auto* o = as<NewOrder>(f)) { lastOrder_ = *o; lastHdr_ = *f; lastIsReplace_ = false; if (mode_ == Mode::Live) decideLive(f); else decideVerify(f); return; }
        if (auto* r = as<ReplaceOrder>(f)) { lastReplace_ = *r; lastHdr_ = *f; lastIsReplace_ = true; if (mode_ == Mode::Live) decideLive(f); else decideVerify(f); return; }
        if (auto* d = as<RiskDecision>(f)) {
            if (f->sourceId != src_) return;
            // whether this decision answers a replace is on the decision itself (the risk engine sets the
            // Replace check bit); the last order frame read is stale after a burst
            bool replace = (d->checkMask & Check::Replace) != 0;
            if (mode_ == Mode::Verify) verifyDecision(f, *d);
            if (d->verdict == RiskVerdict::Accept && !replace) { if (mode_ == Mode::Live) routeLive(f, d->orderId); else routeVerify(f, d->orderId); }
            return;
        }
        if (auto* st = as<OrderState>(f)) {                // the OMS asks for a plan when a parent has leaves and no live child
            if (f->sourceId == 4 && st->reason == uint16_t(Reason::RerouteRequired)) { if (mode_ == Mode::Live) routeLive(f, st->orderId); else routeVerify(f, st->orderId); }
            return;
        }
        if (auto* rt = as<RouteDecision>(f)) { if (mode_ == Mode::Verify && f->sourceId == src_) verifyRoute(f, *rt); return; }
        if (auto* c = as<ChildOrder>(f)) { if (mode_ == Mode::Verify && f->sourceId == src_) verifyChild(f, *c); return; }
    }

    // ----- outputs (Live): batches of frames the harness sequences after each poll
    struct Batch { std::vector<std::vector<std::byte>> frames; };
    std::deque<Batch>& pending() { return pending_; }

    // ----- state
    std::array<uint8_t, 32> hash() const {
        blake3_hasher h; blake3_hasher_init(&h);
        auto r = risk_.stateHash(); blake3_hasher_update(&h, r.data(), 32);
        oms_.hashInto(h); router_.hashInto(h);
        std::array<uint8_t, 32> out{}; blake3_hasher_finalize(&h, out.data(), 32); return out;
    }
    const RiskEngine& risk() const { return risk_; } const oms::Oms& oms() const { return oms_; } const router::Router& router() const { return router_; }
    uint64_t checkpoints() const { return checkpoints_; }
    uint64_t verified() const { return verified_; } uint64_t mismatches() const { return mismatches_; }
    uint64_t unidentified() const { return unidentified_; }
    const std::string& firstMismatch() const { return firstMismatch_; }

private:
    void emit(FrameHeader* out, uint64_t cause, bool newBatch = true) {
        if (mode_ != Mode::Live) return;
        out->sourceId = out->sourceId ? out->sourceId : src_; if (!out->causeSeq) out->causeSeq = cause;
        if (newBatch || pending_.empty()) pending_.emplace_back();
        const auto* p = reinterpret_cast<const std::byte*>(out);
        pending_.back().frames.emplace_back(p, p + out->frameLength);
    }
    void emitWatermarks(uint64_t cause) {
        for (size_t b = 0; b < wm_.banks(); ++b) {
            Frame<MdWatermark> w; w.init(); w.header.sourceId = src_; wm_.fill(w.body, b);
            emit(&w.header, cause, b == 0);
        }
    }
    void decideLive(const FrameHeader* f) {
        Frame<RiskDecision> d; d.init(); d.header.sourceId = src_;
        if (lastIsReplace_) risk_.evaluateReplace(f, lastReplace_, d.body); else risk_.evaluate(f, lastOrder_, d.body);
        emitWatermarks(f->seq); emit(&d.header, f->seq, false);          // [watermark(s), decision] is one batch
    }
    // The router's view of a parent: what is left to route, at the current price.
    static bool routeInput(const oms::Order* o, NewOrder& n) {
        if (!o || o->leaves <= 0) return false;
        n = NewOrder{}; n.orderId = o->orderId; n.clOrdId = o->clOrdId; n.accountIdx = o->accountIdx; n.symbolIdx = o->symbolIdx; n.side = o->side;
        n.ordType = o->ordType; n.tif = o->tif; n.orderFlags = o->orderFlags; n.qty = o->leaves; n.price = o->price; n.displayQty = o->displayQty; return true;
    }
    // RouteDecision records up to eight venues and the first four quantities; the ChildOrder frames
    // beside it in the same atomic batch carry every child in full. The decision is the summary and
    // the reason, the children are the record, and the batch is what keeps them together.
    static void fillDecision(const NewOrder& o, const router::Plan& p, RouteDecision& rt) {
        rt.parentOrderId = o.orderId;
        rt.strategyTag = p.strategyTag;
        rt.childCount = p.count;
        for (uint8_t i = 0; i < p.count && i < 8; ++i) rt.venueIds[i] = p.children[i].venueId;
        for (uint8_t i = 0; i < p.count && i < 4; ++i) rt.qtys[i] = p.children[i].qty;
        rt.expectedCost = p.expectedCost;
        rt.rejectedAltCost = p.rejectedAltCost;
        rt.rejectedAltVenue = p.rejectedAltVenue;
        rt.reason = p.reason;
    }
    void fillChild(const NewOrder& o, const router::Child& rc, ChildOrder& c, uint64_t childId, int64_t at) const {
        c.childOrderId = childId; c.parentOrderId = o.orderId; c.accountIdx = o.accountIdx; c.symbolIdx = o.symbolIdx;
        c.venueId = rc.venueId; c.venueSessionIdx = rc.venueSessionIdx;
        c.side = o.side; c.ordType = rc.ordType; c.tif = rc.tif;
        c.orderFlags = rc.orderFlags; c.strategyTag = rc.strategyTag;
        c.qty = rc.qty; c.price = rc.price; c.displayQty = rc.displayQty;
        c.releaseTs = at + rc.releaseOffsetNs;              // the slowest venue sets the clock
    }
    void routeLive(const FrameHeader* f, uint64_t orderId) {
        NewOrder o; if (!routeInput(oms_.find(orderId), o)) return;
        router::Plan p;
        router_.plan(o, o.qty, f->seqTs, p);                // false means no children, which the OMS reads as a decline
        router_.placed(p);
        Frame<RouteDecision> rt; rt.init(); rt.header.sourceId = src_;
        fillDecision(o, p, rt.body);
        emitWatermarks(f->seq);
        emit(&rt.header, f->seq, false);
        for (uint8_t i = 0; i < p.count; ++i) {
            Frame<ChildOrder> c; c.init(); c.header.sourceId = src_;
            fillChild(o, p.children[i], c.body, (uint64_t(src_) << 48) | ++childCounter_, f->seqTs);
            emit(&c.header, f->seq, false);
        }
    }
    void onCheckpoint(const FrameHeader* f) {
        auto h = hash(); ++checkpoints_;
        const Checkpoint* cp = as<Checkpoint>(f);
        if (mode_ == Mode::Live) {
            Frame<CheckpointAck> a; a.init(); a.header.sourceId = src_; a.body.checkpointId = cp->checkpointId;
            std::memcpy(a.body.stateHash.data(), h.data(), 32); a.body.engineSeq = f->seq; emit(&a.header, f->seq);
        } else expectedAcks_.push_back({cp->checkpointId, h});
    }
    // ----- verify mode
    void pullMd(const MdWatermark& w) {
        for (int i = 0; i < w.count; ++i) pullToVenueSeq(w.streamIds[i], w.seqs[i]);
    }
    // Feed the journal forward until this feed has reached the venue sequence the live engine had
    // consumed. One feed today, so stopping at the first frame past the target is exact. With
    // several feeds interleaved in one journal it would be conservative; MD-6 removes the question
    // by giving each feed its own journal keyed by its own venueSeq, where this becomes a seek.
    void pullToVenueSeq(uint32_t feedId, uint64_t targetVenueSeq) {
        if (!mdJournal_ || targetVenueSeq == 0) return;
        if (wm_.seqFor(feedId) >= targetVenueSeq) return;
        uint64_t last = mdJournal_->replayWhile(mdCursor_ + 1, [&](const FrameHeader* m) {
            if (md::identity(m).venueSeq > targetVenueSeq) return false;
            onMd(m); return true;
        });
        if (last) mdCursor_ = last;
    }
    void note(const std::string& what) { ++mismatches_; if (firstMismatch_.empty()) firstMismatch_ = what; }
    // Decisions are evaluated when the order is read (as live did) and matched to the sequenced
    // decision by causeSeq: under back-pressure the live engine reads many orders before their
    // decisions are sequenced, so "the last order" is not the one a decision belongs to.
    void decideVerify(const FrameHeader* f) {
        pullTo(f->seq);                                        // market data to where the live engine was when it decided
        Expected e{}; e.replace = lastIsReplace_;
        if (lastIsReplace_) risk_.evaluateReplace(f, lastReplace_, e.d); else risk_.evaluate(f, lastOrder_, e.d);
        expected_[f->seq] = e;
    }
    void verifyDecision(const FrameHeader* f, const RiskDecision& d) {
        ++verified_;
        auto it = expected_.find(f->causeSeq);
        if (it == expected_.end()) { note("RiskDecision at seq " + std::to_string(f->seq) + " with no order at cause " + std::to_string(f->causeSeq)); return; }
        const RiskDecision& e = it->second.d;
        if (e.orderId != d.orderId || e.verdict != d.verdict || e.reason != d.reason || e.buyingPowerAfter != d.buyingPowerAfter || e.notional != d.notional)
            note("RiskDecision for order " + std::to_string(d.orderId) + " (cause " + std::to_string(f->causeSeq) + "): live reason " + std::to_string(d.reason)
                 + " mask " + std::to_string(d.checkMask) + " vs replay " + std::to_string(e.reason) + " mask " + std::to_string(e.checkMask));
        expected_.erase(it);
    }
    void routeVerify(const FrameHeader* f, uint64_t orderId) {
        NewOrder o; if (!routeInput(oms_.find(orderId), o)) return;
        pullTo(f->seq);                                        // the book as the live engine saw it when it planned
        router::Plan p;
        router_.plan(o, o.qty, f->seqTs, p);
        router_.placed(p);
        plans_[f->seq] = PlannedRoute{p, 0};
    }
    // Every frame the live OMS sequenced must be exactly what this OMS produced from the same inputs,
    // in the same order: the dispute drill (regenerate the day's client reports) runs on every scenario.
    void verifyOmsFrame(const FrameHeader* f) {
        ++verified_;
        if (omsExpected_.empty()) { note("OMS frame at seq " + std::to_string(f->seq) + " that the replay did not produce"); return; }
        const auto& e = omsExpected_.front(); const auto* eh = reinterpret_cast<const FrameHeader*>(e.data());
        if (eh->templateId != f->templateId || eh->causeSeq != f->causeSeq || eh->frameLength != f->frameLength
            || std::memcmp(e.data() + sizeof(FrameHeader), reinterpret_cast<const std::byte*>(f) + sizeof(FrameHeader), f->frameLength - sizeof(FrameHeader)) != 0)
            note("OMS frame at seq " + std::to_string(f->seq) + " (" + lookup(f->templateId)->name + ", cause " + std::to_string(f->causeSeq) + ") differs from the replay's");
        omsExpected_.pop_front();
    }
    void pullTo(uint64_t causeSeq) {
        if (!wmIndex_ || !mdJournal_) return;
        auto it = std::lower_bound(wmIndex_->begin(), wmIndex_->end(), std::make_pair(causeSeq, uint64_t(0)));
        if (it != wmIndex_->end() && it->first == causeSeq) pullToVenueSeq(FEED_XNAS_ITCH, it->second);
    }
    // The plan the replay computed must be the plan the live engine sequenced, field for field.
    // Anything less than that - comparing only a cost, or only the first venue - would pass a
    // router that had quietly changed where it sent the second child.
    void verifyRoute(const FrameHeader* f, const RouteDecision& rt) {
        ++verified_;
        auto it = plans_.find(f->causeSeq);
        if (it == plans_.end()) { note("RouteDecision with no plan at cause " + std::to_string(f->causeSeq)); return; }
        const router::Plan& p = it->second.p;
        auto bad = [&](const char* what) {
            note(std::string("RouteDecision ") + what + " for parent " + std::to_string(rt.parentOrderId)
                 + " (cause " + std::to_string(f->causeSeq) + ")");
        };
        if (p.count != rt.childCount) bad("child count");
        else if (p.expectedCost != rt.expectedCost) bad("expected cost");
        else if (p.rejectedAltCost != rt.rejectedAltCost || p.rejectedAltVenue != rt.rejectedAltVenue) bad("rejected alternative");
        else if (p.reason != rt.reason) bad("reason");
        else if (p.strategyTag != rt.strategyTag) bad("strategy");
        else for (uint8_t i = 0; i < p.count && i < 8; ++i)
            if (p.children[i].venueId != rt.venueIds[i]) { bad("venue order"); break; }
        if (rt.childCount == 0) plans_.erase(it);           // a declined plan has no children to match
    }
    void verifyChild(const FrameHeader* f, const ChildOrder& c) {
        ++verified_;
        auto it = plans_.find(f->causeSeq);
        if (it == plans_.end()) { note("ChildOrder with no plan at cause " + std::to_string(f->causeSeq)); return; }
        PlannedRoute& pr = it->second;
        if (pr.seen >= pr.p.count) { note("ChildOrder beyond the plan at cause " + std::to_string(f->causeSeq)); return; }
        const router::Child& e = pr.p.children[pr.seen++];
        if (c.venueId != e.venueId || c.venueSessionIdx != e.venueSessionIdx || c.qty != e.qty
            || c.price != e.price || c.tif != e.tif || c.orderFlags != e.orderFlags)
            note("ChildOrder " + std::to_string(pr.seen - 1) + " differs from the plan at cause " + std::to_string(f->causeSeq));
        if (pr.seen >= pr.p.count) plans_.erase(it);
    }
    void verifyAck(const CheckpointAck& a) {
        ++verified_;
        for (auto& e : expectedAcks_) if (e.first == a.checkpointId) {
            if (std::memcmp(e.second.data(), a.stateHash.data(), 32) != 0) note("CheckpointAck " + std::to_string(a.checkpointId) + " hash differs");
            return;
        }
        note("CheckpointAck without a Checkpoint");
    }

    Mode mode_; uint16_t src_;
    static md::BookBuilder::Params bookParams(const Snapshot& snap) {
        md::BookBuilder::Params b{};
        b.sourceId = 3; b.feedId = FEED_XNAS_ITCH;
        b.maxSymbolIdx = uint32_t(snap.instruments().size()) + 1;
        b.maxVenueId = BOOK_MAX_VENUE;
        return b;
    }
    static router::Router::Params routerParams(uint32_t sessions) {
        router::Router::Params r{};
        r.sourceId = 3; r.maxVenueId = 4; r.sessionsPerVenue = uint16_t(sessions);
        return r;
    }

    RefData rd_; RiskEngine risk_; oms::Oms oms_;
    std::vector<std::byte> bookMem_; md::BookBuilder book_; md::BookReader bookReader_;
    router::Router router_; WatermarkTracker wm_;
    std::deque<std::vector<std::byte>> omsExpected_;
    const Journal* mdJournal_; uint64_t mdCursor_ = 0; const std::vector<std::pair<uint64_t, uint64_t>>* wmIndex_ = nullptr;
    std::array<uint8_t, 32> snapHash_{};
    NewOrder lastOrder_{}; ReplaceOrder lastReplace_{}; FrameHeader lastHdr_{}; bool lastIsReplace_ = false;
    struct Expected { RiskDecision d; bool replace; };
    struct PlannedRoute { router::Plan p; uint8_t seen; };
    std::map<uint64_t, Expected> expected_; std::map<uint64_t, PlannedRoute> plans_;   // keyed by cause seq (verify only)
    std::deque<Batch> pending_;
    std::vector<std::pair<uint64_t, std::array<uint8_t, 32>>> expectedAcks_;
    uint64_t childCounter_ = 0, checkpoints_ = 0, verified_ = 0, mismatches_ = 0, unidentified_ = 0; std::string firstMismatch_;
};

class Harness {
public:
    explicit Harness(Config cfg) : cfg_(std::move(cfg)) {}

    Stats runLive() {
        namespace fs = std::filesystem;
        fs::create_directories(cfg_.workdir);
        std::string coreDir = cfg_.workdir + "/core.jnl", mdDir = cfg_.workdir + "/md.jnl";
        fs::remove_all(coreDir); fs::remove_all(mdDir);
        Snapshot snap(cfg_.snapshot);
        const uint32_t NSYM = uint32_t(std::count_if(snap.instruments().begin(), snap.instruments().end(), [](const SymbolRecord& r) { return r.status != 0; }));
        FakeClock fake; SteadyClock steady; Clock& clock = cfg_.benchMode ? static_cast<Clock&>(steady) : static_cast<Clock&>(fake);
        std::mt19937_64 rng(cfg_.seed);
        BroadcastRing coreRing(1 << 14), mdRing(1 << 16);
        Journal coreJ(coreDir, 0, 1, cfg_.segmentBytes), mdJ(mdDir, FEED_XNAS_ITCH, 1, cfg_.segmentBytes);
        Sequencer core(0, 1, coreJ, coreRing, clock, cfg_.checkpointEvery), md(FEED_XNAS_ITCH, 8, mdJ, mdRing, clock, 0);
        uint64_t maxLag = 0;
        auto tick = [&]() -> int64_t { if (cfg_.benchMode) return steady.nowNs(); fake.t += 200'000 + int64_t(rng() % 1'800'000); return fake.t; };

        Engine eng(snap, cfg_.sessions, Engine::Mode::Live);
        StreamReader engCore(coreRing, coreJ, 1), engMd(mdRing, mdJ, 1), venueCore(coreRing, coreJ, 1);
        VenueSim venue(VenueSim::Params{2, 7, cfg_.sessions, cfg_.throttlePerSec, 12'000}, snap.instruments().size() + 1);
        // The real stack, not a stand-in: ITCH bytes on two lines, arbitrated, decoded, and built
        // into a book whose top of book is what the client simulator prices against. Every message
        // of every simulated day now goes through the components a real day would.
        const std::string bookPath = cfg_.workdir + "/book.seg";
        fs::remove(bookPath);
        MdStack::Params msp{};
        msp.feedId = FEED_XNAS_ITCH; msp.venueId = 2; msp.sourceId = 8;
        msp.symbols = NSYM; msp.maxVenueId = 4;
        msp.midnight = SESSION_MIDNIGHT_NS;
        msp.seed = cfg_.seed ^ 0xFEED;
        BookSegment bookSeg(bookPath, NSYM, 4, snap.businessDate(), true);
        MdStack feed(snap, engRef(eng), msp, bookSeg.base());
        ClientSim client(ClientSim::Params{42, 2, 9, NSYM, cfg_.adversarial, 15, 30}, cfg_.seed ^ 0xC11E);

        auto flushEngine = [&]() {
            auto& pend = eng.pending();
            while (!pend.empty()) {
                Engine::Batch b = std::move(pend.front()); pend.pop_front();
                std::vector<FrameHeader*> hs; for (auto& f : b.frames) hs.push_back(reinterpret_cast<FrameHeader*>(f.data()));
                while (!core.submitBatch(std::span<FrameHeader*>(hs))) engCore.poll([&](const FrameHeader* f) { eng.onCore(f); });
            }
        };
        auto drainCore = [&]() { if (cfg_.benchMode) { uint64_t l = coreRing.lag(0); if (l > maxLag) maxLag = l; } engCore.poll([&](const FrameHeader* f) { eng.onCore(f); }); flushEngine(); };
        auto drainMd = [&]() { engMd.poll([&](const FrameHeader* f) { eng.onMd(f); }); };
        auto submitCore = [&](FrameHeader* f) { f->flags = uint16_t(f->flags | FrameFlags::simulated); while (!core.submit(f)) drainCore(); };
        auto venueEmit = [&](FrameHeader* f) { while (!core.submit(f)) drainCore(); };

        // session set-up as sequenced messages, then the day
        core.openSession(snap.businessDate(), snap.version(), snap.contentHash());
        setupAccount(submitCore, 42);
        drainCore();
        std::vector<std::vector<std::byte>> events; size_t eventIdx = 0;
        if (!cfg_.eventsFile.empty()) events = loadEvents(cfg_.eventsFile);
        const uint32_t steps = events.empty() ? cfg_.steps : uint32_t(events.size());
        for (uint32_t step = 0; step < steps; ++step) {
            int64_t now = tick();                                          // 0.2 to 2 ms between steps on the fake clock
            if (!events.empty()) {
                auto* f = reinterpret_cast<FrameHeader*>(events[eventIdx++].data());
                if (!cfg_.benchMode && f->originTs > fake.t) fake.t = f->originTs;
                if (f->streamId == 1) { f->flags = uint16_t(f->flags | FrameFlags::simulated); while (!md.submit(f)) drainMd(); venue.onMd(f); }
                else submitCore(f);
            } else {
                feed.burst(uint32_t(1 + rng() % 3), now, [&](const FrameHeader* cf) {
                    auto* f = const_cast<FrameHeader*>(cf);
                    f->flags = uint16_t(f->flags | FrameFlags::simulated);
                    // A per-feed stream is replayed to a watermark of (feedId, venueSeq), so a
                    // message the venue does not sequence cannot go on one: there is no position to
                    // replay to. VenueStatus is the case in practice - a venue-wide open or close
                    // rather than a fact about a symbol - and it belongs on the core stream, which
                    // is replayed by a sequence of ours.
                    if (md::identityVenueSeq(f) == 0) { submitCore(f); return; }
                    while (!md.submit(f)) drainMd();
                    venue.onMd(f);
                });
                if (cfg_.drill == "rate-burst" && step == steps / 2) client.burst(2500, now, [&](uint32_t s) { return feed.mid(s); }, submitCore);
                else client.step(now, [&](uint32_t s) { return feed.mid(s); }, submitCore);
            }
            if (cfg_.drill == "kill" && (step == steps / 2 || step == 3 * steps / 4)) {
                Frame<KillSwitch> k; k.init(); k.header.sourceId = 13; k.body.release = step == steps / 2 ? 0 : 1; k.body.operatorId = 1; submitCore(&k.header);
            }
            if (cfg_.drill == "session-drop" && step == steps / 3) venue.dropSession(0, now, venueEmit);
            if (cfg_.drill == "session-drop" && step == steps / 2) venue.restoreSession(0, now, venueEmit);
            venueCore.poll([&](const FrameHeader* f) { venue.onCore(f, now); });
            venue.flush(now, venueEmit); venue.process(now, venueEmit);
            if (rng() & 1) { drainMd(); drainCore(); } else { drainCore(); drainMd(); }
        }
        // settle: let in-flight children arrive and everything drain
        for (int i = 0; i < 64; ++i) { int64_t t = cfg_.benchMode ? steady.nowNs() : (fake.t += 1'000'000); venueCore.poll([&](const FrameHeader* f) { venue.onCore(f, t); }); venue.flush(t, venueEmit); venue.process(t, venueEmit); drainMd(); drainCore(); }
        core.checkpoint(); drainCore(); drainCore();

        Stats s; s.orders = eng.oms().orders(); s.accepted = eng.oms().accepted(); s.rejected = eng.oms().rejected();
        s.fills = eng.oms().fills(); s.cancelled = eng.oms().cancelled(); s.venueRejects = eng.oms().venueRejected(); s.children = eng.router().children();
        s.checkpoints = eng.checkpoints(); s.coreSeq = core.lastSeq(); s.mdSeq = md.lastSeq(); s.delayed = venue.delayed();
        for (const auto& e : venue.dropCopy()) if (e.execType == ExecType::Fill || e.execType == ExecType::PartialFill) ++s.dropCopyFills;
        s.placed = eng.router().placements(2); s.finalHash = eng.hash(); s.maxRingLag = maxLag; s.unidentifiedMd = eng.unidentified();
        liveStats_ = s; return s;
    }

    // Front-to-back stage latencies from the core journal alone: originTs to seqTs for ingress, then
    // each causeSeq hop. No instrumentation in the engines; the header carries it.
    struct Stage { std::string name; std::vector<double> ns; };
    std::vector<Stage> stageLatencies() {
        Journal coreJ(cfg_.workdir + "/core.jnl", 0, 1, cfg_.segmentBytes);
        struct Rec { uint16_t tid; int64_t seqTs, originTs; uint64_t cause; };
        std::vector<Rec> recs(coreJ.lastSeq() + 2);
        std::vector<Stage> st = {{"f2b.ingress_origin_to_seq", {}}, {"f2b.risk_order_to_decision", {}}, {"f2b.route_decision_to_child", {}},
                                 {"f2b.venue_child_to_ack", {}}, {"f2b.oms_venue_exec_to_client_report", {}}, {"f2b.order_origin_to_child_sequenced", {}}};
        coreJ.replay(1, 0, [&](const FrameHeader* f) {
            recs[f->seq] = {f->templateId, f->seqTs, f->originTs, f->causeSeq};
            auto hop = [&](size_t i) { const Rec& c = recs[f->causeSeq]; st[i].ns.push_back(double(f->seqTs - c.seqTs)); };
            if (f->templateId == uint16_t(TemplateId::NewOrder) && f->originTs) st[0].ns.push_back(double(f->seqTs - f->originTs));
            else if (f->templateId == uint16_t(TemplateId::RiskDecision) && f->sourceId == 3 && recs[f->causeSeq].tid == uint16_t(TemplateId::NewOrder)) hop(1);
            else if (f->templateId == uint16_t(TemplateId::ChildOrder) && f->sourceId == 3) {
                hop(2);
                const Rec& rd = recs[f->causeSeq]; const Rec& o = recs[rd.cause];
                if (o.tid == uint16_t(TemplateId::NewOrder) && o.originTs) st[5].ns.push_back(double(f->seqTs - o.originTs));
            }
            else if (f->templateId == uint16_t(TemplateId::VenueAck) && f->causeSeq) hop(3);
            else if (f->templateId == uint16_t(TemplateId::ExecReport) && f->sourceId == 4 && f->causeSeq && recs[f->causeSeq].tid == uint16_t(TemplateId::ExecReport)) hop(4);
        });
        return st;
    }

    // Cold replay of the core journal; market data pulled to each watermark. Returns mismatches.
    struct VerifyResult { uint64_t verified = 0, mismatches = 0, frames = 0, checkpoints = 0; std::array<uint8_t, 32> finalHash{}; std::string first; };
    VerifyResult verify() {
        Snapshot snap(cfg_.snapshot);
        Journal coreJ(cfg_.workdir + "/core.jnl", 0, 1, cfg_.segmentBytes), mdJ(cfg_.workdir + "/md.jnl", FEED_XNAS_ITCH, 1, cfg_.segmentBytes);
        Engine eng(snap, cfg_.sessions, Engine::Mode::Verify, &mdJ);
        std::vector<std::pair<uint64_t, uint64_t>> wmIndex;      // (cause seq, md target) for the engine's watermark batches
        coreJ.replay(1, 0, [&](const FrameHeader* f) {
            if (auto* w = as<MdWatermark>(f); w && f->sourceId == 3) {
                uint64_t target = 0; for (int i = 0; i < w->count; ++i) if (w->streamIds[i] == FEED_XNAS_ITCH) target = w->seqs[i];
                if (wmIndex.empty() || wmIndex.back().first != f->causeSeq) wmIndex.push_back({f->causeSeq, target});
            }
        });
        std::sort(wmIndex.begin(), wmIndex.end());
        eng.setWatermarkIndex(&wmIndex);
        VerifyResult r;
        r.frames = coreJ.replay(1, 0, [&](const FrameHeader* f) { eng.onCore(f); });
        r.verified = eng.verified(); r.mismatches = eng.mismatches(); r.checkpoints = eng.checkpoints(); r.finalHash = eng.hash(); r.first = eng.firstMismatch();
        return r;
    }
    const Stats& liveStats() const { return liveStats_; }

private:
    static const RefData& engRef(const Engine& e) { return e.risk().refData(); }
    template <class Submit> void setupAccount(Submit& submit, uint32_t acct) {
        Frame<LimitUpdate> l; l.init(); l.header.sourceId = 13; l.body.accountIdx = acct; l.body.maxOrderQty = 5000; l.body.maxOrderNotional = 5'000'000LL * 100'000'000LL;
        l.body.maxGrossExposure = 500'000'000LL * 100'000'000LL; l.body.maxNetExposure = 300'000'000LL * 100'000'000LL; l.body.priceCollarBps = 500; l.body.maxMsgRate = 2000; l.body.maxOpenOrders = 2000;
        l.body.dupWindowMs = cfg_.adversarial ? 50 : 0; submit(&l.header);
        Frame<BuyingPowerUpdate> b; b.init(); b.header.sourceId = 11; b.body.accountIdx = acct; b.body.buyingPower = 1'000'000'000LL * 100'000'000LL; submit(&b.header);
        Frame<LocateGranted> g; g.init(); g.header.sourceId = 10; g.body.locateId = 1; g.body.accountIdx = acct; g.body.symbolIdx = 7; g.body.qty = 1'000'000; g.body.result = LocateResult::Granted; g.body.expiryTs = INT64_MAX; submit(&g.header);
    }
    static std::vector<std::vector<std::byte>> loadEvents(const std::string& path) {
        std::ifstream in(path, std::ios::binary); std::vector<char> raw((std::istreambuf_iterator<char>(in)), {});
        std::vector<std::vector<std::byte>> out; size_t off = 0;
        while (off + sizeof(FrameHeader) <= raw.size()) {
            const auto* h = reinterpret_cast<const FrameHeader*>(raw.data() + off);
            if (h->frameLength < sizeof(FrameHeader) || off + h->frameLength > raw.size()) throw std::runtime_error("events: bad frame");
            const auto* p = reinterpret_cast<const std::byte*>(raw.data() + off);
            out.emplace_back(p, p + h->frameLength); off += h->frameLength;
        }
        return out;
    }
    Config cfg_; Stats liveStats_;
};

} // namespace trading::sim
