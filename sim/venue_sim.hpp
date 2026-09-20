// sim/venue_sim.hpp : a simulated exchange.
//
// Keeps a level book per symbol from the feed it is shown, runs several order-entry
// sessions each with its own message throttle and cancel-on-disconnect scope, and answers
// ChildOrders with VenueAck, venue-level ExecReports (childOrderId set) and VenueRejects.
// Aggressive children cross displayed liquidity in price order; passive children rest with
// a queue position equal to the displayed quantity at their level on arrival and fill as
// trades at that price consume the queue ahead of them. Every execution is also recorded
// on a drop-copy list so the harness can prove the OMS-side reconciliation.
#pragma once
#include "book_lite.hpp"
#include <deque>
#include <queue>
#include <functional>
#include <cstring>
#include <cstdio>

namespace trading::sim {

class VenueSim {
public:
    struct Params {
        uint16_t venueId = 2; uint16_t sourceId = 7; uint32_t sessions = 3;
        uint32_t throttlePerSec = 2000; int64_t latencyNs = 12'000;
    };
    using Emit = std::function<void(FrameHeader*)>;

    explicit VenueSim(Params p, size_t symbols = 4096) : p_(p), book_(symbols), sessions_(p.sessions) {}

    // ----- what the venue sees from the market (the harness shows it every md frame)
    void onMd(const FrameHeader* f) {
        if (auto* d = as<BookDelta>(f)) {
            if (d->venueId != p_.venueId) return;
            book_.apply(*d);
            // displayed size shrank at a level: resting orders there are closer to the front
            for (auto& r : resting_) if (r.symbolIdx == d->symbolIdx && r.price == d->price && r.bookSide() == d->side && r.queueAhead > d->qty) r.queueAhead = d->qty;
        } else if (auto* t = as<Trade>(f)) {
            if (t->venueId != p_.venueId) return;
            pendingTrades_.push_back(*t);
        }
    }

    // ----- what the venue sees from us (its session workers): children and cancels
    void onCore(const FrameHeader* f, int64_t now) {
        if (auto* c = as<ChildOrder>(f)) {
            if (c->venueId != p_.venueId) return;
            if (c->venueSessionIdx >= sessions_.size() || !sessions_[c->venueSessionIdx].up) { rejectChild(*c, 1, now); return; }
            Arrival a; a.child = *c; a.childSeq = f->seq;
            a.arriveTs = (c->releaseTs > now ? c->releaseTs : now) + p_.latencyNs;
            arrivals_.push(a);
        } else if (auto* c = as<CancelOrder>(f)) {
            if (f->sourceId != 4) return;                                   // only the OMS addresses children; orderId is the childOrderId
            cancels_.push_back({c->orderId, now + p_.latencyNs});
        }
    }

    // ----- advance to `now`: deliver arrivals, apply trades to resting orders, run cancels
    void process(int64_t now, const Emit& emit) {
        // sessions refill their throttle every second
        for (auto& s : sessions_) if (now - s.windowTs >= 1'000'000'000LL) { s.windowTs = now - (now % 1'000'000'000LL); s.used = 0; }
        while (!arrivals_.empty() && arrivals_.top().arriveTs <= now) {
            Arrival a = arrivals_.top(); arrivals_.pop();
            Session& s = sessions_[a.child.venueSessionIdx];
            if (!s.up) { rejectChild(a.child, 1, now, emit, a.childSeq); continue; }
            if (s.used >= p_.throttlePerSec) {                       // throttled: the port delays, never drops
                a.arriveTs = s.windowTs + 1'000'000'000LL; ++s.delayed; arrivals_.push(a); continue;
            }
            ++s.used; ++s.received;
            if (a.child.qty <= 0 || (a.child.ordType == OrdType::Limit && a.child.price <= 0)) { rejectChild(a.child, 2, now, emit, a.childSeq); continue; }
            ack(a.child, a.childSeq, now, emit);
            ++s.openOrders;
            match(a.child, a.childSeq, s, now, emit);
        }
        for (const Trade& t : pendingTrades_) tradeFills(t, now, emit);
        pendingTrades_.clear();
        for (size_t i = 0; i < cancels_.size();) {
            if (cancels_[i].dueTs <= now) { cancelChild(cancels_[i].childId, now, emit, 0); cancels_[i] = cancels_.back(); cancels_.pop_back(); }
            else ++i;
        }
        if (now - lastStatusTs_ >= 250'000'000LL) { lastStatusTs_ = now; for (uint16_t i = 0; i < sessions_.size(); ++i) status(i, now, emit); }
    }

    // ----- drills
    void dropSession(uint16_t idx, int64_t now, const Emit& emit) {
        Session& s = sessions_[idx]; if (!s.up) return;
        s.up = false;
        for (size_t i = 0; i < resting_.size();) {
            if (resting_[i].sessionIdx == idx) { Rest r = resting_[i]; resting_[i] = resting_.back(); resting_.pop_back(); execCancelled(r, now, emit, 2); --s.openOrders; }
            else ++i;
        }
        status(idx, now, emit);
    }
    void restoreSession(uint16_t idx, int64_t now, const Emit& emit) { sessions_[idx].up = true; status(idx, now, emit); }

    // ----- introspection
    const std::vector<ExecReport>& dropCopy() const { return dropCopy_; }
    size_t resting() const { return resting_.size(); }
    uint64_t fills() const { return fills_; }
    uint64_t acks() const { return acks_; }
    uint64_t rejects() const { return rejects_; }
    uint64_t delayed() const { uint64_t n = 0; for (const auto& s : sessions_) n += s.delayed; return n; }
    const BookLite& book() const { return book_; }

private:
    struct Session { bool up = true; int64_t windowTs = 0; uint32_t used = 0, openOrders = 0; uint64_t received = 0, delayed = 0; };
    // Ordered by arrival, then by the sequence the order was sent in. The tiebreak is not a
    // nicety: a priority queue is not stable, so two children released at the same instant come out
    // in whatever order the heap happens to hold them, and that order differs between standard
    // libraries. One child per parent never produced a tie; a router that sweeps does, on every
    // multi-venue plan. Sending order is also the right answer - a venue receives what a session
    // sent it in the order it was sent.
    struct Arrival {
        ChildOrder child; int64_t arriveTs; uint64_t childSeq;
        bool operator>(const Arrival& o) const {
            if (arriveTs != o.arriveTs) return arriveTs > o.arriveTs;
            return childSeq > o.childSeq;
        }
    };
    struct Rest {
        uint64_t childId, parentId; uint32_t accountIdx, symbolIdx; Side side; int64_t price, leaves, cum, queueAhead; uint16_t sessionIdx; uint8_t tif; uint64_t childSeq;
        BookSide bookSide() const { return side == Side::Buy ? BookSide::Bid : BookSide::Ask; }
    };
    struct PendingCancel { uint64_t childId; int64_t dueTs; };

    void ack(const ChildOrder& c, uint64_t childSeq, int64_t now, const Emit& emit) {
        Frame<VenueAck> a; a.init(); a.header.sourceId = p_.sourceId; a.header.causeSeq = childSeq; a.body.childOrderId = c.childOrderId; a.body.venueId = p_.venueId;
        std::snprintf(a.body.venueOrderId, sizeof a.body.venueOrderId, "SIM%016llx", (unsigned long long)c.childOrderId); a.body.venueTs = now;
        ++acks_; emit(&a.header);
    }
    void rejectChild(const ChildOrder& c, uint32_t why, int64_t now, const Emit& emit, uint64_t childSeq = 0) {
        Frame<VenueReject> r; r.init(); r.header.sourceId = p_.sourceId; r.header.causeSeq = childSeq; r.body.childOrderId = c.childOrderId; r.body.venueId = p_.venueId;
        r.body.reason = uint16_t(why == 1 ? Reason::OrderNotLive : Reason::QtyZero); r.body.venueReason = why; r.body.venueTs = now;
        ++rejects_; emit(&r.header);
    }
    void rejectChild(const ChildOrder& c, uint32_t why, int64_t now) { deferredRejects_.push_back({c, why, now}); }
    void exec(const Rest& r, ExecType type, int64_t qty, int64_t px, int64_t now, const Emit& emit, uint16_t reason = 0) {
        Frame<ExecReport> e; e.init(); e.header.sourceId = p_.sourceId; e.header.causeSeq = r.childSeq;
        ExecReport& b = e.body;
        b.orderId = r.parentId; b.childOrderId = r.childId; b.accountIdx = r.accountIdx; b.symbolIdx = r.symbolIdx;
        b.execType = type; b.side = r.side; b.venueId = p_.venueId; b.lastQty = qty; b.lastPx = px;
        b.leavesQty = r.leaves; b.cumQty = r.cum; b.liquidityFlag = type == ExecType::Cancelled ? Liquidity::Unset : Liquidity::Removed;
        b.ordStatus = type == ExecType::Fill ? OrdStatus::Filled : type == ExecType::PartialFill ? OrdStatus::PartiallyFilled : OrdStatus::Cancelled;
        b.execId = (uint64_t(p_.sourceId) << 48) | ++execCounter_; b.rejectReason = reason; b.venueTs = now;
        std::snprintf(b.venueExecId, sizeof b.venueExecId, "SIM%013llu", (unsigned long long)execCounter_);
        dropCopy_.push_back(b);
        if (type != ExecType::Cancelled) ++fills_;
        emit(&e.header);
    }
    void execCancelled(const Rest& r, int64_t now, const Emit& emit, uint16_t reason) { exec(r, ExecType::Cancelled, 0, 0, now, emit, reason); }
    void status(uint16_t idx, int64_t now, const Emit& emit) {
        const Session& s = sessions_[idx];
        Frame<VenueSessionStatus> st; st.init(); st.header.sourceId = p_.sourceId;
        st.body.venueId = p_.venueId; st.body.sessionIdx = idx; st.body.state = s.up ? SessionState::Up : SessionState::Down;
        st.body.throttleUsedBps = uint32_t(uint64_t(s.used) * 10000 / (p_.throttlePerSec ? p_.throttlePerSec : 1));
        st.body.openOrders = s.openOrders; st.body.queueDepth = uint32_t(arrivals_.size()); st.body.ackLatencyNs = p_.latencyNs; st.body.lastVenueSeq = execCounter_;
        (void)now; emit(&st.header);
    }
    void match(const ChildOrder& c, uint64_t childSeq, Session& s, int64_t now, const Emit& emit) {
        Rest r{c.childOrderId, c.parentOrderId, c.accountIdx, c.symbolIdx, c.side, c.price, c.qty, 0, 0, c.venueSessionIdx, uint8_t(c.tif), childSeq};
        bool buy = c.side == Side::Buy;
        auto& opp = buy ? book_.asks(c.symbolIdx) : book_.bids(c.symbolIdx);
        auto crosses = [&](int64_t lvl) { return c.ordType == OrdType::Market || (buy ? c.price >= lvl : c.price <= lvl); };
        while (r.leaves > 0 && !opp.empty()) {
            auto it = buy ? opp.begin() : std::prev(opp.end());
            if (!crosses(it->first)) break;
            int64_t q = std::min(r.leaves, it->second);
            r.leaves -= q; r.cum += q; it->second -= q;
            exec(r, r.leaves == 0 ? ExecType::Fill : ExecType::PartialFill, q, it->first, now, emit);
            if (it->second == 0) opp.erase(it);
        }
        if (r.leaves == 0) { --s.openOrders; return; }
        if (c.tif == Tif::Ioc || c.tif == Tif::Fok || c.ordType == OrdType::Market) { --s.openOrders; execCancelled(r, now, emit, 0); return; }
        r.queueAhead = book_.qtyAt(c.symbolIdx, r.bookSide(), c.price);
        resting_.push_back(r);
    }
    void tradeFills(const Trade& t, int64_t now, const Emit& emit) {
        for (size_t i = 0; i < resting_.size();) {
            Rest& r = resting_[i];
            if (r.symbolIdx != t.symbolIdx || r.price != t.price) { ++i; continue; }
            int64_t q = t.qty;
            if (r.queueAhead > 0) { int64_t k = std::min(q, r.queueAhead); r.queueAhead -= k; q -= k; }
            if (q <= 0) { ++i; continue; }
            int64_t fill = std::min(q, r.leaves);
            r.leaves -= fill; r.cum += fill;
            exec(r, r.leaves == 0 ? ExecType::Fill : ExecType::PartialFill, fill, t.price, now, emit);
            if (r.leaves == 0) { --sessions_[r.sessionIdx].openOrders; resting_[i] = resting_.back(); resting_.pop_back(); }
            else ++i;
        }
    }
    void cancelChild(uint64_t childId, int64_t now, const Emit& emit, uint16_t reason) {
        for (size_t i = 0; i < resting_.size(); ++i) if (resting_[i].childId == childId) {
            Rest r = resting_[i]; resting_[i] = resting_.back(); resting_.pop_back();
            --sessions_[r.sessionIdx].openOrders; execCancelled(r, now, emit, reason); return;
        }
        // not resting: either still in flight (arrives later and is then cancelled) or already done
        pendingCancelIds_.push_back(childId);
    }
public:
    // deferred rejects (from onCore, which has no emit) and in-flight cancels are flushed here
    void flush(int64_t now, const Emit& emit) {
        for (auto& d : deferredRejects_) rejectChild(d.child, d.why, now, emit);
        deferredRejects_.clear();
        if (!pendingCancelIds_.empty()) {
            std::vector<uint64_t> again;
            for (uint64_t id : pendingCancelIds_) {
                bool found = false;
                for (size_t i = 0; i < resting_.size(); ++i) if (resting_[i].childId == id) {
                    Rest r = resting_[i]; resting_[i] = resting_.back(); resting_.pop_back();
                    --sessions_[r.sessionIdx].openOrders; execCancelled(r, now, emit, 0); found = true; break;
                }
                if (!found && inFlight(id)) again.push_back(id);
            }
            pendingCancelIds_.swap(again);
        }
    }
private:
    bool inFlight(uint64_t childId) const {
        std::priority_queue<Arrival, std::vector<Arrival>, std::greater<Arrival>> copy = arrivals_;
        while (!copy.empty()) { if (copy.top().child.childOrderId == childId) return true; copy.pop(); }
        return false;
    }
    struct DeferredReject { ChildOrder child; uint32_t why; int64_t now; };

    Params p_;
    BookLite book_;
    std::vector<Session> sessions_;
    std::priority_queue<Arrival, std::vector<Arrival>, std::greater<Arrival>> arrivals_;
    std::vector<Rest> resting_;
    std::vector<Trade> pendingTrades_;
    std::vector<PendingCancel> cancels_;
    std::vector<uint64_t> pendingCancelIds_;
    std::vector<DeferredReject> deferredRejects_;
    std::vector<ExecReport> dropCopy_;
    uint64_t execCounter_ = 0, fills_ = 0, acks_ = 0, rejects_ = 0; int64_t lastStatusTs_ = 0;
};

} // namespace trading::sim
