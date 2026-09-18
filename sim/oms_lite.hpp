// sim/oms_lite.hpp : the smallest parent/child aggregation that lets the harness run an
// order from client to venue and back. It is a stand-in for core/oms (v0.3): one child
// per parent, no replace, no fee model. It reacts only to sequenced frames, so a cold
// replay reaches the same state, and it emits the client-facing (parent-level, childOrderId
// = 0) ExecReports that the risk engine consumes.
#pragma once
#include "trading.hpp"
#include "flatmap.hpp"
#include "blake3.h"
#include <functional>
#include <array>

namespace trading::sim {

class OmsLite {
public:
    using Emit = std::function<void(FrameHeader*)>;
    explicit OmsLite(uint16_t sourceId = 4, size_t expected = 1 << 16) : src_(sourceId), parents_(expected), childToParent_(expected) {}

    void apply(const FrameHeader* f, const Emit& emit) {
        if (auto* o = as<NewOrder>(f)) {
            Parent p{}; p.order = *o; p.status = OrdStatus::PendingNew; p.leaves = o->qty; p.seq = f->seq;
            parents_.insert(o->orderId, p); ++orders_;
        } else if (auto* d = as<RiskDecision>(f)) {
            Parent* p = parents_.find(d->orderId); if (!p) return;
            if (p->status != OrdStatus::PendingNew) return;                      // decisions on replaces are not ours yet
            if (d->verdict == RiskVerdict::Accept) { p->status = OrdStatus::Live; ++accepted_; }
            else { p->status = OrdStatus::Rejected; ++rejected_; report(*p, ExecType::Rejected, 0, 0, f->seq, emit, d->reason); }
        } else if (auto* c = as<ChildOrder>(f)) {
            childToParent_.insert(c->childOrderId, c->parentOrderId);
            if (Parent* p = parents_.find(c->parentOrderId)) { p->childId = c->childOrderId; p->sessionIdx = c->venueSessionIdx; }
        } else if (auto* r = as<VenueReject>(f)) {
            uint64_t* pid = childToParent_.find(r->childOrderId); if (!pid) return;
            Parent* p = parents_.find(*pid); if (!p || p->status != OrdStatus::Live) return;
            p->status = OrdStatus::Rejected; ++venueRejected_; report(*p, ExecType::Rejected, 0, 0, f->seq, emit, r->reason);
        } else if (auto* e = as<ExecReport>(f)) {
            if (e->childOrderId == 0) return;                                    // our own parent-level report
            uint64_t* pid = childToParent_.find(e->childOrderId); if (!pid) return;
            Parent* p = parents_.find(*pid); if (!p) return;
            if (e->execType == ExecType::PartialFill || e->execType == ExecType::Fill) {
                p->cum += e->lastQty; p->leaves -= e->lastQty; p->cumNotional += e->lastQty * e->lastPx; ++fills_;
                bool done = p->leaves <= 0;
                p->status = done ? OrdStatus::Filled : OrdStatus::PartiallyFilled;
                report(*p, done ? ExecType::Fill : ExecType::PartialFill, e->lastQty, e->lastPx, f->seq, emit);
            } else if (e->execType == ExecType::Cancelled) {
                if (p->status == OrdStatus::Filled || p->status == OrdStatus::Cancelled) return;
                p->status = OrdStatus::Cancelled; ++cancelled_;
                report(*p, ExecType::Cancelled, 0, 0, f->seq, emit, e->rejectReason);
            }
        } else if (auto* c = as<CancelOrder>(f)) {
            if (f->sourceId == src_) return;                                     // our own cancel to a child
            Parent* p = parents_.find(c->orderId);
            if (!p || (p->status != OrdStatus::Live && p->status != OrdStatus::PartiallyFilled) || p->childId == 0) {
                ++cancelRejected_; Frame<ExecReport> e; e.init(); e.header.sourceId = src_; e.header.causeSeq = f->seq;
                e.body.orderId = c->orderId; e.body.clOrdId = c->clOrdId; e.body.accountIdx = c->accountIdx; e.body.execType = ExecType::Rejected;
                e.body.ordStatus = p ? p->status : OrdStatus::Rejected; e.body.rejectReason = uint16_t(p ? Reason::OrderNotLive : Reason::UnknownOrder);
                emit(&e.header); return;                                          // a cancel reject never touches the parent
            }
            p->status = OrdStatus::PendingCancel; ++cancelsSent_;
            Frame<CancelOrder> k; k.init(); k.header.sourceId = src_; k.header.causeSeq = f->seq;
            k.body.orderId = p->childId; k.body.origClOrdId = p->order.clOrdId; k.body.clOrdId = c->clOrdId; k.body.accountIdx = c->accountIdx; k.body.sessionId = c->sessionId;
            emit(&k.header);
        }
    }

    // What the venue's drop copy must agree with: fills we aggregated, by execId count and quantity.
    uint64_t fills() const { return fills_; }
    uint64_t orders() const { return orders_; }
    uint64_t accepted() const { return accepted_; }
    uint64_t rejected() const { return rejected_; }
    uint64_t cancelled() const { return cancelled_; }
    uint64_t venueRejected() const { return venueRejected_; }
    uint64_t cancelsSent() const { return cancelsSent_; }
    uint64_t cancelRejected() const { return cancelRejected_; }
    const NewOrder* parent(uint64_t orderId) const { const Parent* p = parents_.find(orderId); return p ? &p->order : nullptr; }
    OrdStatus status(uint64_t orderId) const { const Parent* p = parents_.find(orderId); return p ? p->status : OrdStatus::Unset; }

    // Order-independent state hash (sums and histograms), so it does not depend on table iteration order.
    void hashInto(blake3_hasher& h) const {
        uint64_t n = parents_.size(); int64_t leaves = 0, cum = 0, notional = 0; std::array<uint64_t, 16> hist{};
        parents_.forEach([&](uint64_t, const Parent& p) { leaves += p.leaves; cum += p.cum; notional += p.cumNotional; ++hist[uint8_t(p.status) & 15]; });
        blake3_hasher_update(&h, &n, 8); blake3_hasher_update(&h, &leaves, 8); blake3_hasher_update(&h, &cum, 8);
        blake3_hasher_update(&h, &notional, 8); blake3_hasher_update(&h, hist.data(), hist.size() * 8);
        blake3_hasher_update(&h, &fills_, 8); blake3_hasher_update(&h, &cancelled_, 8); blake3_hasher_update(&h, &rejected_, 8);
    }

private:
    struct Parent { NewOrder order; OrdStatus status; int64_t leaves, cum, cumNotional; uint64_t childId, seq; uint16_t sessionIdx; };
    void report(const Parent& p, ExecType type, int64_t lastQty, int64_t lastPx, uint64_t cause, const Emit& emit, uint16_t reason = 0) {
        Frame<ExecReport> e; e.init(); e.header.sourceId = src_; e.header.causeSeq = cause;
        ExecReport& b = e.body;
        b.orderId = p.order.orderId; b.clOrdId = p.order.clOrdId; b.accountIdx = p.order.accountIdx; b.symbolIdx = p.order.symbolIdx;
        b.execType = type; b.ordStatus = p.status; b.side = p.order.side; b.lastQty = lastQty; b.lastPx = lastPx;
        b.leavesQty = p.leaves; b.cumQty = p.cum; b.avgPx = p.cum ? p.cumNotional / p.cum : 0; b.rejectReason = reason;
        b.clientTag = p.order.clientTag; b.execId = (uint64_t(src_) << 48) | ++execCounter_;
        b.liquidityFlag = lastQty ? Liquidity::Removed : Liquidity::Unset;
        emit(&e.header);
    }
    uint16_t src_;
    util::FlatMap<uint64_t, Parent> parents_;
    util::FlatMap<uint64_t, uint64_t> childToParent_;
    uint64_t execCounter_ = 0, orders_ = 0, accepted_ = 0, rejected_ = 0, fills_ = 0, cancelled_ = 0, venueRejected_ = 0, cancelsSent_ = 0, cancelRejected_ = 0;
};

} // namespace trading::sim
