// core/oms/oms.hpp : the order manager.
//
// Owns the lifecycle of every parent order and every child the router spawns for it,
// aggregates child fills to the parent, and produces the client-facing ExecReport for every
// event with the fee (from the fee table) and the NBBO at that moment on it. It reacts only
// to sequenced frames, so a cold replay reaches the same state and regenerates every client
// report byte for byte. Transitions are a data table; an illegal one is counted and ignored,
// never guessed. No allocation after construction: an arena of order slots, a free list, and
// open-addressing tables sized at start.
//
// Conventions on the stream:
//   venue-level ExecReport   childOrderId != 0, written by a venue gateway; the OMS's input
//   client-level ExecReport  childOrderId == 0, written here; what the risk engine and clients consume
//   CancelOrder from the OMS orderId = childOrderId, addressed to a venue gateway
//   OrderState with reason RerouteRequired: the parent has leaves and no live child; the router must plan
//   RouteDecision           the router's answer to an accept or a re-route request; childCount 0 means it declined
//
// A parent that has been accepted (or asked for a re-route) is "unrouted" until the router answers.
// A cancel or replace on an unrouted parent waits: the router's child, when it arrives, is cancelled
// at once; a decision with no child completes the cancel. Assuming "no child yet" means "no child
// coming" would orphan a child that is already on its way to a venue.
#pragma once
#include "trading.hpp"
#include "snapshot.hpp"
#include "flatmap.hpp"
#include "blake3.h"
#include <functional>
#include <vector>
#include <array>
#include <cstring>

namespace trading::oms {

using trading::util::FlatMap;
using trading::util::Key128;

enum class Event : uint8_t { RiskAccept, RiskReject, ChildAck, ChildReject, PartialFill, Fill, CancelRequest, CancelAck,
                             ReplaceRequest, ReplaceAck, ReplaceReject, Expire, Kill, Bust, Reroute, COUNT };
inline const char* to_string(Event e) noexcept {
    static const char* n[] = {"RiskAccept", "RiskReject", "ChildAck", "ChildReject", "PartialFill", "Fill", "CancelRequest", "CancelAck",
                              "ReplaceRequest", "ReplaceAck", "ReplaceReject", "Expire", "Kill", "Bust", "Reroute"};
    return n[uint8_t(e)];
}

// The parent state machine as data. Unset means illegal. Rows: OrdStatus (1..9), columns: Event.
inline constexpr size_t STATUS_COUNT = 10;
using Row = std::array<OrdStatus, size_t(Event::COUNT)>;
constexpr OrdStatus U = OrdStatus::Unset, PN = OrdStatus::PendingNew, L = OrdStatus::Live, PF = OrdStatus::PartiallyFilled, F = OrdStatus::Filled,
                    C = OrdStatus::Cancelled, R = OrdStatus::Rejected, X = OrdStatus::Expired, PC = OrdStatus::PendingCancel, PR = OrdStatus::PendingReplace;
inline constexpr std::array<Row, STATUS_COUNT> TABLE = {{
    //          RiskAcc RiskRej ChildAck ChildRej PartFill Fill  CancelReq CancelAck ReplReq ReplAck ReplRej Expire Kill  Bust  Reroute
    /*Unset*/  {{U,     U,      U,       U,       U,       U,    U,        U,        U,      U,      U,      U,     U,    U,    U}},
    /*PN*/     {{L,     R,      U,       U,       U,       U,    U,        U,        U,      U,      U,      X,     R,    U,    U}},
    /*Live*/   {{U,     U,      L,       L,       PF,      F,    PC,       C,        PR,     U,      U,      X,     PC,   U,    L}},
    /*PF*/     {{U,     U,      PF,      PF,      PF,      F,    PC,       C,        PR,     U,      U,      X,     PC,   PF,   PF}},
    /*Filled*/ {{U,     U,      U,       U,       U,       U,    U,        U,        U,      U,      U,      U,     U,    PF,   U}},
    /*Cancel*/ {{U,     U,      U,       U,       U,       U,    U,        U,        U,      U,      U,      U,     U,    U,    U}},
    /*Reject*/ {{U,     U,      U,       U,       U,       U,    U,        U,        U,      U,      U,      U,     U,    U,    U}},
    /*Expired*/{{U,     U,      U,       U,       U,       U,    U,        U,        U,      U,      U,      U,     U,    U,    U}},
    /*PC*/     {{U,     U,      PC,      PC,      PC,      F,    U,        C,        U,      U,      U,      U,     PC,   U,    U}},
    /*PR*/     {{U,     U,      PR,      PR,      PR,      F,    PC,       PR,       U,      L,      L,      X,     PC,   U,    U}},
}};
inline constexpr OrdStatus next(OrdStatus s, Event e) noexcept { return TABLE[uint8_t(s)][uint8_t(e)]; }
inline constexpr bool terminal(OrdStatus s) noexcept { return s == F || s == C || s == R || s == X; }

struct alignas(64) Order {                       // one slot; parents and children share the arena
    uint64_t orderId, clOrdId, clientTag, locateId;
    uint64_t parentSlotSeq;                      // parent's NewOrder seq (for cause chains), 0 for children
    uint32_t accountIdx, symbolIdx, sessionId, self, parent, firstChild, nextSibling;
    uint16_t routingProfile, strategyId, venueId, venueSessionIdx, reason, pad0;
    Side side; OrdType ordType; Tif tif; uint8_t orderFlags; OrdStatus status, prevStatus; uint8_t reroutes, liveChildren, isChild, unrouted, pad1[2];
    int64_t qty, leaves, cum, cumNotional, price, stopPrice, displayQty, minQty;
    int64_t pendQty, pendPrice;                  // replace in flight
    uint64_t pendClOrdId;                        // clOrdId of the cancel or replace in flight (echoed on its report)
};
static_assert(sizeof(Order) <= 256, "order slot must fit four cache lines");

class Oms {
public:
    using Emit = std::function<void(FrameHeader*)>;
    static constexpr uint8_t MAX_REROUTES = 3;

    explicit Oms(uint16_t sourceId = 4, size_t capacity = 1 << 20)
        : src_(sourceId), slots_(capacity), byId_(capacity), byClOrd_(capacity), fees_(256), nbbo_(refdata::MAX_SYMBOLS, {0, 0}) {
        free_.reserve(capacity); for (uint32_t i = uint32_t(capacity - 1); i >= 1; --i) free_.push_back(i);
    }
    // Start of day: the fee schedule in force.
    void load(const refdata::Snapshot& snap) { for (const FeeRecord& f : snap.fees()) putFee(f); }

    // Market data the OMS needs on reports.
    void onNbbo(const Nbbo& n) noexcept { if (n.symbolIdx < nbbo_.size()) nbbo_[n.symbolIdx] = {n.bid, n.ask}; }

    // Every core-stream frame, in sequence order. Emits through `emit` (Live) or whatever the caller passes.
    void apply(const FrameHeader* f, const Emit& emit) {
        cur_ = f;
        if (auto* o = as<NewOrder>(f)) onNew(*o, f, emit);
        else if (auto* d = as<RiskDecision>(f)) onRisk(*d, f, emit);
        else if (auto* c = as<ChildOrder>(f)) onChild(*c, f, emit);
        else if (auto* rt = as<RouteDecision>(f)) onRoute(*rt, f, emit);
        else if (auto* a = as<VenueAck>(f)) onAck(*a, emit);
        else if (auto* r = as<VenueReject>(f)) onVenueReject(*r, f, emit);
        else if (auto* e = as<ExecReport>(f)) { if (e->childOrderId != 0) onVenueExec(*e, f, emit); }
        else if (auto* c = as<CancelOrder>(f)) { if (f->sourceId != src_) onCancel(*c, f, emit); }
        else if (auto* r = as<ReplaceOrder>(f)) onReplace(*r, f, emit);
        else if (auto* k = as<KillSwitch>(f)) { if (k->release == 0) onKill(*k, f, emit); }
        else if (auto* fs = as<FeeScheduleUpdate>(f)) putFee(fs->record);
        else if (auto* n = as<Nbbo>(f)) onNbbo(*n);
    }

    // ----- introspection
    const Order* find(uint64_t orderId) const noexcept { const uint32_t* s = byId_.find(orderId); return s ? &slots_[*s] : nullptr; }
    uint64_t orders() const noexcept { return orders_; } uint64_t accepted() const noexcept { return accepted_; } uint64_t rejected() const noexcept { return rejected_; }
    uint64_t fills() const noexcept { return fills_; } uint64_t cancelled() const noexcept { return cancelled_; } uint64_t venueRejected() const noexcept { return venueRejected_; }
    uint64_t cancelsSent() const noexcept { return cancelsSent_; } uint64_t cancelRejected() const noexcept { return cancelRejected_; }
    uint64_t reroutes() const noexcept { return reroutes_; } uint64_t replaced() const noexcept { return replaced_; } uint64_t illegal() const noexcept { return illegal_; }
    size_t live() const noexcept { return size_t(slots_.size() - 1 - free_.size()); }

    // Order-independent state hash (sums and histograms), so it does not depend on table iteration order.
    void hashInto(blake3_hasher& h) const {
        uint64_t n = 0; int64_t leaves = 0, cum = 0, notional = 0; std::array<uint64_t, 16> hist{}; uint64_t kids = 0;
        byId_.forEach([&](uint64_t, const uint32_t& s) { const Order& o = slots_[s]; if (o.isChild) { ++kids; return; } ++n; leaves += o.leaves; cum += o.cum; notional += o.cumNotional; ++hist[uint8_t(o.status) & 15]; });
        blake3_hasher_update(&h, &n, 8); blake3_hasher_update(&h, &kids, 8); blake3_hasher_update(&h, &leaves, 8); blake3_hasher_update(&h, &cum, 8);
        blake3_hasher_update(&h, &notional, 8); blake3_hasher_update(&h, hist.data(), hist.size() * 8);
        blake3_hasher_update(&h, &fills_, 8); blake3_hasher_update(&h, &cancelled_, 8); blake3_hasher_update(&h, &rejected_, 8); blake3_hasher_update(&h, &reroutes_, 8);
        blake3_hasher_update(&h, &illegal_, 8);
    }

private:
    struct Fee { int64_t perShare, cap; uint16_t code; };
    static uint64_t feeKey(uint16_t venue, Liquidity liq) noexcept { return (uint64_t(venue) << 8) | uint8_t(liq); }
    void putFee(const FeeRecord& f) { fees_.insert(feeKey(f.venueId, Liquidity(f.liquidity)), Fee{f.feePerShare, f.capPerOrder, f.feeCode}); }
    static Key128 clKey(uint32_t acct, uint32_t session, uint64_t clOrdId) noexcept { return Key128{(uint64_t(acct) << 32) | session, clOrdId}; }

    // ----- transitions
    bool transition(Order& o, Event e) noexcept {
        OrdStatus n = next(o.status, e);
        if (n == U) { ++illegal_; return false; }
        o.prevStatus = o.status; o.status = n; return true;
    }
    Order* alloc() noexcept { if (free_.empty()) return nullptr; uint32_t s = free_.back(); free_.pop_back(); Order& o = slots_[s]; std::memset(&o, 0, sizeof o); o.self = s; return &o; }
    void release(Order& o) noexcept {                       // a terminal parent and its children leave the tables at once
        for (uint32_t c = o.firstChild; c;) { uint32_t nx = slots_[c].nextSibling; byId_.erase(slots_[c].orderId); free_.push_back(c); c = nx; }
        byId_.erase(o.orderId); byClOrd_.erase(clKey(o.accountIdx, o.sessionId, o.clOrdId)); free_.push_back(o.self);
    }
    Order* parentOf(uint64_t childId) noexcept { const uint32_t* s = byId_.find(childId); if (!s) return nullptr; Order& c = slots_[*s]; return c.isChild ? &slots_[c.parent] : nullptr; }
    Order* childSlot(uint64_t childId) noexcept { const uint32_t* s = byId_.find(childId); return s && slots_[*s].isChild ? &slots_[*s] : nullptr; }

    // ----- inputs
    void onNew(const NewOrder& n, const FrameHeader* f, const Emit& emit) {
        ++orders_;
        if (byId_.find(n.orderId)) { ++illegal_; return; }
        Order* o = alloc();
        if (!o) { rejectNew(n, f, Reason::MaxOpenOrders, emit); return; }
        o->orderId = n.orderId; o->clOrdId = n.clOrdId; o->clientTag = n.clientTag; o->locateId = n.locateId; o->parentSlotSeq = f->seq;
        o->accountIdx = n.accountIdx; o->symbolIdx = n.symbolIdx; o->sessionId = n.sessionId; o->routingProfile = n.routingProfile; o->strategyId = n.strategyId;
        o->side = n.side; o->ordType = n.ordType; o->tif = n.tif; o->orderFlags = n.orderFlags; o->status = PN;
        o->qty = n.qty; o->leaves = n.qty; o->price = n.price; o->stopPrice = n.stopPrice; o->displayQty = n.displayQty; o->minQty = n.minQty;
        byId_.insert(n.orderId, o->self); byClOrd_.insert(clKey(n.accountIdx, n.sessionId, n.clOrdId), o->self);
    }
    void rejectNew(const NewOrder& n, const FrameHeader* f, Reason r, const Emit& emit) {
        Frame<ExecReport> e; e.init(); e.header.sourceId = src_; e.header.causeSeq = f->seq; ExecReport& b = e.body;
        b.orderId = n.orderId; b.clOrdId = n.clOrdId; b.accountIdx = n.accountIdx; b.symbolIdx = n.symbolIdx; b.side = n.side; b.execType = ExecType::Rejected;
        b.ordStatus = R; b.rejectReason = uint16_t(r); b.clientTag = n.clientTag; b.leavesQty = 0; b.execId = nextExecId(); ++rejected_; emit(&e.header);
    }
    void onRisk(const RiskDecision& d, const FrameHeader* f, const Emit& emit) {
        const uint32_t* s = byId_.find(d.orderId); if (!s) return;
        Order& o = slots_[*s]; if (o.isChild) return;
        if (o.status == PR) {                                   // the decision on a replace
            if (d.verdict == RiskVerdict::Accept) {
                int64_t newQty = o.pendQty ? o.pendQty : o.qty; o.qty = newQty; o.leaves = newQty - o.cum; if (o.pendPrice) o.price = o.pendPrice;
                ++replaced_; report(o, ExecType::Replaced, 0, 0, Liquidity::Unset, 0, f, emit, 0, o.pendClOrdId);
                if (o.liveChildren == 0 && !o.unrouted) finishReplace(o, f, emit); else cancelChildren(o, f, emit);   // children are cancelled and re-planned at the new shape; an unrouted parent waits for its child
            } else {
                OrdStatus before = o.prevStatus; o.prevStatus = PR; o.status = before == U ? L : before;   // back to where it was
                cancelReject(o, f, Reason(d.reason), emit, o.pendClOrdId, ExecType::Rejected);
            }
            o.pendQty = o.pendPrice = 0; o.pendClOrdId = 0; return;
        }
        if (o.status != PN) return;
        if (d.verdict == RiskVerdict::Accept) { transition(o, Event::RiskAccept); o.unrouted = 1; ++accepted_; report(o, ExecType::New, 0, 0, Liquidity::Unset, 0, f, emit); state(o, f, emit, 0); }
        else { transition(o, Event::RiskReject); ++rejected_; report(o, ExecType::Rejected, 0, 0, Liquidity::Unset, 0, f, emit, d.reason); release(o); }
    }
    void onRoute(const RouteDecision& rt, const FrameHeader* f, const Emit& emit) {
        const uint32_t* ps = byId_.find(rt.parentOrderId); if (!ps) return;
        Order& p = slots_[*ps]; if (p.isChild || terminal(p.status)) return;
        p.unrouted = 0;
        if (rt.childCount == 0 && p.liveChildren == 0) {           // the router declined: a cancel in flight completes, otherwise the client is told
            if (p.status == PC) { transition(p, Event::CancelAck); ++cancelled_; report(p, ExecType::Cancelled, 0, 0, Liquidity::Unset, 0, f, emit, 0, p.pendClOrdId); state(p, f, emit, 0); release(p); return; }
            if (p.status == PR) { finishReplace(p, f, emit); return; }
            if (p.cum == 0) { p.status = R; ++rejected_; report(p, ExecType::Rejected, 0, 0, Liquidity::Unset, 0, f, emit, rt.reason); }
            else { p.status = C; ++cancelled_; report(p, ExecType::Cancelled, 0, 0, Liquidity::Unset, 0, f, emit, rt.reason); }
            state(p, f, emit, rt.reason); release(p);
        }
    }
    void onChild(const ChildOrder& c, const FrameHeader* f, const Emit& emit) {
        const uint32_t* ps = byId_.find(c.parentOrderId); if (!ps) { ++illegal_; return; }
        Order& p = slots_[*ps]; if (p.isChild || terminal(p.status)) { ++illegal_; return; }
        Order* ch = alloc(); if (!ch) { ++illegal_; return; }
        p.unrouted = 0;
        ch->isChild = 1; ch->orderId = c.childOrderId; ch->parent = p.self; ch->accountIdx = c.accountIdx; ch->symbolIdx = c.symbolIdx;
        ch->venueId = c.venueId; ch->venueSessionIdx = c.venueSessionIdx; ch->side = c.side; ch->ordType = c.ordType; ch->tif = c.tif;
        ch->qty = c.qty; ch->leaves = c.qty; ch->price = c.price; ch->status = PN; ch->parentSlotSeq = f->seq;
        ch->nextSibling = p.firstChild; p.firstChild = ch->self; ++p.liveChildren;
        byId_.insert(c.childOrderId, ch->self);
        if (p.status == PC || p.status == PR) cancelChild(p, *ch, f, emit);   // a cancel or replace was waiting for exactly this child
    }
    void onAck(const VenueAck& a, const Emit&) { Order* ch = childSlot(a.childOrderId); if (!ch || ch->status != PN) return; ch->status = L; Order& p = slots_[ch->parent]; transition(p, Event::ChildAck); }
    void onVenueReject(const VenueReject& r, const FrameHeader* f, const Emit& emit) {
        Order* ch = childSlot(r.childOrderId); if (!ch || terminal(ch->status)) return;
        ch->status = R; Order& p = slots_[ch->parent]; if (p.liveChildren) --p.liveChildren; ++venueRejected_;
        if (!transition(p, Event::ChildReject)) return;
        childEnded(p, f, emit, r.reason, true);
    }
    void onVenueExec(const ExecReport& e, const FrameHeader* f, const Emit& emit) {
        Order* ch = childSlot(e.childOrderId); if (!ch) return;
        Order& p = slots_[ch->parent];
        if (e.execType == ExecType::PartialFill || e.execType == ExecType::Fill) {
            // every execution the venue reports is booked, even on a child already counted done (an
            // over-fill after a replace-down, a late fill racing a cancel): it happened, the client owns it
            if (terminal(p.status)) return;
            ch->cum += e.lastQty; ch->leaves -= e.lastQty; p.cum += e.lastQty; p.leaves -= e.lastQty; p.cumNotional += e.lastQty * e.lastPx; ++fills_;
            if ((e.execType == ExecType::Fill || ch->leaves <= 0) && !terminal(ch->status)) { ch->status = F; if (p.liveChildren) --p.liveChildren; }
            bool parentDone = p.leaves <= 0 && p.liveChildren == 0;   // done only once no child can still execute
            if (!transition(p, parentDone ? Event::Fill : Event::PartialFill)) return;
            Fee fee = feeFor(e.venueId, e.liquidityFlag);
            int64_t amount = e.lastQty * fee.perShare; if (fee.cap && amount > fee.cap) amount = fee.cap;
            report(p, parentDone ? ExecType::Fill : ExecType::PartialFill, e.lastQty, e.lastPx, e.liquidityFlag, amount, f, emit, 0, 0, fee.code, e.venueId, e.venueExecId, e.venueTs);
            if (parentDone) { state(p, f, emit, 0); release(p); }
        } else if (e.execType == ExecType::Cancelled) {
            if (terminal(ch->status)) return;
            ch->status = C; if (p.liveChildren) --p.liveChildren;
            childEnded(p, f, emit, e.rejectReason, false);
        } else if (e.execType == ExecType::TradeBust) {
            // a bust on a still-open parent reverses the fill; a bust after the parent was released is
            // a post-trade matter for the position keeper and is not applied here
            if (e.lastQty <= 0 || terminal(p.status)) return;
            if (!transition(p, Event::Bust)) return;
            ch->cum -= e.lastQty; ch->leaves += e.lastQty; p.cum -= e.lastQty; p.leaves += e.lastQty; p.cumNotional -= e.lastQty * e.lastPx;
            if (ch->status == F) { ch->status = L; ++p.liveChildren; }
            report(p, ExecType::TradeBust, e.lastQty, e.lastPx, e.liquidityFlag, 0, f, emit, 0, 0, 0, e.venueId, e.venueExecId, e.venueTs);
        }
    }
    // A child ended (venue reject or venue cancel). Complete a client cancel or replace if one is in
    // flight; otherwise re-plan the remainder, or give up when the re-route budget is spent.
    void childEnded(Order& p, const FrameHeader* f, const Emit& emit, uint16_t reason, bool wasReject) {
        if (p.liveChildren > 0) return;
        if (p.leaves <= 0) {                                    // filled while a cancel or replace was in flight
            transition(p, Event::Fill); report(p, ExecType::Fill, 0, 0, Liquidity::Unset, 0, f, emit); state(p, f, emit, 0); release(p); return;
        }
        if (p.status == PC) { transition(p, Event::CancelAck); ++cancelled_; report(p, ExecType::Cancelled, 0, 0, Liquidity::Unset, 0, f, emit, 0, p.pendClOrdId); state(p, f, emit, 0); release(p); return; }
        if (p.status == PR) { finishReplace(p, f, emit); return; }
        bool ioc = p.tif == Tif::Ioc || p.tif == Tif::Fok;
        if (!ioc && p.reroutes < MAX_REROUTES) { ++p.reroutes; ++reroutes_; transition(p, Event::Reroute); p.unrouted = 1; state(p, f, emit, uint16_t(Reason::RerouteRequired)); return; }
        if (wasReject && p.cum == 0) { p.status = R; ++rejected_; report(p, ExecType::Rejected, 0, 0, Liquidity::Unset, 0, f, emit, reason); }
        else { p.status = C; ++cancelled_; report(p, ExecType::Cancelled, 0, 0, Liquidity::Unset, 0, f, emit, reason); }
        state(p, f, emit, reason); release(p);
    }
    void finishReplace(Order& p, const FrameHeader* f, const Emit& emit) {
        transition(p, Event::ReplaceAck); p.reroutes = 0;
        if (p.leaves <= 0) { p.status = F; state(p, f, emit, 0); release(p); return; }
        p.unrouted = 1; state(p, f, emit, uint16_t(Reason::RerouteRequired));
    }
    void cancelChild(const Order& p, const Order& ch, const FrameHeader* f, const Emit& emit) {
        Frame<CancelOrder> k; k.init(); k.header.sourceId = src_; k.header.causeSeq = f->seq;
        k.body.orderId = ch.orderId; k.body.origClOrdId = p.clOrdId; k.body.clOrdId = p.pendClOrdId; k.body.accountIdx = p.accountIdx; k.body.sessionId = p.sessionId;
        ++cancelsSent_; emit(&k.header);
    }
    void cancelChildren(Order& p, const FrameHeader* f, const Emit& emit) {
        for (uint32_t c = p.firstChild; c; c = slots_[c].nextSibling) { const Order& ch = slots_[c]; if (!terminal(ch.status)) cancelChild(p, ch, f, emit); }
    }
    void onCancel(const CancelOrder& c, const FrameHeader* f, const Emit& emit) {
        Order* o = nullptr;
        if (const uint32_t* s = byId_.find(c.orderId)) o = &slots_[*s];
        else if (const uint32_t* s2 = byClOrd_.find(clKey(c.accountIdx, c.sessionId, c.origClOrdId))) o = &slots_[*s2];
        if (!o || o->isChild) { cancelRejectUnknown(c, f, emit); return; }
        if (!transition(*o, Event::CancelRequest)) { cancelReject(*o, f, Reason::OrderNotLive, emit, c.clOrdId, ExecType::Rejected); return; }
        o->pendClOrdId = c.clOrdId;
        if (o->liveChildren == 0 && !o->unrouted) { transition(*o, Event::CancelAck); ++cancelled_; report(*o, ExecType::Cancelled, 0, 0, Liquidity::Unset, 0, f, emit, 0, c.clOrdId); state(*o, f, emit, 0); release(*o); return; }
        state(*o, f, emit, 0); cancelChildren(*o, f, emit);           // an unrouted parent waits here for the router's child
    }
    void onReplace(const ReplaceOrder& r, const FrameHeader* f, const Emit& emit) {
        Order* o = nullptr;
        if (const uint32_t* s = byId_.find(r.orderId)) o = &slots_[*s];
        else if (const uint32_t* s2 = byClOrd_.find(clKey(r.accountIdx, r.sessionId, r.origClOrdId))) o = &slots_[*s2];
        if (!o || o->isChild) { ++cancelRejected_; Frame<ExecReport> e; e.init(); e.header.sourceId = src_; e.header.causeSeq = f->seq; e.body.orderId = r.orderId; e.body.clOrdId = r.clOrdId; e.body.accountIdx = r.accountIdx; e.body.execType = ExecType::Rejected; e.body.ordStatus = R; e.body.rejectReason = uint16_t(Reason::UnknownOrder); e.body.execId = nextExecId(); emit(&e.header); return; }
        if (!transition(*o, Event::ReplaceRequest)) { cancelReject(*o, f, Reason::OrderNotLive, emit, r.clOrdId, ExecType::Rejected); return; }
        o->pendQty = r.qty; o->pendPrice = r.price; o->pendClOrdId = r.clOrdId;
        state(*o, f, emit, 0);                                  // the risk engine's decision on the replace follows
    }
    void onKill(const KillSwitch& k, const FrameHeader* f, const Emit& emit) {
        byId_.forEach([&](uint64_t, const uint32_t& s) {
            Order& o = slots_[s];
            if (o.isChild || terminal(o.status)) return;
            if (k.accountIdx != 0 && o.accountIdx != k.accountIdx) return;
            if (k.strategyId != 0 && o.strategyId != k.strategyId) return;
            if (!transition(o, Event::Kill)) return;
            if (o.status == R) { ++rejected_; report(o, ExecType::Rejected, 0, 0, Liquidity::Unset, 0, f, emit, uint16_t(Reason::FirmKilled)); pendingRelease_.push_back(o.self); return; }
            o.pendClOrdId = 0; state(o, f, emit, uint16_t(Reason::FirmKilled));
            if (o.liveChildren == 0 && !o.unrouted) { transition(o, Event::CancelAck); ++cancelled_; report(o, ExecType::Cancelled, 0, 0, Liquidity::Unset, 0, f, emit, uint16_t(Reason::FirmKilled)); pendingRelease_.push_back(o.self); }
            else cancelChildren(o, f, emit);
        });
        for (uint32_t s : pendingRelease_) release(slots_[s]);   // not while iterating the table
        pendingRelease_.clear();
    }

    // ----- outputs
    uint64_t nextExecId() noexcept { return (uint64_t(src_) << 48) | ++execCounter_; }
    Fee feeFor(uint16_t venue, Liquidity liq) const noexcept { const Fee* f = fees_.find(feeKey(venue, liq)); return f ? *f : Fee{0, 0, 0}; }
    void report(const Order& o, ExecType type, int64_t lastQty, int64_t lastPx, Liquidity liq, int64_t fee, const FrameHeader* cause, const Emit& emit,
                uint16_t reason = 0, uint64_t clOrdOverride = 0, uint16_t feeCode = 0, uint16_t venueId = 0, const char* venueExecId = nullptr, int64_t venueTs = 0) {
        Frame<ExecReport> e; e.init(); e.header.sourceId = src_; e.header.causeSeq = cause->seq;
        ExecReport& b = e.body;
        b.orderId = o.orderId; b.clOrdId = clOrdOverride ? clOrdOverride : o.clOrdId; b.execId = nextExecId(); b.childOrderId = 0;
        b.accountIdx = o.accountIdx; b.symbolIdx = o.symbolIdx; b.execType = type; b.ordStatus = o.status; b.side = o.side; b.liquidityFlag = liq;
        b.venueId = venueId; b.feeCode = feeCode; b.lastQty = lastQty; b.lastPx = lastPx; b.leavesQty = o.leaves; b.cumQty = o.cum;
        b.avgPx = o.cum ? o.cumNotional / o.cum : 0; b.fee = fee;
        if (o.symbolIdx < nbbo_.size()) { b.nbboBid = nbbo_[o.symbolIdx].first; b.nbboAsk = nbbo_[o.symbolIdx].second; }
        if (venueExecId) std::memcpy(b.venueExecId, venueExecId, sizeof b.venueExecId);
        b.rejectReason = reason; b.venueTs = venueTs; b.clientTag = o.clientTag;
        emit(&e.header);
    }
    void state(const Order& o, const FrameHeader* cause, const Emit& emit, uint16_t reason) {
        Frame<OrderState> s; s.init(); s.header.sourceId = src_; s.header.causeSeq = cause->seq;
        s.body.orderId = o.orderId; s.body.parentOrderId = 0; s.body.status = o.status; s.body.prevStatus = o.prevStatus;
        s.body.leavesQty = o.leaves; s.body.cumQty = o.cum; s.body.avgPx = o.cum ? o.cumNotional / o.cum : 0; s.body.reason = reason;
        emit(&s.header);
    }
    void cancelReject(const Order& o, const FrameHeader* f, Reason r, const Emit& emit, uint64_t clOrdId, ExecType type) {
        ++cancelRejected_; report(o, type, 0, 0, Liquidity::Unset, 0, f, emit, uint16_t(r), clOrdId);   // ordStatus stays the order's: a cancel/replace reject, not an order reject
    }
    void cancelRejectUnknown(const CancelOrder& c, const FrameHeader* f, const Emit& emit) {
        ++cancelRejected_;
        Frame<ExecReport> e; e.init(); e.header.sourceId = src_; e.header.causeSeq = f->seq;
        e.body.orderId = c.orderId; e.body.clOrdId = c.clOrdId; e.body.accountIdx = c.accountIdx; e.body.execType = ExecType::Rejected; e.body.ordStatus = R;
        e.body.rejectReason = uint16_t(Reason::UnknownOrder); e.body.execId = nextExecId(); emit(&e.header);
    }

    uint16_t src_;
    std::vector<Order> slots_; std::vector<uint32_t> free_;
    FlatMap<uint64_t, uint32_t> byId_; FlatMap<Key128, uint32_t> byClOrd_;
    FlatMap<uint64_t, Fee> fees_;
    std::vector<std::pair<int64_t, int64_t>> nbbo_;
    std::vector<uint32_t> pendingRelease_;
    const FrameHeader* cur_ = nullptr;
    uint64_t execCounter_ = 0, orders_ = 0, accepted_ = 0, rejected_ = 0, fills_ = 0, cancelled_ = 0, venueRejected_ = 0, cancelsSent_ = 0, cancelRejected_ = 0, reroutes_ = 0, replaced_ = 0, illegal_ = 0;
};

} // namespace trading::oms
