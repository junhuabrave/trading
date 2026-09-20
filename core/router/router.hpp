// core/router/router.hpp : where best execution is either provable or not.
//
// The order manager hands this an accepted parent and asks for a plan. What comes back is up to
// eight children with a venue, a session, a quantity, a price and a release offset - and, just as
// importantly, the cost of the alternative that was not taken. That second number is the whole
// point. A router that only records what it did can say it routed somewhere; a router that records
// what else it could have done and what that would have cost can be asked, months later and by
// someone unsympathetic, why it chose what it chose.
//
// Three things constrain the arithmetic, in this order.
//
// Price priority is not an optimisation. A rebate can make a worse price look cheaper net, and
// taking it while a better protected quote stands is a trade-through. So venues are ordered by
// price first and by net cost only within a price, and the sweep exhausts every better price
// before it touches a worse one. Where it does cross several prices at once it marks the children
// ISO, which is what makes that lawful, and it is lawful only because it is simultaneously taking
// the better quotes. If a better-priced venue cannot be routed to at all - no session up - the
// sweep stops at that price rather than stepping over it, because an intermarket sweep order that
// does not actually sweep is a trade-through with a flag on it.
//
// Then cost. Expected net cost per share is the price the client pays or receives, adjusted by the
// venue's fee or rebate, weighted by how likely that venue is to fill, with the shortfall charged
// at what the fallback would cost, and with the scorecard's post-fill reversion added as the
// adverse-selection cost it is. Everything is per share in 1e-8 units and signed so that lower is
// better for the client on either side, which is the only way a buy and a sell can be compared by
// the same code.
//
// Then session placement: among the venue's sessions that are up and have throttle headroom, the
// least loaded, counting what this plan has already put on them. Symbol affinity and treating
// saturation as a cost rather than a wall wait for the venue gateways (RT-7).
//
// Deterministic: no clock, no map iteration order, venues walked by id. The same parent against the
// same book and the same scorecards produces the same plan, live and in replay, which is what lets
// the harness recompute every decision from the journal.
#pragma once
#include "book.hpp"
#include "flatmap.hpp"
#include "refdata.hpp"
#include "blake3.h"
#include <algorithm>
#include <array>

namespace trading::router {

inline constexpr size_t MAX_CHILDREN = 8;
inline constexpr size_t MAX_VENUES = 16;
inline constexpr size_t MAX_SESSIONS = 16;

// Why a plan came out the way it did. Recorded in RouteDecision.reason, so a best-execution review
// reads a cause rather than inferring one.
namespace PlanReason {
inline constexpr uint16_t Swept            = 1;   // displayed liquidity taken across venues
inline constexpr uint16_t SweptWithIso     = 2;   // and it crossed a protected quote, so ISO
inline constexpr uint16_t NoLiquidity      = 3;   // nothing displayed at or better than the limit
inline constexpr uint16_t PostedRemainder  = 4;   // what the sweep could not take was rested
inline constexpr uint16_t BlockedByBetter  = 5;   // a better price existed that we could not route to
inline constexpr uint16_t NoSession        = 6;   // no venue had a session that could carry it
}

struct Child {
    uint16_t venueId = 0, venueSessionIdx = 0, strategyTag = 0;
    int64_t  qty = 0, price = 0, displayQty = 0;
    OrdType  ordType = OrdType::Limit;
    Tif      tif = Tif::Day;
    uint8_t  orderFlags = 0;
    int64_t  releaseOffsetNs = 0;
    int64_t  expectedCostPerShare = 0;      // what the model thought this child would cost
};

struct Plan {
    uint8_t  count = 0;
    Child    children[MAX_CHILDREN]{};
    int64_t  expectedCost = 0;              // expected net, in 1e-8 units, lower is better
    int64_t  rejectedAltCost = 0;           // the same figure for the best alternative not taken
    uint16_t rejectedAltVenue = 0;
    uint16_t strategyTag = 0;
    uint16_t reason = 0;

    int64_t qty() const noexcept {
        int64_t t = 0;
        for (uint8_t i = 0; i < count; ++i) t += children[i].qty;
        return t;
    }
};

class Router {
public:
    struct Params {
        uint16_t sourceId = 0;
        uint16_t maxVenueId = 4;
        uint16_t sessionsPerVenue = 3;
        // What the model assumes when the TCA service has not spoken yet. A fill rate of one and no
        // reversion is the neutral assumption: it makes the model reduce to price plus fee, which
        // is what a router without a scorecard should do.
        uint32_t defaultFillRateBps = 10'000;
        int32_t  defaultReversionBps = 0;
        // What not being filled is assumed to cost, over the price we would have paid. Charged
        // against the unfilled fraction so a venue that is cheap but unreliable is not free.
        int64_t  missPenaltyBps = 25;
        int64_t  maxChildren = MAX_CHILDREN;
    };

    Router(const refdata::Snapshot& snap, Params p)
        : p_(p), fees_(256) {
        for (const FeeRecord& f : snap.fees()) putFee(f);
        for (auto& v : venue_) v = VenueState{};
        latency_.fill(0);
        for (const VenueRecord& v : snap.venues())
            if (v.venueId < MAX_VENUES) latency_[v.venueId] = v.oneWayLatencyNs;
    }

    void setBook(const md::BookReader* b) noexcept { book_ = b; }

    // Everything the router learns from the sequenced core stream.
    void apply(const FrameHeader* f) {
        if (const auto* s = as<VenueSessionStatus>(f)) {
            if (s->venueId >= MAX_VENUES || s->sessionIdx >= MAX_SESSIONS) return;
            Session& ss = venue_[s->venueId].sessions[s->sessionIdx];
            ss.up = s->state == SessionState::Up;
            ss.openOrders = s->openOrders;
            ss.queueDepth = s->queueDepth;
            ss.throttleUsedBps = s->throttleUsedBps;
            ss.known = true;
        } else if (const auto* c = as<VenueScorecard>(f)) {
            if (c->venueId >= MAX_VENUES) return;
            // symbolIdx 0 is the venue-wide card; a per-symbol card overrides it for that symbol.
            Scorecard& sc = c->symbolIdx == 0 ? venue_[c->venueId].card : venue_[c->venueId].card;
            sc.fillRateBps = c->fillRateBps;
            sc.reversionBps = c->reversionBps;
            sc.latencyNs = c->latencyP50Ns;
            sc.known = true;
            ++scorecards_;
        } else if (const auto* fs = as<FeeScheduleUpdate>(f)) {
            putFee(fs->record);
        } else if (const auto* ch = as<ChildOrder>(f)) {
            if (ch->venueId < MAX_VENUES) ++venue_[ch->venueId].children;
            ++children_;
        }
    }

    // The plan for `leaves` of this parent. Returns false when there is nothing routable at all,
    // which the caller turns into a reject or a wait rather than an empty child.
    bool plan(const NewOrder& o, int64_t leaves, int64_t now, Plan& out) const {
        out = Plan{};
        out.strategyTag = uint16_t(RouteStrategy::Sweep);
        if (leaves <= 0) { out.reason = PlanReason::NoLiquidity; return false; }
        return sweep(o, leaves, now, out);
    }

    // Tell the router a plan was acted on, so the next plan in the same poll sees the load it added.
    void placed(const Plan& p) noexcept {
        for (uint8_t i = 0; i < p.count; ++i) {
            const Child& c = p.children[i];
            if (c.venueId < MAX_VENUES && c.venueSessionIdx < MAX_SESSIONS)
                ++venue_[c.venueId].sessions[c.venueSessionIdx].placed;
        }
        ++plans_;
    }

    void hashInto(blake3_hasher& h) const {
        for (uint16_t v = 0; v < MAX_VENUES; ++v) {
            blake3_hasher_update(&h, &venue_[v].children, sizeof(uint64_t));
            for (const Session& s : venue_[v].sessions) blake3_hasher_update(&h, &s.placed, sizeof(uint64_t));
        }
        blake3_hasher_update(&h, &children_, sizeof children_);
    }

    uint64_t plans() const noexcept { return plans_; }
    uint64_t children() const noexcept { return children_; }
    uint64_t scorecards() const noexcept { return scorecards_; }
    uint64_t isoPlans() const noexcept { return isoPlans_; }
    uint64_t blockedByBetter() const noexcept { return blockedByBetter_; }
    uint64_t posted() const noexcept { return posted_; }
    uint64_t noLiquidity() const noexcept { return noLiquidity_; }
    int64_t  venueLatencyNs(uint16_t venueId) const noexcept { return venueId < MAX_VENUES ? latency_[venueId] : 0; }
    uint64_t placedOn(uint16_t venueId, uint16_t session) const noexcept {
        return venueId < MAX_VENUES && session < MAX_SESSIONS ? venue_[venueId].sessions[session].placed : 0;
    }
    // What every session of a venue has been given, for a harness that reports session balance.
    std::array<uint64_t, MAX_SESSIONS> placements(uint16_t venueId) const noexcept {
        std::array<uint64_t, MAX_SESSIONS> out{};
        if (venueId < MAX_VENUES) for (size_t i = 0; i < MAX_SESSIONS; ++i) out[i] = venue_[venueId].sessions[i].placed;
        return out;
    }

    // ---- the cost model, exposed so it can be unit-tested against worked examples ------------
    struct Quote { uint16_t venueId; int64_t price; int64_t qty; };

    int64_t feePerShare(uint16_t venueId, Liquidity liq) const noexcept {
        const int64_t* f = fees_.find(feeKey(venueId, liq));
        return f ? *f : 0;
    }
    // The price the client actually pays or receives per share, fee included, signed so that lower
    // is better on either side. A buyer's cost goes up with the price and with a taker fee; a
    // seller's goes down as the price goes up, so the seller's figure is the negated proceeds.
    int64_t netPerShare(uint16_t venueId, Side side, int64_t price, Liquidity liq) const noexcept {
        const int64_t fee = feePerShare(venueId, liq);
        return side == Side::Buy ? price + fee : fee - price;
    }
    // And what the model expects that to cost once the venue's own record is taken into account:
    // the fill it probably gets, the shortfall at what the fallback would cost, and the post-fill
    // reversion the scorecard has measured, which is a cost whichever way the order went.
    int64_t expectedPerShare(uint16_t venueId, Side side, int64_t price, Liquidity liq,
                             int64_t fallbackPerShare) const noexcept {
        const Scorecard& sc = card(venueId);
        const int64_t filled = netPerShare(venueId, side, price, liq);
        const int64_t pFillBps = sc.fillRateBps;
        const int64_t missBps = 10'000 - pFillBps;
        const int64_t reversion = price * sc.reversionBps / 10'000;
        int64_t v = (filled * pFillBps + fallbackPerShare * missBps) / 10'000;
        return v + reversion;
    }
    int64_t missPenaltyPerShare(int64_t price) const noexcept { return price * p_.missPenaltyBps / 10'000; }

private:
    struct Session {
        uint64_t placed = 0;
        uint32_t openOrders = 0, queueDepth = 0, throttleUsedBps = 0;
        bool up = false, known = false;
    };
    struct Scorecard { uint32_t fillRateBps = 0; int32_t reversionBps = 0; int64_t latencyNs = 0; bool known = false; };
    struct VenueState {
        std::array<Session, MAX_SESSIONS> sessions{};
        Scorecard card{};
        uint64_t children = 0;
    };

    static uint64_t feeKey(uint16_t venue, Liquidity liq) noexcept { return (uint64_t(venue) << 8) | uint8_t(liq); }
    void putFee(const FeeRecord& f) { fees_.insert(feeKey(f.venueId, Liquidity(f.liquidity)), f.feePerShare); }

    Scorecard card(uint16_t venueId) const noexcept {
        if (venueId < MAX_VENUES && venue_[venueId].card.known) return venue_[venueId].card;
        Scorecard d{};
        d.fillRateBps = p_.defaultFillRateBps;
        d.reversionBps = p_.defaultReversionBps;
        return d;
    }

    // The least loaded session that is up and has throttle headroom, counting what this plan has
    // already put on it. Returns false when the venue cannot carry the order at all.
    bool pickSession(uint16_t venueId, uint16_t& out) const noexcept {
        if (venueId >= MAX_VENUES) return false;
        const VenueState& v = venue_[venueId];
        bool found = false;
        uint64_t best = 0;
        for (uint16_t i = 0; i < p_.sessionsPerVenue && i < MAX_SESSIONS; ++i) {
            const Session& s = v.sessions[i];
            // A session we have never heard from is not a session. Routing to a venue on the
            // assumption that a gateway exists is the same fault as an intermarket sweep that does
            // not actually sweep: the order goes nowhere and the parent waits for an ack that is
            // never coming.
            if (!s.known || !s.up) continue;
            if (s.throttleUsedBps >= 10'000) continue;              // saturated: a wall until RT-7
            // What is already on the session, what is queued behind it, and what this plan has
            // just put there - the last one so two children of one parent do not pile onto the
            // same port because neither could see the other.
            const uint64_t load = uint64_t(s.openOrders) + s.queueDepth + s.placed;
            if (!found || load < best) { best = load; out = i; found = true; }
        }
        return found;
    }

    // Every displayed quote at or better than the limit, one per venue, from the shared book.
    // Venues are walked by id so the order never depends on anything but the data.
    size_t quotesAtOrBetter(const NewOrder& o, int64_t limit, Quote* into, size_t cap,
                            int64_t& bestProtected) const {
        size_t n = 0;
        bestProtected = 0;
        if (!book_) return 0;
        const bool buy = o.side == Side::Buy;
        for (uint16_t v = 1; v <= p_.maxVenueId && v < MAX_VENUES; ++v) {
            md::LadderSnapshot l{};
            if (!book_->ladder(o.symbolIdx, v, l)) continue;
            const uint64_t mask = buy ? l.askMask : l.bidMask;
            if (!mask) continue;
            const int slot = buy ? std::countr_zero(mask) : 63 - std::countl_zero(mask);
            const int64_t px = l.priceAt(buy ? BookSide::Ask : BookSide::Bid, slot);
            const int64_t qty = (buy ? l.askQty : l.bidQty)[slot];
            if (qty <= 0) continue;
            // The protected quote is the best displayed price anywhere, limit or no limit: it is
            // what a trade-through is measured against.
            if (!bestProtected || (buy ? px < bestProtected : px > bestProtected)) bestProtected = px;
            if (limit && (buy ? px > limit : px < limit)) continue;
            if (n < cap) into[n++] = Quote{v, px, qty};
        }
        return n;
    }

    bool sweep(const NewOrder& o, int64_t leaves, int64_t now, Plan& out) const;

    Params p_;
    util::FlatMap<uint64_t, int64_t> fees_;
    const md::BookReader* book_ = nullptr;
    mutable std::array<VenueState, MAX_VENUES> venue_{};
    std::array<int64_t, MAX_VENUES> latency_{};
    mutable uint64_t plans_ = 0, children_ = 0, scorecards_ = 0, isoPlans_ = 0, blockedByBetter_ = 0;
    mutable uint64_t posted_ = 0, noLiquidity_ = 0;
};

// ---- the sweep ---------------------------------------------------------------------------------
inline bool Router::sweep(const NewOrder& o, int64_t leaves, int64_t now, Plan& out) const {
    (void)now;
    const bool buy = o.side == Side::Buy;
    const int64_t limit = o.ordType == OrdType::Market ? 0 : o.price;

    Quote q[MAX_VENUES];
    int64_t bestProtected = 0;
    const size_t n = quotesAtOrBetter(o, limit, q, MAX_VENUES, bestProtected);

    struct Ranked { Quote quote; int64_t expected; uint16_t session; bool routable; };
    Ranked r[MAX_VENUES];
    size_t rn = 0;
    for (size_t i = 0; i < n; ++i) {
        Ranked x{};
        x.quote = q[i];
        uint16_t s = 0;
        x.routable = pickSession(q[i].venueId, s);
        x.session = s;
        // What it costs to miss here: the same price plus the penalty, which is the model's stand-in
        // for having to come back and pay up somewhere else.
        const int64_t fallback = netPerShare(q[i].venueId, o.side, q[i].price, Liquidity::Removed)
                               + missPenaltyPerShare(q[i].price);
        x.expected = expectedPerShare(q[i].venueId, o.side, q[i].price, Liquidity::Removed, fallback);
        r[rn++] = x;
    }
    // Price first, because price priority is regulation and not preference. Net cost decides only
    // between venues showing the same price, and the venue id makes the order total so that two
    // runs over the same book sort identically.
    std::sort(r, r + rn, [&](const Ranked& a, const Ranked& b) {
        if (a.quote.price != b.quote.price) return buy ? a.quote.price < b.quote.price : a.quote.price > b.quote.price;
        if (a.expected != b.expected) return a.expected < b.expected;
        return a.quote.venueId < b.quote.venueId;
    });

    int64_t remaining = leaves, worstTaken = 0;
    bool blocked = false;
    // A post-only order has said it will not take liquidity. Sweeping one would have the venue
    // reject every child, so the taking phase is skipped and the whole quantity is rested.
    const bool postOnly = (o.orderFlags & OrderFlags::postOnly) != 0;
    for (size_t i = 0; !postOnly && i < rn && remaining > 0 && out.count < p_.maxChildren; ++i) {
        // A better price we cannot reach stops the sweep here. Stepping over it would be a
        // trade-through, and an ISO does not excuse one: the flag is lawful only because the
        // better quote is being taken at the same time, which it cannot be if there is no session.
        if (!r[i].routable) { blocked = true; break; }
        const int64_t take = std::min(remaining, r[i].quote.qty);
        if (take <= 0) continue;
        Child& c = out.children[out.count++];
        c.venueId = r[i].quote.venueId;
        c.venueSessionIdx = r[i].session;
        c.qty = take;
        c.price = r[i].quote.price;
        c.displayQty = take;
        c.ordType = OrdType::Limit;
        c.tif = Tif::Ioc;                                   // a sweep takes or it does not
        c.orderFlags = o.orderFlags;
        c.strategyTag = uint16_t(RouteStrategy::Sweep);
        c.expectedCostPerShare = r[i].expected;
        remaining -= take;
        worstTaken = c.price;
    }
    if (blocked) ++blockedByBetter_;

    // Crossing several prices at once is lawful as an intermarket sweep, and only as one, because
    // the better quotes are in the same plan and go out together.
    const bool crossed = out.count > 0 && bestProtected
                      && (buy ? worstTaken > bestProtected : worstTaken < bestProtected);
    if (crossed) {
        for (uint8_t i = 0; i < out.count; ++i) out.children[i].orderFlags = uint8_t(out.children[i].orderFlags | OrderFlags::iso);
        ++isoPlans_;
    }

    // What the sweep could not take rests at the limit, where posting pays best. A market order
    // rests nothing: it asked to trade, not to quote.
    bool postedHere = false;
    if (remaining > 0 && out.count < p_.maxChildren && limit != 0) {
        uint16_t bestVenue = 0, bestSession = 0;
        int64_t bestCost = 0;
        bool have = false;
        for (uint16_t v = 1; v <= p_.maxVenueId && v < MAX_VENUES; ++v) {
            uint16_t s = 0;
            if (!pickSession(v, s)) continue;
            const int64_t fallback = netPerShare(v, o.side, limit, Liquidity::Added) + missPenaltyPerShare(limit);
            const int64_t cost = expectedPerShare(v, o.side, limit, Liquidity::Added, fallback);
            if (!have || cost < bestCost) { bestCost = cost; bestVenue = v; bestSession = s; have = true; }
        }
        if (have) {
            Child& c = out.children[out.count++];
            c.venueId = bestVenue;
            c.venueSessionIdx = bestSession;
            c.qty = remaining;
            c.price = limit;
            c.displayQty = o.displayQty ? std::min(o.displayQty, remaining) : remaining;
            c.ordType = OrdType::Limit;
            c.tif = o.tif;
            c.orderFlags = o.orderFlags;
            c.strategyTag = uint16_t(RouteStrategy::Sweep);
            c.expectedCostPerShare = bestCost;
            remaining = 0;
            postedHere = true;
            ++posted_;
        }
    }

    if (out.count == 0) {
        out.reason = blocked ? PlanReason::BlockedByBetter : (rn ? PlanReason::NoSession : PlanReason::NoLiquidity);
        ++noLiquidity_;
        return false;
    }
    // The reason names what actually happened to this parent, worst first: a better price we could
    // not reach is the thing a reviewer wants to find, then an intermarket sweep, then a remainder
    // left resting, and only then a plain sweep.
    if (blocked)             out.reason = PlanReason::BlockedByBetter;
    else if (crossed)        out.reason = PlanReason::SweptWithIso;
    else if (postedHere)     out.reason = PlanReason::PostedRemainder;
    else                     out.reason = PlanReason::Swept;

    // Children land together: the slowest venue in the plan sets the clock and everything faster
    // waits for it, so one venue does not print before another has seen the order.
    int64_t slowest = 0;
    for (uint8_t i = 0; i < out.count; ++i) slowest = std::max(slowest, venueLatencyNs(out.children[i].venueId));
    for (uint8_t i = 0; i < out.count; ++i)
        out.children[i].releaseOffsetNs = slowest - venueLatencyNs(out.children[i].venueId);

    for (uint8_t i = 0; i < out.count; ++i) out.expectedCost += out.children[i].expectedCostPerShare * out.children[i].qty;

    // The runner-up, which is the number a best-execution review actually asks for: what the whole
    // order on one venue would have cost. When the plan is already one venue, the alternative is
    // the next venue; when it is a sweep, it is the best single venue.
    bool haveAlt = false;
    for (size_t i = 0; i < rn; ++i) {
        if (out.count == 1 && r[i].quote.venueId == out.children[0].venueId) continue;
        const int64_t cost = r[i].expected * leaves;
        if (!haveAlt || cost < out.rejectedAltCost) { out.rejectedAltCost = cost; out.rejectedAltVenue = r[i].quote.venueId; haveAlt = true; }
    }
    return true;
}

} // namespace trading::router
